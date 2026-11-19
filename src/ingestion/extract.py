from pathlib import Path
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import pandas as pd
from sqlalchemy import inspect, text
from src.config.settings import BASE_DIR, EXTRACT_PIPELINE_RULES, OLIST_7_TABLES, SRC_DB_TIMEZONE
from src.core.connection import get_source_engine
from src.core.logger import get_logger
from src.schema.contract import (
    SchemaValidationError,
    parse_columns_config,
    resolve_and_validate_columns,
)

logger = get_logger(__name__)


def _format_batch_cutoff(batch_time):
    if isinstance(batch_time, datetime):
        if batch_time.tzinfo is not None:
            try:
                target_tz = ZoneInfo(SRC_DB_TIMEZONE)
            except ZoneInfoNotFoundError:
                logger.warning(f"Múi giờ SRC_DB_TIMEZONE='{SRC_DB_TIMEZONE}' không hợp lệ -> fallback về UTC.")
                target_tz = timezone.utc
            batch_time = batch_time.astimezone(target_tz)
        return batch_time.strftime("%Y-%m-%d %H:%M:%S")
    return str(batch_time)[:19]


def resolve_table_name(db_tables, candidates):
    lookup = {t.lower(): t for t in db_tables}
    for name in candidates:
        if name.lower() in lookup:
            return lookup[name.lower()]
    return None


def build_dynamic_query(
    logical_name,
    sql_table_name,
    rule,
    last_watermark=None,
    schema="dbo",
    include_order_by=True,
    resolved_columns=None,
    batch_time=None,
):
    strategy = rule.get("strategy")
    if not strategy:
        logger.error(f"Bảng '{logical_name}' lỗi cấu hình: thiếu 'strategy' trong YAML -> Hủy trích xuất.")
        return None

    strategy = str(strategy).lower().strip()
    if strategy == "cancel":
        logger.warning(f"Bảng '{logical_name}' có chế độ trích xuất 'CANCEL' -> Hủy trích xuất.")
        return None

    if resolved_columns:
        cols_str = ", ".join(f"[{c.strip()}]" for c in resolved_columns)
    else:
        mode, col_list = parse_columns_config(rule)
        if mode == "selected" and col_list:
            cols_str = ", ".join(f"[{c.strip()}]" for c in col_list)
        else:
            cols_str = "*"

    base_query = f"SELECT {cols_str} FROM [{schema}].[{sql_table_name}]"
    col = rule.get("watermark_column")
    where_filter = ""
    cutoff_filter = ""
    if batch_time and col:
        cutoff = _format_batch_cutoff(batch_time)
        cutoff_filter = f" AND [{col}] <= '{cutoff}'"

    if strategy == "full":
        logger.info(f"Bảng '{logical_name}' dùng chế độ trích xuất: FULL")
        return base_query

    elif strategy == "watermark":
        if not col:
            logger.error(
                f"Bảng '{logical_name}' lỗi Watermark: thiếu cấu hình 'watermark_column' "
                f"-> Hủy trích xuất để bảo vệ dữ liệu."
            )
            return None

        mark = last_watermark or rule.get("default_start_watermark")
        if not mark:
            logger.error(
                f"Bảng '{logical_name}' lỗi Watermark: thiếu 'default_start_watermark' và chưa có 'last_watermark' "
                f"-> Hủy trích xuất."
            )
            return None

        op = ">=" if rule.get("resume_in_progress") else ">"
        where_filter = f"[{col}] {op} '{mark}'{cutoff_filter}"
        logger.info(f"Bảng '{logical_name}' dùng chế độ trích xuất: WATERMARK ({where_filter})")

    elif strategy == "lookback_window":
        if not col:
            logger.error(
                f"Bảng '{logical_name}' lỗi Lookback: thiếu cấu hình 'watermark_column' "
                f"-> Hủy trích xuất để bảo vệ dữ liệu."
            )
            return None

        mark = last_watermark or rule.get("default_start_watermark")
        if not mark:
            logger.error(
                f"Bảng '{logical_name}' lỗi Lookback: thiếu 'default_start_watermark' và chưa có 'last_watermark' "
                f"-> Hủy trích xuất."
            )
            return None

        days = rule.get("lookback_days")
        if days is None:
            logger.error(
                f"Bảng '{logical_name}' lỗi Lookback: thiếu cấu hình 'lookback_days' "
                f"-> Hủy trích xuất."
            )
            return None

        try:
            dt = datetime.fromisoformat(str(mark)[:19])
            adjusted_mark = (dt - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
            where_filter = f"[{col}] > '{adjusted_mark}'{cutoff_filter}"
            logger.info(f"Bảng '{logical_name}' dùng chế độ trích xuất: LOOKBACK ({where_filter})")
        except Exception as e:
            logger.error(
                f"Bảng '{logical_name}' lỗi Lookback: không parse được mốc thời gian '{mark}' ({e}) "
                f"-> Hủy trích xuất."
            )
            return None

    elif strategy == "date_range":
        start_d = rule.get("start_date")
        end_d = rule.get("end_date")
        if not col or not start_d or not end_d:
            logger.error(
                f"Bảng '{logical_name}' lỗi Date Range: thiếu 'watermark_column', 'start_date' hoặc 'end_date' "
                f"-> Hủy trích xuất."
            )
            return None
        where_filter = f"[{col}] >= '{start_d}' AND [{col}] < '{end_d}'"
        logger.info(f"Bảng '{logical_name}' dùng chế độ trích xuất: DATE_RANGE ({start_d} -> {end_d})")

    else:
        logger.error(
            f"Bảng '{logical_name}' lỗi Strategy: chiến lược '{strategy}' không được hỗ trợ "
            f"-> Hủy trích xuất."
        )
        return None

    if where_filter:
        base_query = f"{base_query} WHERE {where_filter}"

    if include_order_by and strategy in ("watermark", "lookback_window", "date_range") and col:
        order_cols = [f"[{col}] ASC"]
        for pk in (rule.get("primary_keys") or []):
            if pk != col and (not resolved_columns or pk in resolved_columns):
                order_cols.append(f"[{pk}] ASC")
        base_query = f"{base_query} ORDER BY {', '.join(order_cols)}"

    return base_query


def extract_table(logical_name, sql_table_name, rule, engine, batch_time, batch_id, last_watermark=None, query_str=None, schema="dbo"):
    start_time = time.time()
    chunksize = rule.get("chunksize")
    if not chunksize:
        logger.error(f"Bảng '{logical_name}' lỗi cấu hình: thiếu 'chunksize' trong YAML -> Hủy trích xuất.")
        return

    if query_str is None:
        query_str = build_dynamic_query(logical_name, sql_table_name, rule, last_watermark, schema)

    if not query_str:
        logger.warning(f"Bỏ qua trích xuất bảng '{logical_name}' (không có câu truy vấn).")
        return

    query = text(query_str)
    total_rows = 0
    chunk_idx = 0
    col_count = 0
    extract_duration = 0.0

    extracted_at_val = (
        batch_time.astimezone(timezone.utc).replace(tzinfo=None)
        if isinstance(batch_time, datetime) and batch_time.tzinfo is not None
        else batch_time
    )

    try:
        with engine.connect().execution_options(stream_results=True) as conn:
            try:
                conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;"))
            except Exception:
                pass
            reader = pd.read_sql(query, conn, chunksize=chunksize, dtype_backend="numpy_nullable")
            t0 = time.time()
            for chunk in reader:
                extract_duration += time.time() - t0

                if chunk.empty:
                    t0 = time.time()
                    continue

                chunk_idx += 1
                total_rows += len(chunk)
                col_count = len(chunk.columns)

                chunk = chunk.copy()
                chunk["_extracted_at"] = extracted_at_val
                chunk["_batch_id"] = batch_id

                logger.debug(f"Extract chunk #{chunk_idx} '{logical_name}': {len(chunk):,} dòng")
                yield chunk
                t0 = time.time()
    except Exception as e:
        logger.error(f"Lỗi trích xuất bảng '{logical_name}': {e}. Rollback bảng này và đánh dấu FAILED!")
        raise

    speed = (total_rows / extract_duration) if extract_duration > 0 else 0
    logger.info(
        f"Hoàn tất trích xuất '{logical_name:<14}': {total_rows:>7,} dòng, "
        f"{col_count:>2} cột ({chunk_idx} chunks, {extract_duration:>5.2f}s, {speed:,.0f} dòng/giây)"
        if extract_duration > 0
        else f"Hoàn tất trích xuất '{logical_name:<14}': {total_rows:>7,} dòng"
    )


def extract_from_sqlserver(
    schema="dbo",
    batch_time=None,
    watermark_states=None,
    source_baseline=None,
    in_progress_tables=None,
):
    total_start = time.time()
    if batch_time is None:
        batch_time = datetime.now(timezone.utc)
    batch_id = batch_time.strftime("%Y%m%d_%H%M%S")
    watermark_states = watermark_states or {}
    source_baseline = source_baseline or {}
    in_progress_set = in_progress_tables or set()

    logger.info(f"=== BẮT ĐẦU EXTRACT LINH HOẠT TỪ SOURCE - BATCH: {batch_id} ===")

    engine = get_source_engine()
    inspector = None
    existing_tables = None

    for logical_name, rule_template in EXTRACT_PIPELINE_RULES.items():
        rule = dict(rule_template)
        rule["resume_in_progress"] = logical_name in in_progress_set
        if logical_name in source_baseline:
            base_info = source_baseline[logical_name]
            actual_name = base_info["actual_table_name"]
            resolved_columns = base_info["expected_columns"]
            source_columns = base_info.get("source_columns_meta", [])
        else:
            if inspector is None:
                inspector = inspect(engine)
                existing_tables = set(inspector.get_table_names(schema=schema))
            actual_name = resolve_table_name(existing_tables, rule["candidates"])
            if not actual_name:
                msg = (
                    f"Thiếu bảng nguồn: không tìm thấy bảng '{logical_name}' trên SQL Server. "
                    f"Các bảng hiện có: {sorted(existing_tables)} -> Dừng pipeline có kiểm soát."
                )
                logger.error(msg)
                raise SchemaValidationError(msg)

            source_columns = inspector.get_columns(actual_name, schema=schema)
            resolved_columns, mode = resolve_and_validate_columns(
                logical_name=logical_name,
                actual_table_name=actual_name,
                rule=rule,
                source_columns=source_columns,
            )

        rule["resolved_columns"] = resolved_columns
        rule["source_columns"] = source_columns

        last_wm = watermark_states.get(logical_name)

        query_str = build_dynamic_query(
            logical_name,
            actual_name,
            rule,
            last_watermark=last_wm,
            schema=schema,
            resolved_columns=resolved_columns,
            batch_time=batch_time,
        )
        if not query_str:
            logger.warning(f"⏩ Bỏ qua bảng '{logical_name}' do bị hủy theo cơ chế/cancel. Tiếp tục các bảng khác...")
            continue

        table_stream = extract_table(
            logical_name=logical_name,
            sql_table_name=actual_name,
            rule=rule,
            engine=engine,
            batch_time=batch_time,
            batch_id=batch_id,
            last_watermark=last_wm,
            query_str=query_str,
            schema=schema,
        )

        yield logical_name, actual_name, rule, table_stream

    total_elapsed = time.time() - total_start
    logger.info(f"=== KẾT THÚC BƯỚC EXTRACT (TỔNG THỜI GIAN: {total_elapsed:.2f}s) ===")