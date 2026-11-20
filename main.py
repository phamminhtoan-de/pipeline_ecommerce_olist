import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import re
import sys
import time
from datetime import datetime, timezone
from src.config.settings import ETL_MAX_WORKERS, EXTRACT_PIPELINE_RULES, LOGS_DIR, TGT_DB_SCHEMA
from src.core.alert import send_alert_webhook
from src.core.connection import (
    get_source_engine,
    get_target_engine,
    test_connections,
    test_source_connection,
    test_target_connection,
)
from src.core.logger import get_logger
from src.ingestion.extract import build_dynamic_query, extract_from_sqlserver, extract_table
from src.ingestion.load import load_single_table
from src.monitoring.collect_errors import init_monitoring_schema
from src.schema.contract import SchemaValidationError
from src.schema.init_schema import init_bronze_schema
from src.state.history import save_pipeline_run_history
from src.state.state_manager import (
    get_active_checkpoints,
    init_checkpoint_manager,
    mark_all_checkpoints_completed,
)
from src.state.watermark import get_watermark_states
from src.validate.validate_source import measure_source_baseline
from src.validate.validate_target import validate_staging_against_baseline

logger = get_logger(__name__)

REQUIRED_TABLES = [
    name
    for name, rule in EXTRACT_PIPELINE_RULES.items()
    if str(rule.get("strategy", "")).lower().strip() != "cancel"
]


def ensure_all_tables_loaded(pipeline_stats, required_tables=None):
    expected_tables = REQUIRED_TABLES if required_tables is None else required_tables
    failed = {n: s["status"] for n, s in pipeline_stats.items() if s["status"] != "SUCCESS"}
    skipped = [n for n in expected_tables if n not in pipeline_stats]

    if failed or skipped:
        details = [f"  - {n}: {status}" for n, status in failed.items()]
        details += [f"  - {n}: SKIPPED (không được extract/load)" for n in skipped]
        raise RuntimeError(
            f"Có {len(failed) + len(skipped)} bảng không nạp thành công -> DỪNG PIPELINE:\n" + "\n".join(details)
        )


def _sanitize_run_id(run_id):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(run_id)).strip("_") or "default"


def _run_state_dir(run_id):
    return LOGS_DIR / "run_state" / _sanitize_run_id(run_id)


def _save_validation_snapshots(run_id, source_baseline, pipeline_stats):
    state_dir = _run_state_dir(run_id)
    state_dir.mkdir(parents=True, exist_ok=True)
    for logical_name, base_info in source_baseline.items():
        payload = {
            "baseline": {
                "actual_table_name": base_info.get("actual_table_name"),
                "target_table_name": base_info.get("target_table_name"),
                "strategy": base_info.get("strategy"),
                "source_row_count": base_info.get("source_row_count"),
                "expected_extract_rows": base_info.get("expected_extract_rows"),
                "expected_columns": list(base_info.get("expected_columns", [])),
                "date_range": base_info.get("date_range"),
            },
            "stat": pipeline_stats.get(logical_name),
        }
        target_path = state_dir / f"{logical_name}.json"
        tmp_path = state_dir / f"{logical_name}.{os.getpid()}.json.tmp"
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp_path, target_path)
    return state_dir


def _load_validation_snapshots(run_id, expected_tables):
    state_dir = _run_state_dir(run_id)
    source_baseline = {}
    pipeline_stats = {}
    missing = []
    for logical_name in expected_tables:
        snapshot_path = state_dir / f"{logical_name}.json"
        if not snapshot_path.exists():
            missing.append(logical_name)
            continue
        data = json.loads(snapshot_path.read_text(encoding="utf-8"))
        source_baseline[logical_name] = data["baseline"]
        if data.get("stat") is not None:
            pipeline_stats[logical_name] = data["stat"]

    if missing:
        raise RuntimeError(f"Thiếu file snapshot đối soát cho bảng {missing} trong run_id='{run_id}'")
    return source_baseline, pipeline_stats


def _parse_cli_args(argv=None):
    parser = argparse.ArgumentParser(description="Olist E-Commerce Medallion Bronze Pipeline")
    parser.add_argument("--preflight", action="store_true", help="Kiểm tra kết nối Source và Target DB rồi thoát")
    parser.add_argument(
        "--preflight-source",
        action="store_true",
        help="Chỉ kiểm tra kết nối Source DB (SQL Server) rồi thoát",
    )
    parser.add_argument(
        "--preflight-target",
        action="store_true",
        help="Chỉ kiểm tra kết nối Target DB (PostgreSQL) rồi thoát",
    )
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help="Khởi tạo trước schema Bronze và bảng metadata (chạy độc lập hoặc cùng --preflight)",
    )
    parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="Bỏ qua bước đối chiếu Bronze trong tiến trình EL để chạy ở task validate riêng",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Chỉ chạy đối chiếu tầng Bronze với baseline nguồn",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Định danh lần chạy dùng để lưu/đọc snapshot baseline giữa task EL và task validate",
    )
    parser.add_argument("--sequential", action="store_true", help="Chạy tuần tự thay vì ThreadPoolExecutor")
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        default=None,
        help="Tên logical bảng cần nạp (lặp lại hoặc phân tách dấu phẩy, vd: --table orders)",
    )
    args = parser.parse_args(argv)

    target_tables = None
    if args.tables:
        parsed_tables = []
        for raw in args.tables:
            for item in raw.split(","):
                cleaned = item.strip()
                if cleaned and cleaned not in parsed_tables:
                    parsed_tables.append(cleaned)
        invalid = [t for t in parsed_tables if t not in EXTRACT_PIPELINE_RULES]
        if invalid:
            parser.error(
                f"Bảng không hợp lệ: {invalid}. Giá trị hợp lệ: {sorted(EXTRACT_PIPELINE_RULES.keys())}"
            )
        target_tables = parsed_tables

    return args, target_tables


def _process_single_table_worker(
    logical_name,
    base_info,
    rule_template,
    watermark_states,
    batch_time,
    batch_id,
    src_engine,
    tgt_engine,
    target_schema,
    in_progress_tables=None,
):
    rule = dict(rule_template)
    actual_name = base_info["actual_table_name"]
    resolved_columns = base_info["expected_columns"]
    rule["resolved_columns"] = resolved_columns
    rule["source_columns"] = base_info.get("source_columns_meta", [])
    rule["resume_in_progress"] = logical_name in (in_progress_tables or set())
    last_wm = watermark_states.get(logical_name)

    query_str = build_dynamic_query(
        logical_name=logical_name,
        sql_table_name=actual_name,
        rule=rule,
        last_watermark=last_wm,
        resolved_columns=resolved_columns,
        batch_time=batch_time,
    )
    if not query_str:
        logger.warning(f"Bỏ qua bảng '{logical_name}' do bị hủy theo cơ chế/cancel.")
        return logical_name, None

    chunk_stream = extract_table(
        logical_name=logical_name,
        sql_table_name=actual_name,
        rule=rule,
        engine=src_engine,
        batch_time=batch_time,
        batch_id=batch_id,
        last_watermark=last_wm,
        query_str=query_str,
    )
    stat = load_single_table(
        logical_name=logical_name,
        actual_name=actual_name,
        rule=rule,
        chunk_stream=chunk_stream,
        tgt_engine=tgt_engine,
        tgt_schema=target_schema,
    )
    return logical_name, stat


def main():
    args, target_tables = _parse_cli_args(sys.argv[1:])

    if args.preflight:
        logger.info("Đang kiểm tra kết nối Source (SQL Server) và Target (PostgreSQL)...")
        try:
            test_connections()
            if args.init_schema:
                logger.info("Đang khởi tạo trước schema Bronze và metadata (--init-schema)...")
                tgt_engine = get_target_engine()
                init_bronze_schema(tgt_engine, target_schema=TGT_DB_SCHEMA)
                init_checkpoint_manager(tgt_engine)
                init_monitoring_schema(tgt_engine)
                get_watermark_states(tgt_engine, tgt_schema=TGT_DB_SCHEMA)
            logger.info("Kết nối Source và Target Database đều sẵn sàng!")
            return
        except Exception as exc:
            err_msg = f"Lỗi kết nối Database khi preflight check: {exc}"
            logger.error(err_msg)
            send_alert_webhook(err_msg)
            sys.exit(1)
        finally:
            try:
                get_source_engine().dispose()
            except Exception:
                pass
            try:
                get_target_engine().dispose()
            except Exception:
                pass

    elif args.preflight_source:
        logger.info("Đang kiểm tra kết nối Source Database (SQL Server)...")
        try:
            test_source_connection()
            logger.info("Kết nối Source Database (SQL Server) sẵn sàng!")
            return
        except Exception as exc:
            err_msg = f"Lỗi kết nối Source Database (SQL Server): {exc}"
            logger.error(err_msg)
            send_alert_webhook(err_msg)
            sys.exit(1)
        finally:
            try:
                get_source_engine().dispose()
            except Exception:
                pass

    elif args.preflight_target:
        logger.info("Đang kiểm tra kết nối Target Database (PostgreSQL)...")
        try:
            test_target_connection()
            logger.info("Kết nối Target Database (PostgreSQL) sẵn sàng!")
            return
        except Exception as exc:
            err_msg = f"Lỗi kết nối Target Database (PostgreSQL): {exc}"
            logger.error(err_msg)
            send_alert_webhook(err_msg)
            sys.exit(1)
        finally:
            try:
                get_target_engine().dispose()
            except Exception:
                pass

    elif args.init_schema:
        logger.info("Đang khởi tạo độc lập schema Bronze và metadata (--init-schema)...")
        tgt_engine = None
        try:
            tgt_engine = get_target_engine()
            init_bronze_schema(tgt_engine, target_schema=TGT_DB_SCHEMA)
            init_checkpoint_manager(tgt_engine)
            init_monitoring_schema(tgt_engine)
            get_watermark_states(tgt_engine, tgt_schema=TGT_DB_SCHEMA)
            logger.info("Khởi tạo schema Bronze và metadata hoàn tất!")
            return
        except Exception as exc:
            err_msg = f"Lỗi khởi tạo schema Bronze (--init-schema): {exc}"
            logger.error(err_msg)
            send_alert_webhook(err_msg)
            sys.exit(1)
        finally:
            if tgt_engine is not None:
                try:
                    tgt_engine.dispose()
                except Exception:
                    pass

    elif args.validate_only:
        expected_tables = (
            [t for t in target_tables if t in REQUIRED_TABLES] if target_tables is not None else REQUIRED_TABLES
        )
        scope_label = "toàn bộ 7 bảng" if target_tables is None else target_tables
        logger.info(f"Đang chạy đối chiếu tầng Bronze với baseline ({scope_label})...")
        tgt_engine = None
        try:
            tgt_engine = get_target_engine()
            if args.run_id:
                source_baseline, pipeline_stats = _load_validation_snapshots(args.run_id, expected_tables)
            else:
                test_connections()
                active_checkpoints = get_active_checkpoints(tgt_engine)
                in_progress_tables = {
                    name for name, cp in active_checkpoints.items() if cp.get("status") == "IN_PROGRESS"
                }
                watermark_states = get_watermark_states(tgt_engine, tgt_schema=TGT_DB_SCHEMA)
                source_baseline = measure_source_baseline(
                    watermark_states=watermark_states,
                    batch_time=datetime.now(timezone.utc),
                    in_progress_tables=in_progress_tables,
                    target_tables=target_tables,
                )
                pipeline_stats = {}

            validate_staging_against_baseline(
                source_baseline=source_baseline,
                pipeline_stats=pipeline_stats,
                tgt_schema=TGT_DB_SCHEMA,
                tgt_engine=tgt_engine,
            )
            logger.info("Đối chiếu tầng Bronze hoàn tất!")
            return
        except Exception as exc:
            err_msg = f"Lỗi đối chiếu tầng Bronze (--validate-only): {exc}"
            logger.error(err_msg)
            send_alert_webhook(err_msg)
            sys.exit(1)
        finally:
            if not args.run_id:
                try:
                    get_source_engine().dispose()
                except Exception:
                    pass
            if tgt_engine is not None:
                try:
                    tgt_engine.dispose()
                except Exception:
                    pass

    pipeline_start = time.time()
    batch_time = datetime.now(timezone.utc)
    batch_id = batch_time.strftime("%Y%m%d_%H%M%S")
    pipeline_stats = {}

    target_schema = TGT_DB_SCHEMA
    tgt_engine = None
    sequential_mode = args.sequential or ETL_MAX_WORKERS <= 1
    expected_tables = (
        [t for t in target_tables if t in REQUIRED_TABLES] if target_tables is not None else REQUIRED_TABLES
    )
    scope_desc = f"bảng {target_tables}" if target_tables is not None else "toàn bộ 7 bảng"

    logger.info(f"BẮT ĐẦU CHẠY PIPELINE ({scope_desc} | BATCH ID: {batch_id})")

    try:
        test_connections()
        tgt_engine = get_target_engine()
        src_engine = get_source_engine()

        init_bronze_schema(tgt_engine, target_schema=target_schema)
        init_checkpoint_manager(tgt_engine)
        active_checkpoints = get_active_checkpoints(tgt_engine)
        in_progress_tables = {
            name for name, cp in active_checkpoints.items() if cp.get("status") == "IN_PROGRESS"
        }
        if in_progress_tables:
            logger.warning(
                f"Phát hiện bảng chưa hoàn tất ở lần chạy trước: {sorted(in_progress_tables)} "
                f"-> Kích hoạt chế độ resume (>= watermark)..."
            )

        watermark_states = get_watermark_states(tgt_engine, tgt_schema=target_schema)

        source_baseline = measure_source_baseline(
            watermark_states=watermark_states,
            batch_time=batch_time,
            in_progress_tables=in_progress_tables,
            target_tables=target_tables,
        )

        if sequential_mode:
            for logical_name, actual_name, rule, chunk_stream in extract_from_sqlserver(
                batch_time=batch_time,
                watermark_states=watermark_states,
                source_baseline=source_baseline,
                in_progress_tables=in_progress_tables,
                target_tables=target_tables,
            ):
                pipeline_stats[logical_name] = load_single_table(
                    logical_name, actual_name, rule, chunk_stream, tgt_engine, tgt_schema=target_schema
                )
        elif len(source_baseline) <= 1:
            for logical_name, base_info in source_baseline.items():
                _, stat = _process_single_table_worker(
                    logical_name,
                    base_info,
                    EXTRACT_PIPELINE_RULES.get(logical_name, {}),
                    watermark_states,
                    batch_time,
                    batch_id,
                    src_engine,
                    tgt_engine,
                    target_schema,
                    in_progress_tables,
                )
                if stat is not None:
                    pipeline_stats[logical_name] = stat
        else:
            logger.info(f"Kích hoạt trích xuất & nạp song song ({ETL_MAX_WORKERS} workers)...")
            with ThreadPoolExecutor(max_workers=min(ETL_MAX_WORKERS, len(source_baseline))) as executor:
                futures = [
                    executor.submit(
                        _process_single_table_worker,
                        logical_name,
                        base_info,
                        EXTRACT_PIPELINE_RULES.get(logical_name, {}),
                        watermark_states,
                        batch_time,
                        batch_id,
                        src_engine,
                        tgt_engine,
                        target_schema,
                        in_progress_tables,
                    )
                    for logical_name, base_info in source_baseline.items()
                ]
                for future in as_completed(futures):
                    logical_name, stat = future.result()
                    if stat is not None:
                        pipeline_stats[logical_name] = stat

        ensure_all_tables_loaded(pipeline_stats, required_tables=expected_tables)

        if args.run_id:
            _save_validation_snapshots(args.run_id, source_baseline, pipeline_stats)

        if not args.skip_validate:
            validate_staging_against_baseline(
                source_baseline=source_baseline,
                pipeline_stats=pipeline_stats,
                tgt_schema=target_schema,
                tgt_engine=tgt_engine,
            )

        mark_all_checkpoints_completed(tgt_engine, pipeline_stats=pipeline_stats, batch_id=batch_id)

        save_pipeline_run_history(
            tgt_engine=tgt_engine,
            tgt_schema=target_schema,
            batch_id=batch_id,
            pipeline_stats=pipeline_stats,
        )

        for name, stat in pipeline_stats.items():
            logger.debug(
                f"{name:<16} | {stat['target_table']:<32} | {str(stat['strategy']):<8} | "
                f"{stat['loaded_rows']:>12,} | {stat['elapsed_sec']:>8.2f}s | {stat['status']}"
            )

        total_rows = sum(s["loaded_rows"] for s in pipeline_stats.values())
        total_elapsed = time.time() - pipeline_start
        logger.info(
            f"TỔNG CỘNG BRONZE: {len(pipeline_stats)} bảng | {total_rows:,} dòng | Thời gian nạp: {total_elapsed:.2f}s"
        )
        logger.info("NẠP TẦNG BRONZE THÀNH CÔNG 100%!")

    except SchemaValidationError as sve:
        total_elapsed = time.time() - pipeline_start
        err_msg = (
            f"Lỗi Schema Contract: pipeline dừng có kiểm soát do thay đổi cấu trúc nguồn không hợp lệ "
            f"(sau {total_elapsed:.2f}s):\n{sve}"
        )
        logger.error(err_msg)
        send_alert_webhook(err_msg)
        sys.exit(1)
    except Exception as exc:
        total_elapsed = time.time() - pipeline_start
        err_msg = f"PIPELINE THẤT BẠI SAU {total_elapsed:.2f}s (Batch {batch_id}): {exc}"
        logger.error(err_msg)
        send_alert_webhook(err_msg)

        logger.warning(
            f"Dữ liệu các chunk đã commit được bảo toàn trong '{target_schema}'. "
            f"Checkpoint được lưu trong metadata, hệ thống sẽ tự động khôi phục ở lần chạy sau!"
        )
        sys.exit(1)
    finally:
        try:
            get_source_engine().dispose()
        except Exception:
            pass
        if tgt_engine is not None:
            try:
                tgt_engine.dispose()
            except Exception:
                pass


if __name__ == "__main__":
    main()
