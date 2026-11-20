import argparse
import json
from pathlib import Path
import sys
from sqlalchemy import inspect, text
from src.config.settings import ERROR_RECORDS_TABLE, MONITORING_SCHEMA
from src.core.connection import get_target_engine
from src.core.logger import get_logger
from src.monitoring.normalize_errors import infer_layer, normalize_failure_row

logger = get_logger(__name__)


def init_monitoring_schema(tgt_engine, monitoring_schema=MONITORING_SCHEMA):
    ddl_sql = text(
        f"""
        CREATE SCHEMA IF NOT EXISTS "{monitoring_schema}";

        CREATE TABLE IF NOT EXISTS "{monitoring_schema}"."{ERROR_RECORDS_TABLE}" (
            error_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            run_id VARCHAR(255) NOT NULL,
            layer VARCHAR(20) NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),
            table_name VARCHAR(255) NOT NULL,
            test_name VARCHAR(255) NOT NULL,
            column_name VARCHAR(255),
            error_reason TEXT NOT NULL,
            severity VARCHAR(20) NOT NULL DEFAULT 'error',
            row_data JSONB NOT NULL,
            detected_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_error_records_run_layer
            ON "{monitoring_schema}"."{ERROR_RECORDS_TABLE}" (run_id, layer);

        CREATE INDEX IF NOT EXISTS idx_error_records_layer_table
            ON "{monitoring_schema}"."{ERROR_RECORDS_TABLE}" (layer, table_name);
        """
    )
    with tgt_engine.begin() as conn:
        conn.execute(ddl_sql)
    logger.debug(f"Đã sẵn sàng bảng giám sát lỗi '{monitoring_schema}.{ERROR_RECORDS_TABLE}'.")


def _resolve_failure_relation(node, failure_schema=None):
    rel_schema = failure_schema or node.get("schema") or "test_failures"
    rel_table = node.get("alias") or node.get("name")
    if not rel_table and node.get("relation_name"):
        parts = [p.strip('"') for p in str(node["relation_name"]).split(".") if p.strip('"')]
        if len(parts) >= 2:
            rel_schema = failure_schema or parts[-2]
            rel_table = parts[-1]
    return rel_schema, rel_table


def collect_dbt_errors(
    target_dirs,
    run_id,
    layer=None,
    tgt_engine=None,
    monitoring_schema=MONITORING_SCHEMA,
    failure_schema=None,
):
    tgt_engine = tgt_engine or get_target_engine()
    init_monitoring_schema(tgt_engine, monitoring_schema=monitoring_schema)

    insert_sql = text(
        f"""
        INSERT INTO "{monitoring_schema}"."{ERROR_RECORDS_TABLE}"
            (run_id, layer, table_name, test_name, column_name, error_reason, severity, row_data)
        VALUES
            (:run_id, :layer, :table_name, :test_name, :column_name, :error_reason, :severity, CAST(:row_data_json AS JSONB));
        """
    )

    total_error_rows = 0
    error_tests_count = 0
    warn_tests_count = 0

    with tgt_engine.begin() as conn:
        if layer:
            conn.execute(
                text(
                    f'DELETE FROM "{monitoring_schema}"."{ERROR_RECORDS_TABLE}" '
                    "WHERE run_id = :run_id AND layer = :layer;"
                ),
                {"run_id": str(run_id), "layer": layer},
            )
        else:
            conn.execute(
                text(
                    f'DELETE FROM "{monitoring_schema}"."{ERROR_RECORDS_TABLE}" '
                    "WHERE run_id = :run_id;"
                ),
                {"run_id": str(run_id)},
            )

        inspector = inspect(conn)
        for dir_item in target_dirs:
            t_dir = Path(dir_item)
            results_path = t_dir / "run_results.json"
            manifest_path = t_dir / "manifest.json"
            if not results_path.exists() or not manifest_path.exists():
                logger.warning(f"Không tìm thấy run_results.json hoặc manifest.json tại '{t_dir}'.")
                continue

            run_results = json.loads(results_path.read_text(encoding="utf-8"))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            nodes = manifest.get("nodes", {})

            for res in run_results.get("results", []):
                status = str(res.get("status") or "").lower()
                if status not in ("fail", "warn", "error"):
                    continue

                severity = "warn" if status == "warn" else "error"
                if severity == "warn":
                    warn_tests_count += 1
                else:
                    error_tests_count += 1

                unique_id = res.get("unique_id", "")
                node = nodes.get(unique_id, {})
                resolved_layer = infer_layer(node, fallback_layer=layer)
                rel_schema, rel_table = _resolve_failure_relation(node, failure_schema=failure_schema)

                normalized_batch = []
                failures_count = res.get("failures") or 0
                if failures_count > 0 and rel_table and inspector.has_table(rel_table, schema=rel_schema):
                    rows = conn.execute(text(f'SELECT * FROM "{rel_schema}"."{rel_table}";')).mappings().all()
                    for r in rows:
                        normalized_batch.append(
                            normalize_failure_row(
                                dict(r),
                                run_id=run_id,
                                layer=resolved_layer,
                                node=node,
                                severity=severity,
                            )
                        )

                if status in ("fail", "error") and not normalized_batch:
                    fallback_row = {"message": res.get("message") or f"dbt test {status}"}
                    normalized_batch.append(
                        normalize_failure_row(
                            fallback_row,
                            run_id=run_id,
                            layer=resolved_layer,
                            node=node,
                            severity="error",
                        )
                    )

                if normalized_batch:
                    conn.execute(insert_sql, normalized_batch)
                    total_error_rows += len(normalized_batch)

    logger.info(
        f"Thu thập lỗi dbt hoàn tất (run_id='{run_id}', layer='{layer or 'all'}'): "
        f"{total_error_rows} dòng vi phạm ({error_tests_count} test fail/error, {warn_tests_count} test warn)."
    )
    return {
        "run_id": str(run_id),
        "layer": layer,
        "total_error_rows": total_error_rows,
        "error_tests_count": error_tests_count,
        "warn_tests_count": warn_tests_count,
    }


def evaluate_quality_gate(
    run_id,
    layer,
    target_dirs=None,
    tgt_engine=None,
    monitoring_schema=MONITORING_SCHEMA,
):
    if target_dirs:
        missing = [str(d) for d in target_dirs if not (Path(d) / "run_results.json").exists()]
        if missing:
            raise RuntimeError(
                f"Quality Gate tầng '{layer}' dừng: thiếu run_results.json tại {missing} "
                f"(tiến trình dbt test trước đó bị lỗi hệ thống)."
            )

    tgt_engine = tgt_engine or get_target_engine()
    init_monitoring_schema(tgt_engine, monitoring_schema=monitoring_schema)

    summary_sql = text(
        f"""
        SELECT table_name, test_name, severity, error_reason, COUNT(*) AS cnt
        FROM "{monitoring_schema}"."{ERROR_RECORDS_TABLE}"
        WHERE run_id = :run_id AND layer = :layer
        GROUP BY table_name, test_name, severity, error_reason
        ORDER BY severity DESC, cnt DESC, table_name;
        """
    )

    with tgt_engine.connect() as conn:
        rows = conn.execute(summary_sql, {"run_id": str(run_id), "layer": layer}).mappings().all()

    error_rows = 0
    warn_rows = 0
    error_details = []

    for r in rows:
        cnt = int(r["cnt"])
        if r["severity"] == "error":
            error_rows += cnt
            error_details.append(
                f"  - [{r['table_name']}.{r['test_name']}] {cnt} dòng: {r['error_reason']}"
            )
        else:
            warn_rows += cnt
            logger.warning(
                f"Quality Gate cảnh báo (layer='{layer}'): "
                f"[{r['table_name']}.{r['test_name']}] {cnt} dòng -> {r['error_reason']}"
            )

    if error_rows > 0:
        raise RuntimeError(
            f"Quality Gate tầng '{layer}' KHÔNG ĐẠT (run_id='{run_id}'): "
            f"phát hiện {error_rows} dòng lỗi mức ERROR (và {warn_rows} dòng WARN), "
            f"đã lưu chi tiết vào '{monitoring_schema}.{ERROR_RECORDS_TABLE}':\n"
            + "\n".join(error_details)
        )

    logger.info(
        f"Quality Gate tầng '{layer}' ĐẠT (run_id='{run_id}'): "
        f"0 dòng lỗi nghiêm trọng ({warn_rows} dòng cảnh báo)."
    )
    return {
        "run_id": str(run_id),
        "layer": layer,
        "error_rows": error_rows,
        "warn_rows": warn_rows,
    }


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Thu thập lỗi dbt test vào monitoring.error_records")
    parser.add_argument("--collect", action="store_true", help="Thu thập các lỗi từ dbt run_results/test_failures")
    parser.add_argument("--quality-gate", action="store_true", help="Kiểm tra ngưỡng lỗi để quyết định PASS/FAIL")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], default=None)
    parser.add_argument("--run-id", required=True, help="Định danh run_id của pipeline/Airflow")
    parser.add_argument("--target-dir", nargs="+", required=True, help="Danh sách thư mục --target-path của dbt test")
    return parser.parse_args(argv)


def main():
    args = _parse_args(sys.argv[1:])
    if not args.collect and not args.quality_gate:
        logger.error("Cần truyền ít nhất một cờ --collect hoặc --quality-gate.")
        sys.exit(2)

    tgt_engine = get_target_engine()
    try:
        if args.collect:
            collect_dbt_errors(
                target_dirs=args.target_dir,
                run_id=args.run_id,
                layer=args.layer,
                tgt_engine=tgt_engine,
            )
        if args.quality_gate:
            if not args.layer:
                logger.error("Cần truyền --layer khi chạy --quality-gate.")
                sys.exit(2)
            evaluate_quality_gate(
                run_id=args.run_id,
                layer=args.layer,
                target_dirs=args.target_dir,
                tgt_engine=tgt_engine,
            )
    except Exception as exc:
        logger.error(f"Lỗi tại monitoring collector/quality gate: {exc}")
        sys.exit(1)
    finally:
        tgt_engine.dispose()


if __name__ == "__main__":
    main()
