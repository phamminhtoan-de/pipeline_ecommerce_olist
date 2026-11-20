from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd
from sqlalchemy import inspect, text

import main
import src.state.state_manager as state_manager_mod
from src.config.settings import EXTRACT_PIPELINE_RULES
from src.core.connection import get_target_engine
from src.ingestion.extract import extract_table
from src.ingestion.load import ChunkLoadError, stream_load_table_to_postgres
from src.monitoring.collect_errors import collect_dbt_errors, evaluate_quality_gate
from src.monitoring.normalize_errors import (
    infer_layer,
    infer_table_name,
    infer_test_info,
    normalize_failure_row,
)
from src.schema.evolution import sync_target_schema_evolution
from src.schema.init_schema import init_bronze_schema
from src.state.history import save_pipeline_run_history
from src.state.state_manager import (
    get_active_checkpoints,
    init_checkpoint_manager,
    mark_all_checkpoints_completed,
)


class TestIntegrationPipeline(unittest.TestCase):
    def test_end_to_end_crash_recovery_and_resume_flow(self):
        orders_rule = {
            **EXTRACT_PIPELINE_RULES["orders"],
            "strategy": "watermark",
            "watermark_column": "order_purchase_timestamp",
            "default_start_watermark": "2018-01-01 00:00:00",
            "primary_keys": ["order_id"],
        }
        fake_baseline = {
            "orders": {
                "actual_table_name": "olist_orders_dataset",
                "expected_columns": ["order_id", "customer_id", "order_purchase_timestamp"],
                "source_row_count": 2,
                "expected_extract_rows": 2,
                "strategy": "watermark",
            }
        }
        patched_rules = {**EXTRACT_PIPELINE_RULES, "orders": orders_rule}

        # Run 1: Mid-stream ChunkLoadError -> pipeline stops safely, sends alert, does NOT mark checkpoint COMPLETED
        with patch.object(main, "EXTRACT_PIPELINE_RULES", patched_rules), \
             patch.object(sys, "argv", ["main.py", "--table", "orders"]), \
             patch.object(main, "test_connections"), \
             patch.object(main, "get_source_engine"), \
             patch.object(main, "get_target_engine"), \
             patch.object(main, "init_bronze_schema"), \
             patch.object(main, "init_checkpoint_manager"), \
             patch.object(main, "get_active_checkpoints", return_value={}), \
             patch.object(main, "get_watermark_states", return_value={"orders": "2018-01-01 00:00:00"}), \
             patch.object(main, "measure_source_baseline", return_value=fake_baseline), \
             patch.object(main, "extract_table", return_value=iter([])), \
             patch.object(
                 main,
                 "load_single_table",
                 side_effect=ChunkLoadError(
                     "Simulated chunk 2 failure",
                     batch_idx=2,
                     committed_rows=2,
                     last_checkpoint_wm="2018-01-01 00:00:00",
                 ),
             ), \
             patch.object(main, "mark_all_checkpoints_completed") as mock_mark_done_run1, \
             patch.object(main, "send_alert_webhook") as mock_alert_run1:
            with self.assertRaises(SystemExit) as ctx:
                main.main()
            self.assertEqual(ctx.exception.code, 1)

        mock_mark_done_run1.assert_not_called()
        mock_alert_run1.assert_called_once()

        # Run 2: Previous checkpoint is IN_PROGRESS -> worker builds >= inclusive resume query & completes run
        captured_queries = []

        def fake_extract_table(**kwargs):
            captured_queries.append(kwargs.get("query_str", ""))
            return iter([])

        success_stat = {
            "target_table": "olist_orders_dataset",
            "strategy": "watermark",
            "loaded_rows": 2,
            "elapsed_sec": 0.05,
            "status": "SUCCESS",
        }

        with patch.object(main, "EXTRACT_PIPELINE_RULES", patched_rules), \
             patch.object(sys, "argv", ["main.py", "--table", "orders"]), \
             patch.object(main, "test_connections"), \
             patch.object(main, "get_source_engine"), \
             patch.object(main, "get_target_engine"), \
             patch.object(main, "init_bronze_schema"), \
             patch.object(main, "init_checkpoint_manager"), \
             patch.object(
                 main,
                 "get_active_checkpoints",
                 return_value={"orders": {"status": "IN_PROGRESS", "last_chunk_index": 1}},
             ), \
             patch.object(main, "get_watermark_states", return_value={"orders": "2018-01-01 00:00:00"}), \
             patch.object(main, "measure_source_baseline", return_value=fake_baseline), \
             patch.object(main, "extract_table", side_effect=fake_extract_table), \
             patch.object(main, "load_single_table", return_value=success_stat), \
             patch.object(main, "validate_staging_against_baseline") as mock_validate_run2, \
             patch.object(main, "mark_all_checkpoints_completed") as mock_mark_done_run2, \
             patch.object(main, "save_pipeline_run_history") as mock_history_run2:
            main.main()

        self.assertEqual(len(captured_queries), 1)
        self.assertIn("[order_purchase_timestamp] >= '2018-01-01 00:00:00'", captured_queries[0])
        mock_validate_run2.assert_called_once()
        mock_mark_done_run2.assert_called_once()
        mock_history_run2.assert_called_once()

    def test_normalize_errors_metadata_and_full_row_jsonb(self):
        bronze_node = {
            "name": "source_unique_bronze_olist_customers_dataset_customer_id",
            "sources": [["bronze", "olist_customers_dataset"]],
            "column_name": "customer_id",
            "test_metadata": {"name": "unique", "kwargs": {"column_name": "customer_id"}},
        }
        self.assertEqual(infer_layer(bronze_node), "bronze")
        self.assertEqual(infer_table_name(bronze_node), "olist_customers_dataset")
        self.assertEqual(
            infer_test_info(bronze_node),
            ("unique", "customer_id", "Giá trị cột 'customer_id' bị trùng lặp"),
        )

        silver_node = {
            "name": "not_null_stg_orders_customer_id",
            "attached_node": "model.dbt_transform.stg_orders",
            "column_name": "customer_id",
            "test_metadata": {"name": "not_null", "kwargs": {"column_name": "customer_id"}},
            "refs": [{"name": "stg_orders"}],
        }
        norm_silver = normalize_failure_row(
            {"order_id": "o1", "customer_id": None, "_duplicate_count": 2},
            run_id="manual__test_1",
            layer=infer_layer(silver_node),
            node=silver_node,
            severity="error",
        )
        self.assertEqual(norm_silver["layer"], "silver")
        self.assertEqual(norm_silver["table_name"], "stg_orders")
        self.assertEqual(norm_silver["test_name"], "not_null")
        self.assertEqual(norm_silver["column_name"], "customer_id")
        self.assertNotIn("_duplicate_count", json.loads(norm_silver["row_data_json"]))
        self.assertEqual(json.loads(norm_silver["row_data_json"])["order_id"], "o1")

        gold_node = {
            "name": "assert_fact_orders_match_sales_amount",
            "refs": [{"name": "fact_sales"}, {"name": "fact_orders"}],
            "fqn": ["dbt_transform", "assert_fact_orders_match_sales_amount"],
        }
        self.assertEqual(infer_layer(gold_node), "gold")
        test_name, col_name, reason = infer_test_info(gold_node)
        self.assertEqual(test_name, "assert_fact_orders_match_sales_amount")
        self.assertIsNone(col_name)
        self.assertIn("fact_orders", reason)


class TestPostgresLiveIntegration(unittest.TestCase):
    BRONZE_SCHEMA = "test_int_bronze"
    META_SCHEMA = "test_int_meta"
    MONITORING_SCHEMA = "test_int_monitoring"
    FAILURE_SCHEMA = "test_int_failures"

    def setUp(self):
        self.engine = get_target_engine()
        try:
            with self.engine.begin() as conn:
                conn.execute(text("SELECT 1;"))
                for schema in (self.BRONZE_SCHEMA, self.META_SCHEMA, self.MONITORING_SCHEMA, self.FAILURE_SCHEMA):
                    conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;'))
        except Exception as exc:
            self.skipTest(f"PostgreSQL không khả dụng để chạy live integration test: {exc}")

    def tearDown(self):
        if getattr(self, "engine", None) is not None:
            try:
                with self.engine.begin() as conn:
                    for schema in (self.BRONZE_SCHEMA, self.META_SCHEMA, self.MONITORING_SCHEMA, self.FAILURE_SCHEMA):
                        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;'))
            except Exception:
                pass
            self.engine.dispose()

    def test_postgres_live_bronze_acid_checkpoint_and_schema_evolution(self):
        init_bronze_schema(self.engine, target_schema=self.BRONZE_SCHEMA)
        rule = {
            "strategy": "watermark",
            "watermark_column": "order_purchase_timestamp",
            "primary_keys": ["order_id"],
            "source_columns": [{"name": "loyalty_tier", "type": "VARCHAR(50)"}],
        }
        now_ts = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)

        def failing_chunk_stream():
            yield pd.DataFrame(
                [
                    {
                        "order_id": "ord_001",
                        "customer_id": "cust_001",
                        "order_status": "delivered",
                        "order_purchase_timestamp": "2018-01-01 10:00:00",
                        "_extracted_at": now_ts,
                        "_batch_id": "batch_int_001",
                    },
                    {
                        "order_id": "ord_002",
                        "customer_id": "cust_002",
                        "order_status": "shipped",
                        "order_purchase_timestamp": "2018-01-02 10:00:00",
                        "_extracted_at": now_ts,
                        "_batch_id": "batch_int_001",
                    },
                ]
            )
            raise RuntimeError("Simulated mid-stream crash on chunk 2")

        with patch.object(state_manager_mod, "CHECKPOINT_SCHEMA", self.META_SCHEMA):
            init_checkpoint_manager(self.engine)

            with self.assertRaises(ChunkLoadError):
                stream_load_table_to_postgres(
                    failing_chunk_stream(),
                    table_name="olist_orders_dataset",
                    strategy="watermark",
                    tgt_engine=self.engine,
                    tgt_schema=self.BRONZE_SCHEMA,
                    logical_name="orders",
                    rule=rule,
                )

            with self.engine.connect() as conn:
                committed_rows = conn.execute(
                    text(f'SELECT COUNT(*) FROM "{self.BRONZE_SCHEMA}"."olist_orders_dataset";')
                ).scalar()
            self.assertEqual(committed_rows, 2)

            active_checkpoints = get_active_checkpoints(self.engine)
            self.assertIn("orders", active_checkpoints)
            self.assertEqual(active_checkpoints["orders"]["status"], "IN_PROGRESS")

            # Resume run introduces a new source column 'loyalty_tier' -> schema evolution + completion
            resume_df = pd.DataFrame(
                [
                    {
                        "order_id": "ord_003",
                        "customer_id": "cust_003",
                        "order_status": "delivered",
                        "order_purchase_timestamp": "2018-01-03 10:00:00",
                        "loyalty_tier": "gold",
                        "_extracted_at": now_ts,
                        "_batch_id": "batch_int_002",
                    }
                ]
            )
            sync_target_schema_evolution(
                tgt_engine=self.engine,
                tgt_schema=self.BRONZE_SCHEMA,
                target_table="olist_orders_dataset",
                chunk_columns=list(resume_df.columns),
                source_columns_meta=rule["source_columns"],
            )

            def resume_chunk_stream():
                yield resume_df

            loaded_rows = stream_load_table_to_postgres(
                resume_chunk_stream(),
                table_name="olist_orders_dataset",
                strategy="watermark",
                tgt_engine=self.engine,
                tgt_schema=self.BRONZE_SCHEMA,
                logical_name="orders",
                rule={**rule, "resume_in_progress": True},
            )
            self.assertEqual(loaded_rows, 1)

            cols = {
                c["name"]
                for c in inspect(self.engine).get_columns("olist_orders_dataset", schema=self.BRONZE_SCHEMA)
            }
            self.assertIn("loyalty_tier", cols)

            pipeline_stats = {
                "orders": {
                    "source_table": "olist_orders_dataset",
                    "target_table": "olist_orders_dataset",
                    "strategy": "watermark",
                    "loaded_rows": loaded_rows,
                    "elapsed_sec": 0.1,
                    "status": "SUCCESS",
                }
            }
            mark_all_checkpoints_completed(self.engine, pipeline_stats=pipeline_stats, batch_id="batch_int_002")
            completed_checkpoints = get_active_checkpoints(self.engine)
            self.assertIn("orders", completed_checkpoints)
            self.assertEqual(completed_checkpoints["orders"]["status"], "COMPLETED")
            self.assertEqual(completed_checkpoints["orders"]["committed_rows"], 3)
            self.assertEqual(completed_checkpoints["orders"]["last_checkpoint"], "2018-01-03 10:00:00")

            save_pipeline_run_history(
                self.engine,
                tgt_schema=self.BRONZE_SCHEMA,
                batch_id="batch_int_002",
                pipeline_stats=pipeline_stats,
            )

            with self.engine.connect() as conn:
                total_rows = conn.execute(
                    text(f'SELECT COUNT(*) FROM "{self.BRONZE_SCHEMA}"."olist_orders_dataset";')
                ).scalar()
                evolved_val = conn.execute(
                    text(
                        f'SELECT loyalty_tier FROM "{self.BRONZE_SCHEMA}"."olist_orders_dataset" '
                        f"WHERE order_id = 'ord_003';"
                    )
                ).scalar()
                history_status = conn.execute(
                    text(
                        f'SELECT status FROM "{self.BRONZE_SCHEMA}"."_etl_run_history" '
                        f"WHERE batch_id = 'batch_int_002' AND logical_name = 'orders';"
                    )
                ).scalar()

            self.assertEqual(total_rows, 3)
            self.assertEqual(evolved_val, "gold")
            self.assertEqual(history_status, "SUCCESS")

    def test_postgres_live_monitoring_error_collector_and_quality_gate(self):
        with self.engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{self.FAILURE_SCHEMA}";'))
            conn.execute(
                text(
                    f"""
                    CREATE TABLE "{self.FAILURE_SCHEMA}".source_unique_customers (
                        customer_id VARCHAR(32),
                        customer_city VARCHAR(50),
                        _duplicate_count INT
                    );
                    INSERT INTO "{self.FAILURE_SCHEMA}".source_unique_customers VALUES
                        ('dup_c1', 'hanoi', 2);

                    CREATE TABLE "{self.FAILURE_SCHEMA}".not_null_stg_orders (
                        order_id VARCHAR(32),
                        customer_id VARCHAR(32),
                        order_status VARCHAR(20)
                    );
                    INSERT INTO "{self.FAILURE_SCHEMA}".not_null_stg_orders VALUES
                        ('o1001', NULL, 'delivered'),
                        ('o1002', NULL, 'shipped');
                    """
                )
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            target_dir = Path(tmp_dir)
            manifest = {
                "nodes": {
                    "test.dbt_transform.source_unique_customers": {
                        "name": "source_unique_customers",
                        "alias": "source_unique_customers",
                        "schema": self.FAILURE_SCHEMA,
                        "sources": [["bronze", "olist_customers_dataset"]],
                        "column_name": "customer_id",
                        "test_metadata": {"name": "unique", "kwargs": {"column_name": "customer_id"}},
                    },
                    "test.dbt_transform.not_null_stg_orders": {
                        "name": "not_null_stg_orders",
                        "alias": "not_null_stg_orders",
                        "schema": self.FAILURE_SCHEMA,
                        "attached_node": "model.dbt_transform.stg_orders",
                        "column_name": "customer_id",
                        "test_metadata": {"name": "not_null", "kwargs": {"column_name": "customer_id"}},
                        "refs": [{"name": "stg_orders"}],
                    },
                }
            }
            (target_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

            # Test Bronze warn collection -> Quality Gate should PASS
            bronze_run_results = {
                "results": [
                    {
                        "unique_id": "test.dbt_transform.source_unique_customers",
                        "status": "warn",
                        "failures": 1,
                    }
                ]
            }
            (target_dir / "run_results.json").write_text(json.dumps(bronze_run_results), encoding="utf-8")

            bronze_stats = collect_dbt_errors(
                target_dirs=[target_dir],
                run_id="run_int_001",
                layer="bronze",
                tgt_engine=self.engine,
                monitoring_schema=self.MONITORING_SCHEMA,
                failure_schema=self.FAILURE_SCHEMA,
            )
            self.assertEqual(bronze_stats["total_error_rows"], 1)
            self.assertEqual(bronze_stats["warn_tests_count"], 1)

            gate_bronze = evaluate_quality_gate(
                run_id="run_int_001",
                layer="bronze",
                target_dirs=[target_dir],
                tgt_engine=self.engine,
                monitoring_schema=self.MONITORING_SCHEMA,
            )
            self.assertEqual(gate_bronze["error_rows"], 0)
            self.assertEqual(gate_bronze["warn_rows"], 1)

            # Test Silver fail collection -> Quality Gate should FAIL and preserve 2 JSONB rows
            silver_run_results = {
                "results": [
                    {
                        "unique_id": "test.dbt_transform.not_null_stg_orders",
                        "status": "fail",
                        "failures": 2,
                    }
                ]
            }
            (target_dir / "run_results.json").write_text(json.dumps(silver_run_results), encoding="utf-8")

            silver_stats = collect_dbt_errors(
                target_dirs=[target_dir],
                run_id="run_int_001",
                layer="silver",
                tgt_engine=self.engine,
                monitoring_schema=self.MONITORING_SCHEMA,
                failure_schema=self.FAILURE_SCHEMA,
            )
            self.assertEqual(silver_stats["total_error_rows"], 2)
            self.assertEqual(silver_stats["error_tests_count"], 1)

            with self.assertRaises(RuntimeError) as ctx:
                evaluate_quality_gate(
                    run_id="run_int_001",
                    layer="silver",
                    target_dirs=[target_dir],
                    tgt_engine=self.engine,
                    monitoring_schema=self.MONITORING_SCHEMA,
                )
            self.assertIn("not_null", str(ctx.exception))

        with self.engine.connect() as conn:
            records = conn.execute(
                text(
                    f'SELECT layer, table_name, test_name, column_name, severity, row_data '
                    f'FROM "{self.MONITORING_SCHEMA}".error_records ORDER BY error_id;'
                )
            ).mappings().all()

        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]["layer"], "bronze")
        self.assertEqual(records[0]["table_name"], "olist_customers_dataset")
        self.assertEqual(records[0]["severity"], "warn")
        self.assertNotIn("_duplicate_count", records[0]["row_data"])
        self.assertEqual(records[0]["row_data"]["customer_city"], "hanoi")

        self.assertEqual(records[1]["layer"], "silver")
        self.assertEqual(records[1]["table_name"], "stg_orders")
        self.assertEqual(records[1]["severity"], "error")
        self.assertEqual(records[1]["row_data"]["order_id"], "o1001")
        self.assertEqual(records[2]["row_data"]["order_id"], "o1002")


if __name__ == "__main__":
    unittest.main()
