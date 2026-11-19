from datetime import datetime, timezone
import sys
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


class TestPostgresLiveIntegration(unittest.TestCase):
    BRONZE_SCHEMA = "test_int_bronze"
    META_SCHEMA = "test_int_meta"

    def setUp(self):
        self.engine = get_target_engine()
        try:
            with self.engine.begin() as conn:
                conn.execute(text("SELECT 1;"))
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{self.BRONZE_SCHEMA}" CASCADE;'))
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{self.META_SCHEMA}" CASCADE;'))
        except Exception as exc:
            self.skipTest(f"PostgreSQL không khả dụng để chạy live integration test: {exc}")

    def tearDown(self):
        if getattr(self, "engine", None) is not None:
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f'DROP SCHEMA IF EXISTS "{self.BRONZE_SCHEMA}" CASCADE;'))
                    conn.execute(text(f'DROP SCHEMA IF EXISTS "{self.META_SCHEMA}" CASCADE;'))
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


if __name__ == "__main__":
    unittest.main()
