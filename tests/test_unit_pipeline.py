import ast
from pathlib import Path
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from src.ingestion.extract import build_dynamic_query, resolve_table_name
from src.schema.contract import (
    SchemaValidationError,
    parse_columns_config,
    resolve_and_validate_columns,
)
from src.schema.evolution import map_source_type_to_postgres


class TestSchemaContractAndEvolution(unittest.TestCase):
    def test_resolve_table_name_case_insensitive(self):
        db_tables = {"Olist_Orders_Dataset", "olist_customers_dataset"}
        actual = resolve_table_name(db_tables, ["orders", "olist_orders_dataset"])
        self.assertEqual(actual, "Olist_Orders_Dataset")

    def test_missing_primary_key_raises_schema_validation_error(self):
        rule = {
            "strategy": "full",
            "primary_keys": ["order_id"],
            "columns": {"mode": "all"},
        }
        source_columns = [{"name": "customer_id"}, {"name": "order_status"}]
        with self.assertRaises(SchemaValidationError):
            resolve_and_validate_columns("orders", "olist_orders_dataset", rule, source_columns)

    def test_exclude_primary_key_is_forbidden(self):
        rule = {
            "strategy": "full",
            "primary_keys": ["order_id"],
            "columns": {"mode": "exclude", "list": ["order_id"]},
        }
        source_columns = [{"name": "order_id"}, {"name": "order_status"}]
        with self.assertRaises(SchemaValidationError):
            resolve_and_validate_columns("orders", "olist_orders_dataset", rule, source_columns)

    def test_selected_mode_auto_includes_primary_key(self):
        rule = {
            "strategy": "full",
            "primary_keys": ["order_id"],
            "columns": {"mode": "selected", "list": ["order_status"]},
        }
        source_columns = [{"name": "order_id"}, {"name": "order_status"}, {"name": "customer_id"}]
        resolved_cols, mode = resolve_and_validate_columns(
            "orders", "olist_orders_dataset", rule, source_columns
        )
        self.assertEqual(mode, "selected")
        self.assertIn("order_id", resolved_cols)
        self.assertIn("order_status", resolved_cols)
        self.assertNotIn("customer_id", resolved_cols)

    def test_map_source_type_to_postgres(self):
        self.assertEqual(map_source_type_to_postgres("id", {"type": "BIGINT"}), "BIGINT")
        self.assertEqual(map_source_type_to_postgres("qty", {"type": "INT"}), "INTEGER")
        self.assertEqual(map_source_type_to_postgres("price", {"type": "DECIMAL(10, 2)"}), "NUMERIC")
        self.assertEqual(map_source_type_to_postgres("ts", {"type": "DATETIME2"}), "TIMESTAMP")
        self.assertEqual(map_source_type_to_postgres("active", {"type": "BIT"}), "BOOLEAN")

    def test_build_dynamic_query_watermark_and_date_range(self):
        wm_rule = {
            "strategy": "watermark",
            "watermark_column": "order_purchase_timestamp",
            "default_start_watermark": "2017-01-01 00:00:00",
        }
        q_wm = build_dynamic_query(
            logical_name="orders",
            sql_table_name="olist_orders_dataset",
            rule=wm_rule,
            last_watermark="2018-01-01 00:00:00",
            resolved_columns=["order_id", "order_purchase_timestamp"],
            batch_time=datetime(2018, 6, 1, 12, 0, 0),
        )
        self.assertIn("[order_purchase_timestamp] > '2018-01-01 00:00:00'", q_wm)
        self.assertIn("AND [order_purchase_timestamp] <= '2018-06-01 12:00:00'", q_wm)
        self.assertIn("ORDER BY [order_purchase_timestamp] ASC", q_wm)

        dr_rule = {
            "strategy": "date_range",
            "watermark_column": "order_purchase_timestamp",
            "start_date": "2017-01-01",
            "end_date": "2018-01-01",
        }
        q_dr = build_dynamic_query(
            logical_name="orders",
            sql_table_name="olist_orders_dataset",
            rule=dr_rule,
            resolved_columns=["order_id", "order_purchase_timestamp"],
        )
        self.assertIn("[order_purchase_timestamp] >= '2017-01-01' AND [order_purchase_timestamp] < '2018-01-01'", q_dr)

    def test_checkpoint_schema_and_table_definitions(self):
        from src.state.state_manager import CHECKPOINT_SCHEMA, CHECKPOINT_TABLE, CHUNK_LOG_TABLE
        self.assertEqual(CHECKPOINT_SCHEMA, "etl_metadata")
        self.assertEqual(CHECKPOINT_TABLE, "_pipeline_checkpoints")
        self.assertEqual(CHUNK_LOG_TABLE, "_pipeline_chunk_checkpoints")


class TestAirflowOrchestration(unittest.TestCase):
    def test_airflow_dag_ast_structure_and_tasks(self):
        dag_path = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "olist_medallion_elt.py"
        source = dag_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(dag_path))

        dag_call = None
        task_ids = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func_name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
                if func_name == "DAG":
                    dag_call = node
                elif func_name == "BashOperator":
                    for kw in node.keywords:
                        if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                            task_ids.append(kw.value.value)

        self.assertIsNotNone(dag_call, "Không tìm thấy khai báo DAG trong olist_medallion_elt.py")
        dag_kwargs = {
            kw.arg: kw.value.value
            for kw in dag_call.keywords
            if kw.arg and isinstance(kw.value, ast.Constant)
        }
        self.assertEqual(dag_kwargs.get("dag_id"), "olist_medallion_elt")
        self.assertEqual(dag_kwargs.get("schedule"), "0 2 * * *")
        self.assertIs(dag_kwargs.get("catchup"), False)
        self.assertEqual(dag_kwargs.get("max_active_runs"), 1)

        expected_tasks = [
            "preflight_check",
            "extract_load_bronze",
            "dbt_silver",
            "dbt_test_silver",
            "dbt_snapshot",
            "dbt_gold",
            "dbt_test_gold",
        ]
        self.assertEqual(task_ids, expected_tasks)

        expected_chain = " >> ".join(expected_tasks)
        compact_source = " ".join(source.split())
        self.assertIn(expected_chain, compact_source)
        self.assertIn("dbt.cli.main", source)
        self.assertNotIn("src.transformation.dbt_runner", source)

    def test_main_preflight_checks_connections_and_exits_early(self):
        import main

        with patch.object(sys, "argv", ["main.py", "--preflight"]), \
             patch.object(main, "test_connections") as mock_test_conn, \
             patch.object(main, "init_bronze_schema") as mock_init_schema, \
             patch.object(main, "get_source_engine") as mock_src_engine, \
             patch.object(main, "get_target_engine") as mock_tgt_engine:
            main.main()

        mock_test_conn.assert_called_once()
        mock_init_schema.assert_not_called()
        mock_src_engine.return_value.dispose.assert_called_once()
        mock_tgt_engine.return_value.dispose.assert_called_once()

    def test_build_dynamic_query_resume_and_deterministic_order_by(self):
        wm_rule = {
            "strategy": "watermark",
            "watermark_column": "order_purchase_timestamp",
            "default_start_watermark": "2017-01-01 00:00:00",
            "primary_keys": ["order_id"],
            "resume_in_progress": True,
        }
        q_resume = build_dynamic_query(
            logical_name="orders",
            sql_table_name="olist_orders_dataset",
            rule=wm_rule,
            last_watermark="2018-01-01 00:00:00",
            resolved_columns=["order_id", "order_purchase_timestamp"],
            batch_time=datetime(2018, 6, 1, 12, 0, 0),
        )
        self.assertIn("[order_purchase_timestamp] >= '2018-01-01 00:00:00'", q_resume)
        self.assertIn("ORDER BY [order_purchase_timestamp] ASC, [order_id] ASC", q_resume)

    def test_build_dynamic_query_timezone_aware_cutoff(self):
        import src.ingestion.extract as extract_mod

        wm_rule = {
            "strategy": "watermark",
            "watermark_column": "order_purchase_timestamp",
            "default_start_watermark": "2017-01-01 00:00:00",
            "primary_keys": ["order_id"],
        }
        with patch.object(extract_mod, "SRC_DB_TIMEZONE", "Asia/Ho_Chi_Minh"):
            q_tz = build_dynamic_query(
                logical_name="orders",
                sql_table_name="olist_orders_dataset",
                rule=wm_rule,
                last_watermark="2026-01-01 00:00:00",
                resolved_columns=["order_id", "order_purchase_timestamp"],
                batch_time=datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
            )
        self.assertIn("AND [order_purchase_timestamp] <= '2026-06-01 19:00:00'", q_tz)

    def test_alert_webhook_respects_alert_on_script_error_flag(self):
        import src.core.alert as alert_mod

        with patch.object(alert_mod, "ALERT_ON_SCRIPT_ERROR", False), \
             patch.object(alert_mod, "ALERT_WEBHOOK_URL", "https://example.com/webhook"), \
             patch.object(alert_mod.urllib.request, "urlopen") as mock_urlopen:
            alert_mod.send_alert_webhook("test error")

        mock_urlopen.assert_not_called()


if __name__ == "__main__":
    unittest.main()

