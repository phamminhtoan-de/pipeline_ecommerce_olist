import ast
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


class TestAirflowOrchestration(unittest.TestCase):
    def test_airflow_dag_ast_structure_and_tasks(self):
        dag_path = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "olist_medallion_elt.py"
        source = dag_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(dag_path))

        dag_call = None
        task_ids = []
        group_ids = []
        constants = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                name = node.targets[0].id
                if name in {
                    "BRONZE_TABLES",
                    "SILVER_MODELS",
                    "GOLD_DIM_BASE_MODELS",
                    "GOLD_DIM_DEPENDENT_MODELS",
                    "GOLD_FACT_MODELS",
                }:
                    constants[name] = ast.literal_eval(node.value)
            elif isinstance(node, ast.Call):
                func_name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
                if func_name == "DAG":
                    dag_call = node
                elif func_name == "BashOperator":
                    for kw in node.keywords:
                        if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                            task_ids.append(kw.value.value)
                elif func_name == "TaskGroup":
                    for kw in node.keywords:
                        if kw.arg == "group_id" and isinstance(kw.value, ast.Constant):
                            group_ids.append(kw.value.value)

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
        self.assertEqual(dag_kwargs.get("max_active_tasks"), 3)

        self.assertEqual(group_ids, ["extract_load_bronze", "dbt_silver", "dbt_gold"])
        self.assertEqual(
            task_ids,
            [
                "check_connections",
                "init_bronze_schema",
                "dbt_deps",
                "validate_bronze",
                "dbt_test_bronze_source",
                "dbt_test_silver",
                "dbt_snapshot",
                "dbt_test_gold_schema",
                "dbt_test_business_rules",
            ],
        )
        self.assertEqual(
            constants.get("BRONZE_TABLES"),
            ["customers", "orders", "order_items", "order_payments", "order_reviews", "products", "sellers"],
        )
        self.assertEqual(
            constants.get("SILVER_MODELS"),
            [
                "stg_customers",
                "stg_orders",
                "stg_order_items",
                "stg_order_payments",
                "stg_order_reviews",
                "stg_products",
                "stg_sellers",
            ],
        )
        self.assertEqual(constants.get("GOLD_DIM_BASE_MODELS"), ["dim_date", "dim_location", "dim_product"])
        self.assertEqual(constants.get("GOLD_DIM_DEPENDENT_MODELS"), ["dim_customer", "dim_seller"])
        self.assertEqual(constants.get("GOLD_FACT_MODELS"), ["fact_orders", "fact_sales"])

        compact_source = " ".join(source.split())
        for dep_edge in (
            "check_connections >> [init_bronze_schema, dbt_deps]",
            "init_bronze_schema >> extract_load_bronze",
            "extract_load_bronze >> validate_bronze",
            "[extract_load_bronze, dbt_deps] >> dbt_test_bronze_source",
            "[validate_bronze, dbt_test_bronze_source] >> dbt_silver",
            "dbt_silver >> dbt_test_silver >> dbt_snapshot >> dbt_gold",
            "dbt_gold >> [dbt_test_gold_schema, dbt_test_business_rules]",
        ):
            self.assertIn(dep_edge, compact_source)

        for expected_flag in (
            "--preflight",
            "--init-schema",
            "--table {table_name} --skip-validate --run-id",
            "--validate-only --run-id",
            "--target-path /tmp/dbt_{model_name}",
            "--select source:* --target-path /tmp/dbt_test_bronze_source",
            "--select staging --exclude source:* --target-path /tmp/dbt_test_silver",
            "--select marts,test_type:generic --target-path /tmp/dbt_test_gold_schema",
            "--select test_type:singular --target-path /tmp/dbt_test_business_rules",
        ):
            self.assertIn(expected_flag, source)

        self.assertIn('gold_tasks["dim_location"] >> [gold_tasks["dim_customer"], gold_tasks["dim_seller"]]', compact_source)
        self.assertIn('>> gold_tasks["fact_orders"]', compact_source)
        self.assertIn('>> gold_tasks["fact_sales"]', compact_source)
        self.assertIn("dbt.cli.main", source)
        self.assertNotIn("src.transformation.dbt_runner", source)
        self.assertIn("retry_exponential_backoff=True", source)

    def test_connection_delegates_retry_to_airflow(self):
        import src.core.connection as conn_mod

        self.assertFalse(hasattr(conn_mod, "retry_on_db_error"))
        self.assertTrue(callable(conn_mod.test_connections))

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

    def test_main_preflight_init_schema_initializes_bronze_and_metadata(self):
        import main

        with patch.object(sys, "argv", ["main.py", "--preflight", "--init-schema"]), \
             patch.object(main, "test_connections") as mock_test_conn, \
             patch.object(main, "init_bronze_schema") as mock_init_schema, \
             patch.object(main, "init_checkpoint_manager") as mock_init_ckpt, \
             patch.object(main, "get_watermark_states") as mock_get_wm, \
             patch.object(main, "get_source_engine") as mock_src_engine, \
             patch.object(main, "get_target_engine") as mock_tgt_engine:
            main.main()

        mock_test_conn.assert_called_once()
        mock_init_schema.assert_called_once()
        mock_init_ckpt.assert_called_once()
        mock_get_wm.assert_called_once()
        mock_src_engine.return_value.dispose.assert_called_once()
        mock_tgt_engine.return_value.dispose.assert_called_once()

    def test_main_single_table_flag_filters_execution(self):
        import main

        fake_baseline = {
            "orders": {
                "actual_table_name": "olist_orders_dataset",
                "expected_columns": ["order_id", "customer_id"],
                "source_row_count": 5,
                "expected_extract_rows": 5,
                "strategy": "lookback_window",
            }
        }
        fake_stat = {
            "target_table": "olist_orders_dataset",
            "strategy": "lookback_window",
            "loaded_rows": 5,
            "elapsed_sec": 0.1,
            "status": "SUCCESS",
        }

        with patch.object(sys, "argv", ["main.py", "--table", "orders"]), \
             patch.object(main, "test_connections"), \
             patch.object(main, "get_source_engine"), \
             patch.object(main, "get_target_engine"), \
             patch.object(main, "init_bronze_schema"), \
             patch.object(main, "init_checkpoint_manager"), \
             patch.object(main, "get_active_checkpoints", return_value={}), \
             patch.object(main, "get_watermark_states", return_value={}), \
             patch.object(main, "measure_source_baseline", return_value=fake_baseline) as mock_baseline, \
             patch.object(main, "_process_single_table_worker", return_value=("orders", fake_stat)) as mock_worker, \
             patch.object(main, "validate_staging_against_baseline") as mock_validate, \
             patch.object(main, "mark_all_checkpoints_completed") as mock_mark_done, \
             patch.object(main, "save_pipeline_run_history") as mock_history:
            main.main()

        self.assertEqual(mock_baseline.call_args.kwargs["target_tables"], ["orders"])
        mock_worker.assert_called_once()
        mock_validate.assert_called_once()
        self.assertEqual(mock_mark_done.call_args.kwargs["pipeline_stats"], {"orders": fake_stat})
        self.assertEqual(mock_history.call_args.kwargs["pipeline_stats"], {"orders": fake_stat})

        with patch.object(sys, "argv", ["main.py", "--table", "unknown_table"]):
            with self.assertRaises(SystemExit) as ctx:
                main.main()
            self.assertEqual(ctx.exception.code, 2)

    def test_main_init_schema_standalone_exits_early(self):
        import main

        with patch.object(sys, "argv", ["main.py", "--init-schema"]), \
             patch.object(main, "test_connections") as mock_test_conn, \
             patch.object(main, "init_bronze_schema") as mock_init_schema, \
             patch.object(main, "init_checkpoint_manager") as mock_init_ckpt, \
             patch.object(main, "get_watermark_states") as mock_get_wm, \
             patch.object(main, "get_target_engine") as mock_tgt_engine:
            main.main()

        mock_test_conn.assert_not_called()
        mock_init_schema.assert_called_once()
        mock_init_ckpt.assert_called_once()
        mock_get_wm.assert_called_once()
        mock_tgt_engine.return_value.dispose.assert_called_once()

    def test_main_skip_validate_and_validate_only_roundtrip(self):
        import main

        run_id = "manual__2026-11-19T16:00:00+00:00"
        fake_baseline = {
            "orders": {
                "actual_table_name": "olist_orders_dataset",
                "target_table_name": "olist_orders_dataset",
                "strategy": "lookback_window",
                "source_row_count": 5,
                "expected_extract_rows": 5,
                "expected_columns": ["order_id", "customer_id"],
                "source_columns_meta": [{"name": "order_id", "type": object()}],
                "date_range": {"column": "order_purchase_timestamp", "min": "2018-01-01", "max": "2018-01-02"},
            }
        }
        fake_stat = {
            "target_table": "olist_orders_dataset",
            "strategy": "lookback_window",
            "loaded_rows": 5,
            "elapsed_sec": 0.1,
            "status": "SUCCESS",
        }

        with tempfile.TemporaryDirectory() as tmp_dir, patch.object(main, "LOGS_DIR", Path(tmp_dir)):
            with patch.object(
                sys, "argv", ["main.py", "--table", "orders", "--skip-validate", "--run-id", run_id]
            ), \
                 patch.object(main, "test_connections"), \
                 patch.object(main, "get_source_engine"), \
                 patch.object(main, "get_target_engine"), \
                 patch.object(main, "init_bronze_schema"), \
                 patch.object(main, "init_checkpoint_manager"), \
                 patch.object(main, "get_active_checkpoints", return_value={}), \
                 patch.object(main, "get_watermark_states", return_value={}), \
                 patch.object(main, "measure_source_baseline", return_value=fake_baseline), \
                 patch.object(main, "_process_single_table_worker", return_value=("orders", fake_stat)), \
                 patch.object(main, "validate_staging_against_baseline") as mock_validate_el, \
                 patch.object(main, "mark_all_checkpoints_completed"), \
                 patch.object(main, "save_pipeline_run_history"):
                main.main()

            mock_validate_el.assert_not_called()
            snapshot_file = Path(tmp_dir) / "run_state" / "manual__2026-11-19T16_00_00_00_00" / "orders.json"
            self.assertTrue(snapshot_file.exists())

            with patch.object(
                sys, "argv", ["main.py", "--validate-only", "--table", "orders", "--run-id", run_id]
            ), \
                 patch.object(main, "get_target_engine") as mock_tgt_engine, \
                 patch.object(main, "validate_staging_against_baseline") as mock_validate_only:
                main.main()

            mock_validate_only.assert_called_once()
            passed_baseline = mock_validate_only.call_args.kwargs["source_baseline"]
            passed_stats = mock_validate_only.call_args.kwargs["pipeline_stats"]
            self.assertEqual(passed_baseline["orders"]["source_row_count"], 5)
            self.assertEqual(passed_baseline["orders"]["expected_columns"], ["order_id", "customer_id"])
            self.assertEqual(passed_stats, {"orders": fake_stat})
            mock_tgt_engine.return_value.dispose.assert_called_once()


if __name__ == "__main__":
    unittest.main()
