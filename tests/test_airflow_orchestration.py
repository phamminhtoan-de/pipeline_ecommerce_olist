import ast
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


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


if __name__ == "__main__":
    unittest.main()
