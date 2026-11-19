import unittest
from src.ingestion.extract import resolve_table_name
from src.schema.contract import (
    SchemaValidationError,
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


if __name__ == "__main__":
    unittest.main()
