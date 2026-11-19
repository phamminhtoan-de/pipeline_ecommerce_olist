import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from src.ingestion.extract import build_dynamic_query


class TestExtractQueryBuilder(unittest.TestCase):
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
        self.assertIn(
            "[order_purchase_timestamp] >= '2017-01-01' AND [order_purchase_timestamp] < '2018-01-01'",
            q_dr,
        )

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


if __name__ == "__main__":
    unittest.main()
