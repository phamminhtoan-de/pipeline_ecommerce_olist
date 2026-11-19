import unittest
from unittest.mock import patch


class TestStateAndAlert(unittest.TestCase):
    def test_checkpoint_schema_and_table_definitions(self):
        from src.state.state_manager import CHECKPOINT_SCHEMA, CHECKPOINT_TABLE, CHUNK_LOG_TABLE

        self.assertEqual(CHECKPOINT_SCHEMA, "etl_metadata")
        self.assertEqual(CHECKPOINT_TABLE, "_pipeline_checkpoints")
        self.assertEqual(CHUNK_LOG_TABLE, "_pipeline_chunk_checkpoints")

    def test_alert_webhook_respects_alert_on_script_error_flag(self):
        import src.core.alert as alert_mod

        with patch.object(alert_mod, "ALERT_ON_SCRIPT_ERROR", False), \
             patch.object(alert_mod, "ALERT_WEBHOOK_URL", "https://example.com/webhook"), \
             patch.object(alert_mod.urllib.request, "urlopen") as mock_urlopen:
            alert_mod.send_alert_webhook("test error")

        mock_urlopen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
