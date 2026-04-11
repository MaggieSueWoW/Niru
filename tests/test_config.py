import os
from pathlib import Path
import tempfile
import unittest

from niru.config import load_settings


class ConfigTests(unittest.TestCase):
    def test_load_settings_reads_active_polling_config(self) -> None:
        config_text = """
google:
  raw_tab_name: "raw_data"
  roster_column: "A"
  roster_start_row: 2
  output_start_cell: "C1"

team_activity:
  enabled: true
  window_weeks: 2
  start_hour: 7
  output_start_cell: "C101"

sync:
  interval_minutes: 15
  active_interval_minutes: 5
  active_idle_minutes: 40
  predictive_hot_enabled: true
  predictive_hot_threshold: 0.5
  current_season: "season-mn-1"
  max_players_per_cycle: 250
  failure_backoff_seconds: 30
  max_failure_backoff_seconds: 900
  failure_backoff_jitter_seconds: 5

raiderio:
  base_url: "https://raider.io/api/v1"
  access_key_enabled: false
  requests_per_minute_cap: 60
  timeout_seconds: 30
  retry_attempts: 4
  backoff_seconds: 2.0
  circuit_breaker_threshold: 3
  circuit_breaker_cooldown_seconds: 300

blizzard:
  enabled: true
  base_url: "https://us.api.blizzard.com"
  oauth_url: "https://oauth.battle.net/token"
  requests_per_hour_cap: 36000
  requests_per_second_cap: 100
  timeout_seconds: 30
  retry_attempts: 4
  backoff_seconds: 2.0
  locale: "en_US"
  namespace_profile: "profile-us"
  namespace_dynamic: "dynamic-us"
  run_fingerprint_fuzz_seconds: 2

redis:
  key_prefix: "niru"

mongodb:
  database: "niru"
  players_collection: "players"
  runs_collection: "runs"
  sync_cycles_collection: "sync_cycles"

logging:
  level: "INFO"
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"
            config_path.write_text(config_text, encoding="utf-8")
            previous_env = {
                "GOOGLE_SHEET_ID": os.environ.get("GOOGLE_SHEET_ID"),
                "MONGODB_URI": os.environ.get("MONGODB_URI"),
                "BLIZZARD_CLIENT_ID": os.environ.get("BLIZZARD_CLIENT_ID"),
                "BLIZZARD_CLIENT_SECRET": os.environ.get("BLIZZARD_CLIENT_SECRET"),
            }
            os.environ["GOOGLE_SHEET_ID"] = "sheet-id"
            os.environ["MONGODB_URI"] = "mongodb://localhost:27017"
            os.environ["BLIZZARD_CLIENT_ID"] = "client-id"
            os.environ["BLIZZARD_CLIENT_SECRET"] = "client-secret"
            try:
                settings = load_settings(str(config_path))
            finally:
                for key, value in previous_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

        self.assertEqual(settings.sync.active_interval_minutes, 5)
        self.assertEqual(settings.sync.active_idle_minutes, 40)
        self.assertTrue(settings.sync.predictive_hot_enabled)
        self.assertEqual(settings.sync.predictive_hot_threshold, 0.5)
        self.assertTrue(settings.blizzard.enabled)
        self.assertEqual(settings.blizzard.run_fingerprint_fuzz_seconds, 2)
        self.assertEqual(settings.blizzard.requests_per_hour_cap, 36000)
        self.assertEqual(settings.blizzard.requests_per_second_cap, 100)
        self.assertTrue(settings.team_activity.enabled)
        self.assertEqual(settings.team_activity.window_weeks, 2)
        self.assertEqual(settings.team_activity.start_hour, 7)
        self.assertEqual(settings.google.team_activity_output_start_cell, "C101")

    def test_load_settings_allows_missing_current_season_when_blizzard_enabled(self) -> None:
        config_text = """
google:
  raw_tab_name: "raw_data"
  roster_column: "A"
  roster_start_row: 2
  output_start_cell: "C1"

team_activity:
  enabled: true
  window_weeks: 2
  start_hour: 7
  output_start_cell: "C101"

sync:
  interval_minutes: 15
  active_interval_minutes: 5
  active_idle_minutes: 40
  predictive_hot_enabled: true
  predictive_hot_threshold: 0.5
  max_players_per_cycle: 250
  failure_backoff_seconds: 30
  max_failure_backoff_seconds: 900
  failure_backoff_jitter_seconds: 5

raiderio:
  base_url: "https://raider.io/api/v1"
  access_key_enabled: false
  requests_per_minute_cap: 60
  timeout_seconds: 30
  retry_attempts: 4
  backoff_seconds: 2.0
  circuit_breaker_threshold: 3
  circuit_breaker_cooldown_seconds: 300

blizzard:
  enabled: true
  base_url: "https://us.api.blizzard.com"
  oauth_url: "https://oauth.battle.net/token"
  requests_per_hour_cap: 36000
  requests_per_second_cap: 100
  timeout_seconds: 30
  retry_attempts: 4
  backoff_seconds: 2.0
  locale: "en_US"
  namespace_profile: "profile-us"
  namespace_dynamic: "dynamic-us"
  run_fingerprint_fuzz_seconds: 2

redis:
  key_prefix: "niru"

mongodb:
  database: "niru"
  players_collection: "players"
  runs_collection: "runs"
  sync_cycles_collection: "sync_cycles"

logging:
  level: "INFO"
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.yaml"
            config_path.write_text(config_text, encoding="utf-8")
            previous_env = {
                "GOOGLE_SHEET_ID": os.environ.get("GOOGLE_SHEET_ID"),
                "MONGODB_URI": os.environ.get("MONGODB_URI"),
                "BLIZZARD_CLIENT_ID": os.environ.get("BLIZZARD_CLIENT_ID"),
                "BLIZZARD_CLIENT_SECRET": os.environ.get("BLIZZARD_CLIENT_SECRET"),
            }
            os.environ["GOOGLE_SHEET_ID"] = "sheet-id"
            os.environ["MONGODB_URI"] = "mongodb://localhost:27017"
            os.environ["BLIZZARD_CLIENT_ID"] = "client-id"
            os.environ["BLIZZARD_CLIENT_SECRET"] = "client-secret"
            try:
                settings = load_settings(str(config_path))
            finally:
                for key, value in previous_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

        self.assertIsNone(settings.sync.current_season)


if __name__ == "__main__":
    unittest.main()
