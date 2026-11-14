import json
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

from niru.healthchecks import report_sync_success
from niru.models import SyncStats, utc_now

TEST_PING_URL = "https://hc-ping.com/00000000-0000-0000-0000-000000000000"


class HealthchecksTests(unittest.TestCase):
    def setUp(self) -> None:
        now = utc_now()
        self.cycle = SyncStats(season="season-mn-1").to_document(
            started_at=now, finished_at=now
        )

    def test_disabled_monitoring_does_not_make_a_request(self) -> None:
        with patch("niru.healthchecks.urlopen") as send:
            report_sync_success(None, self.cycle)
        send.assert_not_called()

    def test_success_posts_only_safe_diagnostics_with_a_short_timeout(self) -> None:
        self.cycle["partial"] = True
        self.cycle["warnings"] = ["private player details"]
        response = MagicMock(status=200)
        response.read.return_value = b"OK"
        with patch("niru.healthchecks.urlopen") as send:
            send.return_value.__enter__.return_value = response
            with self.assertLogs("niru.healthchecks", level="INFO") as logs:
                report_sync_success(TEST_PING_URL, self.cycle)

        request = send.call_args.args[0]
        self.assertEqual(request.full_url, TEST_PING_URL)
        self.assertEqual(request.method, "POST")
        self.assertEqual(send.call_args.kwargs["timeout"], 5)
        payload = json.loads(request.data)
        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["season"], "season-mn-1")
        self.assertNotIn("warnings", payload)
        self.assertNotIn("private player details", request.data.decode())
        self.assertNotIn(TEST_PING_URL, str(logs.output))

    def test_monitoring_errors_are_contained_and_do_not_expose_the_url(self) -> None:
        errors = [
            TimeoutError(TEST_PING_URL),
            URLError(TEST_PING_URL),
            HTTPError(TEST_PING_URL, 503, TEST_PING_URL, {}, None),
            ValueError(TEST_PING_URL),
        ]
        for error in errors:
            with self.subTest(error=type(error).__name__):
                with patch("niru.healthchecks.urlopen", side_effect=error) as send:
                    with self.assertLogs("niru.healthchecks", level="WARNING") as logs:
                        report_sync_success(TEST_PING_URL, self.cycle)
                send.assert_called_once()
                self.assertNotIn(TEST_PING_URL, str(logs.output))
                self.assertIsNone(logs.records[0].exc_info)

    def test_unacknowledged_pings_are_not_logged_as_success(self) -> None:
        for status, body in [
            (200, b"OK (not found)"),
            (200, b"OK (rate limited)"),
            (503, b"OK"),
        ]:
            with self.subTest(status=status, body=body):
                response = MagicMock(status=status)
                response.read.return_value = body
                with patch("niru.healthchecks.urlopen") as send:
                    send.return_value.__enter__.return_value = response
                    with self.assertLogs("niru.healthchecks", level="WARNING") as logs:
                        report_sync_success(TEST_PING_URL, self.cycle)
                self.assertIn("did not acknowledge", logs.output[0])
