import unittest

from mplusbot.models import PlayerDataStatus
from mplusbot.roster import parse_roster_rows


class RosterParsingTests(unittest.TestCase):
    def test_parses_valid_roster_entry(self) -> None:
        rows = parse_roster_rows(["us/area-52/Mythics"], start_row=2)
        entry = rows[0]
        self.assertTrue(entry.is_valid)
        self.assertEqual(entry.player_key, "us/area-52/mythics")
        self.assertEqual(entry.identity.realm, "area-52")
        self.assertEqual(entry.identity.name, "Mythics")

    def test_marks_bad_format_invalid(self) -> None:
        rows = parse_roster_rows(["just-a-name"], start_row=2)
        entry = rows[0]
        self.assertFalse(entry.is_valid)
        self.assertEqual(entry.status, PlayerDataStatus.INVALID_PLAYER)
        self.assertIn("format", entry.status_message)

    def test_marks_duplicate_after_first_invalid(self) -> None:
        rows = parse_roster_rows(
            ["us/area-52/Mythics", "us/area-52/Mythics"],
            start_row=2,
        )
        self.assertTrue(rows[0].is_valid)
        self.assertFalse(rows[1].is_valid)
        self.assertEqual(rows[1].status, PlayerDataStatus.INVALID_PLAYER)
        self.assertIn("Duplicate", rows[1].status_message)
