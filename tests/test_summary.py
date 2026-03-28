from datetime import UTC, datetime
import unittest

from niru.service import build_summary_header, build_summary_rows


class SummaryBuilderTests(unittest.TestCase):
    def test_builds_player_dungeon_summary(self) -> None:
        players = [
            {
                "player_key": "us/area-52/mythics",
                "region": "us",
                "realm": "area-52",
                "name": "Mythics",
                "is_valid": True,
                "current_total_score": 382.1,
                "current_dungeon_scores": {"Darkflame Cleft": 382.1},
                "last_successful_sync_at": datetime(2026, 3, 26, 12, 0, tzinfo=UTC),
            }
        ]
        runs = [
            {
                "keystone_run_id": 10,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "score": 190.0,
                "mythic_level": 12,
                "num_keystone_upgrades": 2,
                "completed_at": datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
                "discovered_from_player_keys": ["us/area-52/mythics"],
                "participants": [],
            },
            {
                "keystone_run_id": 11,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "score": 192.1,
                "mythic_level": 13,
                "num_keystone_upgrades": 1,
                "completed_at": datetime(2026, 3, 26, 12, 0, tzinfo=UTC),
                "discovered_from_player_keys": ["us/area-52/mythics"],
                "participants": [],
            },
        ]
        season_dungeons = [
            {
                "season": "season-mn-1",
                "slug": "darkflame-cleft",
                "name": "Darkflame Cleft",
                "short_name": "DFC",
            }
        ]

        summary_rows = build_summary_rows(players, runs, season_dungeons)

        self.assertEqual(len(summary_rows), 1)
        row = summary_rows[0].to_sheet_row()
        self.assertEqual(
            build_summary_header(season_dungeons),
            [
                "region",
                "realm",
                "name",
                "current_total_mythic_plus_rating",
                "last_successful_sync_time_pacific",
                "DFC_current_score",
                "DFC_best_key_level",
                "DFC_best_upgrade_level",
                "DFC_total_runs",
            ],
        )
        self.assertEqual(row[0:4], ["us", "area-52", "Mythics", 382.1])
        self.assertEqual(row[4], datetime(2026, 3, 26, 5, 0))
        self.assertEqual(row[5:9], [382.1, 13, 1, 2])

    def test_keeps_player_visible_without_runs(self) -> None:
        players = [
            {
                "player_key": "us/area-52/mythics",
                "region": "us",
                "realm": "area-52",
                "name": "Mythics",
                "is_valid": True,
                "current_total_score": None,
                "current_dungeon_scores": {},
            }
        ]
        season_dungeons = [
            {
                "season": "season-mn-1",
                "slug": "darkflame-cleft",
                "name": "Darkflame Cleft",
                "short_name": "DFC",
            }
        ]
        summary_rows = build_summary_rows(players, [], season_dungeons)
        self.assertEqual(len(summary_rows), 1)
        self.assertEqual(
            summary_rows[0].to_sheet_row(),
            ["us", "area-52", "Mythics", None, None, None, None, None, 0],
        )

    def test_keeps_summary_rows_in_sheet_roster_order(self) -> None:
        players = [
            {
                "player_key": "us/area-52/second",
                "sheet_row_number": 3,
                "region": "us",
                "realm": "area-52",
                "name": "Second",
                "is_valid": True,
                "current_total_score": 999.9,
                "current_dungeon_scores": {},
            },
            {
                "player_key": "us/area-52/first",
                "sheet_row_number": 2,
                "region": "us",
                "realm": "area-52",
                "name": "First",
                "is_valid": True,
                "current_total_score": 1.0,
                "current_dungeon_scores": {},
            },
        ]

        summary_rows = build_summary_rows(players, [], [])

        self.assertEqual(
            [row.to_sheet_row()[2] for row in summary_rows],
            ["First", "Second"],
        )

    def test_adds_tiebreak_bonus_for_nyph_when_tied_with_gr_name(self) -> None:
        players = [
            {
                "player_key": "us/proudmoore/nyph",
                "sheet_row_number": 2,
                "region": "us",
                "realm": "proudmoore",
                "name": "Nyph",
                "is_valid": True,
                "current_total_score": 3210.4,
                "current_dungeon_scores": {},
            },
            {
                "player_key": "us/proudmoore/gryph",
                "sheet_row_number": 3,
                "region": "us",
                "realm": "proudmoore",
                "name": "Gryph",
                "is_valid": True,
                "current_total_score": 3210.4,
                "current_dungeon_scores": {},
            },
        ]

        summary_rows = build_summary_rows(players, [], [])

        self.assertEqual(summary_rows[0].to_sheet_row()[3], 3210.5)
        self.assertEqual(summary_rows[1].to_sheet_row()[3], 3210.4)
