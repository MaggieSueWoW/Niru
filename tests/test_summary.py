from datetime import UTC, datetime
import unittest

from niru.service import (
    build_summary_header,
    build_summary_metadata_rows,
    build_summary_rows,
)


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
                "weekly_10_plus_run_count",
                "DFC_current_score",
                "DFC_best_key_level",
                "DFC_best_upgrade_level",
                "DFC_total_runs",
            ],
        )
        self.assertEqual(row[0:4], ["us", "area-52", "Mythics", 382.1])
        self.assertEqual(row[4], datetime(2026, 3, 26, 5, 0))
        self.assertIsNone(row[5])
        self.assertEqual(row[6:10], [382.1, 13, 1, 2])

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
            ["us", "area-52", "Mythics", None, None, None, None, None, None, 0],
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

    def test_counts_weekly_10_plus_runs_with_period_boundaries_and_deduping(self) -> None:
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
        runs = [
            {
                "keystone_run_id": 10,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "mythic_level": 10,
                "completed_at": datetime(2026, 3, 24, 15, 0, tzinfo=UTC),
                "discovered_from_player_keys": ["us/area-52/mythics"],
                "participants": [],
            },
            {
                "keystone_run_id": 10,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "mythic_level": 10,
                "completed_at": datetime(2026, 3, 24, 15, 0, tzinfo=UTC),
                "discovered_from_player_keys": [],
                "participants": [{"player_key": "us/area-52/mythics"}],
            },
            {
                "keystone_run_id": 11,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "mythic_level": 9,
                "completed_at": datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
                "discovered_from_player_keys": ["us/area-52/mythics"],
                "participants": [],
            },
            {
                "keystone_run_id": 12,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "mythic_level": 12,
                "completed_at": datetime(2026, 3, 24, 14, 59, tzinfo=UTC),
                "discovered_from_player_keys": ["us/area-52/mythics"],
                "participants": [],
            },
            {
                "keystone_run_id": 13,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "mythic_level": 13,
                "completed_at": datetime(2026, 3, 31, 15, 0, tzinfo=UTC),
                "discovered_from_player_keys": ["us/area-52/mythics"],
                "participants": [],
            },
            {
                "keystone_run_id": 14,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "mythic_level": 11,
                "completed_at": datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
                "discovered_from_player_keys": ["us/area-52/mythics"],
                "participants": [],
            },
        ]

        summary_rows = build_summary_rows(
            players,
            runs,
            [],
            weekly_periods={
                "us": {
                    "period": 1056,
                    "start": datetime(2026, 3, 24, 15, 0, tzinfo=UTC),
                    "end": datetime(2026, 3, 31, 15, 0, tzinfo=UTC),
                }
            },
        )

        self.assertEqual(summary_rows[0].to_sheet_row()[5], 2)

    def test_uses_player_region_for_weekly_count(self) -> None:
        players = [
            {
                "player_key": "us/area-52/usplayer",
                "region": "us",
                "realm": "area-52",
                "name": "USPlayer",
                "is_valid": True,
                "current_total_score": None,
                "current_dungeon_scores": {},
            },
            {
                "player_key": "eu/tarren-mill/euplayer",
                "region": "eu",
                "realm": "tarren-mill",
                "name": "EUPlayer",
                "is_valid": True,
                "current_total_score": None,
                "current_dungeon_scores": {},
            },
        ]
        runs = [
            {
                "keystone_run_id": 20,
                "mythic_level": 10,
                "completed_at": datetime(2026, 3, 25, 0, 0, tzinfo=UTC),
                "discovered_from_player_keys": ["us/area-52/usplayer"],
                "participants": [],
            },
            {
                "keystone_run_id": 21,
                "mythic_level": 10,
                "completed_at": datetime(2026, 3, 25, 0, 0, tzinfo=UTC),
                "discovered_from_player_keys": ["eu/tarren-mill/euplayer"],
                "participants": [],
            },
        ]

        summary_rows = build_summary_rows(
            players,
            runs,
            [],
            weekly_periods={
                "us": {
                    "period": 1056,
                    "start": datetime(2026, 3, 24, 15, 0, tzinfo=UTC),
                    "end": datetime(2026, 3, 31, 15, 0, tzinfo=UTC),
                },
                "eu": {
                    "period": 1056,
                    "start": datetime(2026, 3, 25, 4, 0, tzinfo=UTC),
                    "end": datetime(2026, 4, 1, 4, 0, tzinfo=UTC),
                },
            },
        )

        self.assertEqual(summary_rows[0].to_sheet_row()[5], 1)
        self.assertEqual(summary_rows[1].to_sheet_row()[5], 0)

    def test_builds_unique_run_metadata_for_group_runs(self) -> None:
        header = [
            "region",
            "realm",
            "name",
            "current_total_mythic_plus_rating",
            "last_successful_sync_time_pacific",
        ]
        runs = [
            {
                "keystone_run_id": 101,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "completed_at": datetime(2026, 3, 26, 12, 0, tzinfo=UTC),
                "created_at": datetime(2026, 3, 26, 14, 0, tzinfo=UTC),
            },
            {
                "keystone_run_id": 101,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "completed_at": datetime(2026, 3, 26, 12, 0, tzinfo=UTC),
                "created_at": datetime(2026, 3, 26, 14, 30, tzinfo=UTC),
            },
            {
                "keystone_run_id": 202,
                "dungeon": "Operation: Floodgate",
                "short_name": "FLOOD",
                "completed_at": datetime(2026, 3, 26, 9, 0, tzinfo=UTC),
                "created_at": datetime(2026, 3, 26, 15, 0, tzinfo=UTC),
            },
        ]

        self.assertEqual(
            build_summary_metadata_rows(
                header=header,
                runs=runs,
                now=datetime(2026, 3, 26, 16, 0, tzinfo=UTC),
            ),
            [
                ("unique_runs", 2),
                ("raiderio_lag_now_minutes", 360.0),
                ("raiderio_lag_today_avg_minutes", 255.0),
                ("raiderio_lag_today_max_minutes", 360.0),
                ("raiderio_lag_today_run_count", 2),
            ],
        )

    def test_builds_lag_metadata_with_pacific_day_boundaries(self) -> None:
        header = [
            "region",
            "realm",
            "name",
            "current_total_mythic_plus_rating",
            "last_successful_sync_time_pacific",
        ]
        runs = [
            {
                "keystone_run_id": 101,
                "completed_at": datetime(2026, 3, 27, 3, 30, tzinfo=UTC),
                "created_at": datetime(2026, 3, 27, 6, 30, tzinfo=UTC),
            },
            {
                "keystone_run_id": 202,
                "completed_at": datetime(2026, 3, 27, 6, 30, tzinfo=UTC),
                "created_at": datetime(2026, 3, 27, 8, 30, tzinfo=UTC),
            },
        ]

        self.assertEqual(
            build_summary_metadata_rows(
                header=header,
                runs=runs,
                now=datetime(2026, 3, 27, 12, 0, tzinfo=UTC),
            ),
            [
                ("unique_runs", 2),
                ("raiderio_lag_now_minutes", 120.0),
                ("raiderio_lag_today_avg_minutes", 120.0),
                ("raiderio_lag_today_max_minutes", 120.0),
                ("raiderio_lag_today_run_count", 1),
            ],
        )

    def test_clamps_negative_lag_and_skips_missing_timestamps(self) -> None:
        header = [
            "region",
            "realm",
            "name",
            "current_total_mythic_plus_rating",
            "last_successful_sync_time_pacific",
        ]
        runs = [
            {
                "keystone_run_id": 101,
                "completed_at": datetime(2026, 3, 26, 12, 0, tzinfo=UTC),
                "created_at": datetime(2026, 3, 26, 11, 0, tzinfo=UTC),
            },
            {
                "keystone_run_id": 202,
                "completed_at": datetime(2026, 3, 26, 13, 0, tzinfo=UTC),
            },
            {
                "keystone_run_id": 303,
                "created_at": datetime(2026, 3, 26, 14, 0, tzinfo=UTC),
            },
        ]

        self.assertEqual(
            build_summary_metadata_rows(
                header=header,
                runs=runs,
                now=datetime(2026, 3, 26, 16, 0, tzinfo=UTC),
            ),
            [
                ("unique_runs", 3),
                ("raiderio_lag_now_minutes", 0.0),
                ("raiderio_lag_today_avg_minutes", 0.0),
                ("raiderio_lag_today_max_minutes", 0.0),
                ("raiderio_lag_today_run_count", 1),
            ],
        )

    def test_leaves_lag_values_blank_when_no_runs_have_complete_timestamps(self) -> None:
        header = [
            "region",
            "realm",
            "name",
            "current_total_mythic_plus_rating",
            "last_successful_sync_time_pacific",
        ]
        runs = [
            {"keystone_run_id": 101, "created_at": datetime(2026, 3, 26, 14, 0, tzinfo=UTC)},
            {"keystone_run_id": 202, "completed_at": datetime(2026, 3, 26, 12, 0, tzinfo=UTC)},
        ]

        self.assertEqual(
            build_summary_metadata_rows(
                header=header,
                runs=runs,
                now=datetime(2026, 3, 26, 16, 0, tzinfo=UTC),
            ),
            [
                ("unique_runs", 2),
                ("raiderio_lag_now_minutes", None),
                ("raiderio_lag_today_avg_minutes", None),
                ("raiderio_lag_today_max_minutes", None),
                ("raiderio_lag_today_run_count", 0),
            ],
        )
