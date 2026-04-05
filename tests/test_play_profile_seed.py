from datetime import UTC, datetime
import sys
import unittest
from unittest.mock import patch

from niru.play_profile_seed import (
    PlayProfileSeedService,
    _load_all_active_players,
    _parse_players,
    parse_args,
)


class FakeRepo:
    def __init__(self) -> None:
        self.players = [
            {
                "player_key": "us/proudmoore/maggiesue",
                "region": "us",
                "realm": "proudmoore",
                "name": "MaggieSue",
                "is_valid": True,
            },
            {
                "player_key": "us/proudmoore/badrow",
                "region": "us",
                "realm": "proudmoore",
                "name": "",
                "is_valid": False,
            },
        ]
        self.runs = [
            {
                "season": "season-mn-1",
                "completed_at": datetime(2026, 3, 24, 1, 0, tzinfo=UTC),
                "discovered_from_player_keys": ["us/proudmoore/maggiesue"],
                "participants": [],
            },
            {
                "season": "season-mn-1",
                "completed_at": datetime(2026, 3, 31, 1, 0, tzinfo=UTC),
                "discovered_from_player_keys": ["us/proudmoore/maggiesue"],
                "participants": [],
            },
        ]
        self.updated_profiles = {}

    def list_all_active_players(self):
        return self.players

    def get_runs_for_player(self, *, player_key, season):
        return [
            run
            for run in self.runs
            if run["season"] == season and player_key in run["discovered_from_player_keys"]
        ]

    def upsert_player_play_profile(self, *, player_key, profile):
        self.updated_profiles[player_key] = profile


class PlayProfileSeedTests(unittest.TestCase):
    def test_parse_args_accepts_player_filter(self) -> None:
        with patch.object(sys, "argv", ["play-profile-seed", "--player", "us/proudmoore/MaggieSue"]):
            args = parse_args()

        self.assertEqual(args.players, ["us/proudmoore/MaggieSue"])
        self.assertFalse(args.dry_run)

    def test_parse_players_builds_identities(self) -> None:
        players = _parse_players(["us/proudmoore/MaggieSue"])

        self.assertEqual(players[0].player_key, "us/proudmoore/maggiesue")

    def test_load_all_active_players_filters_invalid_rows(self) -> None:
        players = _load_all_active_players(FakeRepo())

        self.assertEqual([player.player_key for player in players], ["us/proudmoore/maggiesue"])

    def test_seed_service_builds_profiles_from_current_season_runs(self) -> None:
        settings = type(
            "Settings",
            (),
            {"sync": type("Sync", (), {"current_season": "season-mn-1"})()},
        )()
        repo = FakeRepo()
        service = PlayProfileSeedService(settings=settings, repository=repo)
        players = _parse_players(["us/proudmoore/MaggieSue"])

        stats = service.run(players=players)

        self.assertEqual(stats.seeded_players, 1)
        profile = repo.updated_profiles["us/proudmoore/maggiesue"]
        self.assertEqual(profile["play_profile_weeks_observed"], 2)
        self.assertEqual(sum(profile["play_profile_hour_counts"]), 2)

    def test_seed_service_supports_dry_run(self) -> None:
        settings = type(
            "Settings",
            (),
            {"sync": type("Sync", (), {"current_season": "season-mn-1"})()},
        )()
        repo = FakeRepo()
        service = PlayProfileSeedService(settings=settings, repository=repo)
        players = _parse_players(["us/proudmoore/MaggieSue"])

        stats = service.run(players=players, dry_run=True)

        self.assertTrue(stats.dry_run)
        self.assertEqual(repo.updated_profiles, {})


if __name__ == "__main__":
    unittest.main()
