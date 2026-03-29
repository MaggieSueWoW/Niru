from datetime import UTC, datetime
import sys
import unittest
from unittest.mock import patch

from niru.backfill import BackfillService, _load_all_active_players, _parse_players, parse_args
from niru.clients.raiderio_internal import RaiderIOInternalClient
from niru.config import RaiderIOSettings


class FakeControlState:
    def acquire_request_slot(self, *, requests_per_minute):
        return None

    def get_cooldown_remaining_seconds(self):
        return 0.0

    def get_cooldown_reason(self):
        return ""

    def clear_upstream_failure_streak(self):
        return None

    def open_cooldown(self, *, seconds, reason):
        return None

    def increment_upstream_failure_streak(self, *, ttl_seconds):
        return 1


class FakeRepo:
    def __init__(self) -> None:
        self.cached_character_ids = {}
        self.cached_resolved_at = {}
        self.runs = {}
        self.season_dungeons = []

    def list_season_dungeons(self, *, season):
        return [d for d in self.season_dungeons if d["season"] == season]

    def list_all_active_players(self):
        return [
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
            {
                "player_key": "us/proudmoore/nyph",
                "region": "us",
                "realm": "proudmoore",
                "name": "Nyph",
                "is_valid": True,
            },
        ]

    def replace_season_dungeons(self, *, season, dungeons, synced_at):
        self.season_dungeons = [
            {
                "season": dungeon.season,
                "dungeon_id": dungeon.dungeon_id,
                "slug": dungeon.slug,
                "name": dungeon.name,
                "short_name": dungeon.short_name,
            }
            for dungeon in dungeons
        ]

    def get_player_character_id(self, *, player_key):
        return self.cached_character_ids.get(player_key)

    def cache_player_character_id(self, *, player_key, identity, character_id, resolved_at):
        self.cached_character_ids[player_key] = character_id
        self.cached_resolved_at[player_key] = resolved_at

    def get_known_run_ids(self, run_ids):
        return {run_id for run_id in run_ids if run_id in self.runs}

    def attach_player_to_run(self, run_id, player_key):
        document = self.runs.setdefault(run_id, {})
        document.setdefault("discovered_from_player_keys", [])
        if player_key not in document["discovered_from_player_keys"]:
            document["discovered_from_player_keys"].append(player_key)

    def update_run_details(self, *, run_id, payload, player_key, synced_at):
        existing = self.runs.get(run_id, {})
        discovered_from = list(existing.get("discovered_from_player_keys", []))
        if player_key and player_key not in discovered_from:
            discovered_from.append(player_key)
        self.runs[run_id] = {
            "payload": payload,
            "player_key": player_key,
            "synced_at": synced_at,
            "discovered_from_player_keys": discovered_from,
        }


class FakePublicClient:
    def __init__(self) -> None:
        self.api_calls = 0
        self.detail_calls = []

    def get_mythic_plus_static_data(self, *, expansion_id):
        self.api_calls += 1
        return type(
            "Result",
            (),
            {
                "payload": {
                    "seasons": [
                        {
                            "slug": "season-mn-1",
                            "dungeons": [
                                {
                                    "id": 1001,
                                    "slug": "alpha",
                                    "name": "Alpha",
                                    "short_name": "ALP",
                                    "challenge_mode_id": 11,
                                    "keystone_timer_seconds": 1800,
                                    "icon_url": "",
                                    "background_image_url": "",
                                },
                                {
                                    "id": 1002,
                                    "slug": "beta",
                                    "name": "Beta",
                                    "short_name": "BET",
                                    "challenge_mode_id": 12,
                                    "keystone_timer_seconds": 1800,
                                    "icon_url": "",
                                    "background_image_url": "",
                                },
                            ],
                        }
                    ]
                }
            },
        )()

    def get_run_details(self, *, season, run_id):
        self.api_calls += 1
        self.detail_calls.append((season, run_id))
        return type(
            "Result",
            (),
            {
                "payload": {
                    "season": season,
                    "mythic_level": 12,
                    "score": 300.0,
                    "clear_time_ms": 123456,
                    "keystone_time_ms": 150000,
                    "num_chests": 1,
                    "completed_at": "2026-03-28T01:00:00.000Z",
                    "dungeon": {
                        "id": 1001,
                        "name": "Alpha",
                        "short_name": "ALP",
                        "map_challenge_mode_id": 11,
                        "expansion_id": 11,
                        "icon_url": "",
                    },
                    "roster": [
                        {
                            "character": {
                                "region": {"slug": "us"},
                                "realm": {"slug": "proudmoore"},
                                "name": "MaggieSue",
                            }
                        }
                    ],
                }
            },
        )()


class FakeInternalClient:
    def __init__(self) -> None:
        self.api_calls = 0
        self.character_page_calls = []
        self.dungeon_calls = []

    def get_character_page(self, player, *, season, tier=35):
        self.api_calls += 1
        self.character_page_calls.append((player.player_key, season, tier))
        return type(
            "Result",
            (),
            {"payload": {"characterDetails": {"character": {"id": 1063439}}}},
        )()

    def extract_character_id(self, payload):
        return int(payload["characterDetails"]["character"]["id"])

    def get_character_dungeon_runs(self, *, season, character_id, dungeon_id):
        self.api_calls += 1
        self.dungeon_calls.append((season, character_id, dungeon_id))
        if dungeon_id == 1001:
            runs = [
                {"summary": {"keystone_run_id": 2001}},
                {"summary": {"keystone_run_id": 2002}},
            ]
        else:
            runs = [
                {"summary": {"keystone_run_id": 2002}},
                {"summary": {"keystone_run_id": 2003}},
            ]
        return type("Result", (), {"payload": {"runs": runs}})()


class BackfillTests(unittest.TestCase):
    def test_internal_client_uses_website_base_url(self) -> None:
        settings = RaiderIOSettings(
            base_url="https://raider.io/api/v1",
            access_key_enabled=False,
            access_key=None,
            requests_per_minute_cap=60,
            timeout_seconds=30,
            retry_attempts=4,
            backoff_seconds=2.0,
            circuit_breaker_threshold=3,
            circuit_breaker_cooldown_seconds=300,
        )

        client = RaiderIOInternalClient(settings, control_state=FakeControlState())

        self.assertEqual(client._settings.base_url, "https://raider.io/api")

    def test_internal_client_extracts_character_id_from_character_details(self) -> None:
        payload = {"characterDetails": {"character": {"id": 1063439}}}

        self.assertEqual(RaiderIOInternalClient.extract_character_id(payload), 1063439)

    def test_internal_client_url_encodes_non_ascii_character_names(self) -> None:
        settings = RaiderIOSettings(
            base_url="https://raider.io/api/v1",
            access_key_enabled=False,
            access_key=None,
            requests_per_minute_cap=60,
            timeout_seconds=30,
            retry_attempts=4,
            backoff_seconds=2.0,
            circuit_breaker_threshold=3,
            circuit_breaker_cooldown_seconds=300,
        )
        client = RaiderIOInternalClient(settings, control_state=FakeControlState())
        captured = {}

        def fake_get_json(path, params):
            captured["path"] = path
            captured["params"] = params
            return type("Result", (), {"payload": {}})()

        client._get_json = fake_get_json  # type: ignore[method-assign]
        player = _parse_players(["us/illidan/Gëbus"])[0]

        client.get_character_page(player, season="season-mn-1")

        self.assertEqual(captured["path"], "/characters/us/illidan/G%C3%ABbus")
        self.assertEqual(captured["params"], {"season": "season-mn-1", "tier": 35})

    def test_parse_args_accepts_required_players(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["backfill", "--players", "us/proudmoore/MaggieSue", "us/proudmoore/Nyph"],
        ):
            args = parse_args()
        self.assertEqual(args.players, ["us/proudmoore/MaggieSue", "us/proudmoore/Nyph"])
        self.assertFalse(args.dry_run)
        self.assertIsNone(args.limit_runs)

    def test_parse_args_accepts_all_active_players_flag(self) -> None:
        with patch.object(sys, "argv", ["backfill", "--all-active-players"]):
            args = parse_args()
        self.assertTrue(args.all_active_players)
        self.assertIsNone(args.players)

    def test_parse_players_builds_identities(self) -> None:
        players = _parse_players(["us/proudmoore/MaggieSue"])
        self.assertEqual(players[0].player_key, "us/proudmoore/maggiesue")
        self.assertEqual(players[0].realm, "proudmoore")

    def test_load_all_active_players_filters_to_valid_roster_entries(self) -> None:
        players = _load_all_active_players(FakeRepo())

        self.assertEqual(
            [player.player_key for player in players],
            ["us/proudmoore/maggiesue", "us/proudmoore/nyph"],
        )

    def test_backfill_discovers_dedupes_and_fetches_only_missing_ids(self) -> None:
        repo = FakeRepo()
        repo.runs[2001] = {"existing": True}
        service = BackfillService(
            settings=object(),
            repository=repo,
            public_client=FakePublicClient(),
            internal_client=FakeInternalClient(),
        )
        players = _parse_players(["us/proudmoore/MaggieSue"])

        stats = service.run(players=players, season="season-mn-1")

        self.assertEqual(stats.players, 1)
        self.assertEqual(stats.dungeons, 2)
        self.assertEqual(stats.discovered_run_ids, 3)
        self.assertEqual(stats.known_run_ids, 1)
        self.assertEqual(stats.missing_run_ids, 2)
        self.assertEqual(stats.inserted_runs, 2)
        self.assertEqual(
            repo.runs[2001]["discovered_from_player_keys"],
            ["us/proudmoore/maggiesue"],
        )
        self.assertIn(2002, repo.runs)
        self.assertIn(2003, repo.runs)

    def test_backfill_reuses_cached_character_id_and_supports_dry_run(self) -> None:
        repo = FakeRepo()
        repo.cached_character_ids["us/proudmoore/maggiesue"] = 1063439
        public_client = FakePublicClient()
        internal_client = FakeInternalClient()
        service = BackfillService(
            settings=object(),
            repository=repo,
            public_client=public_client,
            internal_client=internal_client,
        )
        players = _parse_players(["us/proudmoore/MaggieSue"])

        stats = service.run(players=players, season="season-mn-1", dry_run=True)

        self.assertEqual(stats.discovered_run_ids, 3)
        self.assertEqual(stats.missing_run_ids, 3)
        self.assertEqual(stats.inserted_runs, 0)
        self.assertEqual(internal_client.character_page_calls, [])
        self.assertEqual(public_client.detail_calls, [])

    def test_backfill_rerun_is_idempotent_for_known_runs(self) -> None:
        repo = FakeRepo()
        public_client = FakePublicClient()
        internal_client = FakeInternalClient()
        service = BackfillService(
            settings=object(),
            repository=repo,
            public_client=public_client,
            internal_client=internal_client,
        )
        players = _parse_players(["us/proudmoore/MaggieSue"])

        first = service.run(players=players, season="season-mn-1")
        second = service.run(players=players, season="season-mn-1")

        self.assertEqual(first.inserted_runs, 3)
        self.assertEqual(second.known_run_ids, 3)
        self.assertEqual(second.missing_run_ids, 0)
        self.assertEqual(second.inserted_runs, 0)

    def test_backfill_processes_players_sequentially(self) -> None:
        repo = FakeRepo()
        public_client = FakePublicClient()
        internal_client = FakeInternalClient()
        service = BackfillService(
            settings=object(),
            repository=repo,
            public_client=public_client,
            internal_client=internal_client,
        )
        players = _parse_players(["us/proudmoore/MaggieSue", "us/proudmoore/Nyph"])

        stats = service.run(players=players, season="season-mn-1")

        self.assertEqual(stats.discovered_run_ids, 3)
        self.assertEqual(stats.inserted_runs, 3)
        self.assertEqual(
            public_client.detail_calls,
            [("season-mn-1", 2001), ("season-mn-1", 2002), ("season-mn-1", 2003)],
        )


if __name__ == "__main__":
    unittest.main()
