from datetime import UTC, datetime
import unittest

from mplusbot.clients.raiderio import RaiderIONotFoundError
from mplusbot.models import PlayerDataStatus
from mplusbot.service import SyncService, build_summary_header


class FakeRepo:
    def __init__(self) -> None:
        self.players = []
        self.runs = []
        self.sync_docs = []
        self.season_dungeons = []

    def sync_roster(self, entries, *, seen_at):
        self.players = [
            {
                "player_key": entry.player_key,
                "sheet_row_number": entry.row_number,
                "sheet_value": entry.raw_value,
                "is_active": True,
                "is_valid": entry.is_valid,
                "status": entry.status.value,
                "status_message": entry.status_message,
                "region": entry.identity.region if entry.identity else "",
                "realm": entry.identity.realm if entry.identity else "",
                "name": entry.identity.name if entry.identity else entry.raw_value,
                "current_dungeon_scores": {},
                "current_total_score": None,
                "gap_flag": False,
            }
            for entry in entries
        ]

    def list_active_players(self, *, limit):
        return self.players[:limit]

    def mark_sync_started(self, player_key, started_at):
        return None

    def mark_gap_flag(self, player_key, message):
        for player in self.players:
            if player["player_key"] == player_key:
                player["gap_flag"] = True
                player["gap_message"] = message

    def update_player_profile(
        self, player_key, *, current_dungeon_scores, current_total_score, synced_at
    ):
        for player in self.players:
            if player["player_key"] == player_key:
                player["status"] = PlayerDataStatus.OK.value
                player["status_message"] = ""
                player["current_dungeon_scores"] = current_dungeon_scores
                player["current_total_score"] = current_total_score
                player["last_successful_sync_at"] = synced_at

    def get_known_run_ids(self, run_ids):
        return {run["keystone_run_id"] for run in self.runs if run["keystone_run_id"] in run_ids}

    def attach_player_to_run(self, run_id, player_key):
        for run in self.runs:
            if run["keystone_run_id"] == run_id:
                run.setdefault("discovered_from_player_keys", []).append(player_key)

    def upsert_run_stub(self, run, *, player_key, season, synced_at):
        self.runs.append(
            {
                "keystone_run_id": run["keystone_run_id"],
                "dungeon": run["dungeon"],
                "short_name": run.get("short_name", ""),
                "score": run["score"],
                "mythic_level": run["mythic_level"],
                "num_keystone_upgrades": run["num_keystone_upgrades"],
                "completed_at": datetime(2026, 3, 25, 12, 0, tzinfo=UTC),
                "discovered_from_player_keys": [player_key],
                "participants": [],
            }
        )

    def update_run_details(self, *, run_id, payload, player_key, synced_at):
        return None

    def mark_invalid_player(self, player_key, message, *, when):
        for player in self.players:
            if player["player_key"] == player_key:
                player["status"] = PlayerDataStatus.INVALID_PLAYER.value
                player["status_message"] = message

    def mark_sync_error(self, player_key, message, *, when):
        for player in self.players:
            if player["player_key"] == player_key:
                player["status"] = PlayerDataStatus.SYNC_ERROR.value
                player["status_message"] = message

    def get_runs_for_players(self, player_keys):
        return self.runs

    def list_season_dungeons(self, *, season):
        return [d for d in self.season_dungeons if d["season"] == season]

    def replace_season_dungeons(self, *, season, dungeons, synced_at):
        self.season_dungeons = [
            {
                "season": dungeon.season,
                "slug": dungeon.slug,
                "name": dungeon.name,
                "short_name": dungeon.short_name,
            }
            for dungeon in dungeons
        ]

    def store_sync_cycle(self, document):
        self.sync_docs.append(document)


class FakeSheets:
    def __init__(self, rows):
        self.rows = rows
        self.last_header = None
        self.last_rows = None

    def read_roster_rows(self):
        return self.rows

    def write_output_rows(self, header, rows):
        self.last_header = header
        self.last_rows = rows
        return len(rows)


class FakeRaiderIO:
    def __init__(self):
        self.api_calls = 0

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
                                    "slug": "darkflame-cleft",
                                    "name": "Darkflame Cleft",
                                    "short_name": "DFC",
                                }
                            ],
                        }
                    ]
                }
            },
        )()

    def get_character_profile(self, player):
        self.api_calls += 1
        if player.name == "Missing":
            raise RaiderIONotFoundError("missing")
        return type(
            "Result",
            (),
            {
                "payload": {
                    "mythic_plus_best_runs": [
                        {
                            "keystone_run_id": 123,
                            "dungeon": "Darkflame Cleft",
                            "short_name": "DFC",
                            "score": 190.2,
                            "mythic_level": 12,
                            "num_keystone_upgrades": 2,
                        }
                    ],
                    "mythic_plus_alternate_runs": [
                        {
                            "keystone_run_id": 124,
                            "dungeon": "Darkflame Cleft",
                            "short_name": "DFC",
                            "score": 180.0,
                            "mythic_level": 11,
                            "num_keystone_upgrades": 1,
                        }
                    ],
                    "mythic_plus_recent_runs": [],
                    "mythic_plus_scores_by_season": [
                        {"season": "season-mn-1", "scores": {"all": 370.2}}
                    ],
                }
            },
        )()

class SyncServiceTests(unittest.TestCase):
    def test_run_cycle_writes_summary_rows(self) -> None:
        settings = type(
            "Settings",
            (),
            {
                "google": type("Google", (), {"roster_start_row": 2})(),
                "sync": type(
                    "Sync",
                    (),
                    {
                        "max_players_per_cycle": 100,
                        "interval_minutes": 15,
                        "gap_detection_cycles": 2,
                        "current_season": "season-mn-1",
                    },
                )(),
            },
        )()
        repo = FakeRepo()
        sheets = FakeSheets(["us/area-52/Mythics"])
        raider = FakeRaiderIO()
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=sheets,
            raiderio_client=raider,
        )

        service.run_cycle()

        self.assertEqual(
            sheets.last_header,
            build_summary_header(repo.season_dungeons),
        )
        self.assertEqual(len(sheets.last_rows), 1)
        self.assertEqual(sheets.last_rows[0][3], 370.2)
        self.assertEqual(sheets.last_rows[0][5], 370.2)
        self.assertEqual(sheets.last_rows[0][6], 12)
        self.assertEqual(sheets.last_rows[0][8], 2)
        self.assertEqual(repo.sync_docs[0]["api_calls"], 2)

    def test_missing_player_still_publishes_row(self) -> None:
        settings = type(
            "Settings",
            (),
            {
                "google": type("Google", (), {"roster_start_row": 2})(),
                "sync": type(
                    "Sync",
                    (),
                    {
                        "max_players_per_cycle": 100,
                        "interval_minutes": 15,
                        "gap_detection_cycles": 2,
                        "current_season": "season-mn-1",
                    },
                )(),
            },
        )()
        repo = FakeRepo()
        sheets = FakeSheets(["us/area-52/Missing"])
        raider = FakeRaiderIO()
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=sheets,
            raiderio_client=raider,
        )

        service.run_cycle()

        self.assertEqual(len(sheets.last_rows), 1)
        self.assertEqual(sheets.last_rows[0][0:4], ["us", "area-52", "Missing", None])

    def test_stop_requested_breaks_sleep_wait(self) -> None:
        settings = type(
            "Settings",
            (),
            {
                "google": type("Google", (), {"roster_start_row": 2})(),
                "sync": type(
                    "Sync",
                    (),
                    {
                        "max_players_per_cycle": 100,
                        "interval_minutes": 15,
                        "gap_detection_cycles": 2,
                        "current_season": "season-mn-1",
                    },
                )(),
            },
        )()
        repo = FakeRepo()
        sheets = FakeSheets([])
        raider = FakeRaiderIO()
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=sheets,
            raiderio_client=raider,
        )

        service._stop_requested = True
        service._stop_event.set()
        service.run_forever()

    def test_gap_detection_handles_naive_mongo_datetime(self) -> None:
        settings = type(
            "Settings",
            (),
            {
                "google": type("Google", (), {"roster_start_row": 2})(),
                "sync": type(
                    "Sync",
                    (),
                    {
                        "max_players_per_cycle": 100,
                        "interval_minutes": 15,
                        "gap_detection_cycles": 2,
                        "current_season": "season-mn-1",
                    },
                )(),
            },
        )()
        repo = FakeRepo()
        repo.players = [
            {
                "player_key": "us/area-52/mythics",
                "region": "us",
                "realm": "area-52",
                "name": "Mythics",
                "is_valid": True,
                "status": PlayerDataStatus.OK.value,
                "status_message": "",
                "gap_flag": False,
                "current_dungeon_scores": {},
                "last_successful_sync_at": datetime(2026, 3, 25, 12, 0),
            }
        ]
        raider = FakeRaiderIO()
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=raider,
        )

        service._sync_player(player=repo.players[0], stats=type("Stats", (), {"partial": False, "warnings": [], "new_runs": 0, "detail_fetches": 0})(), now=datetime(2026, 3, 26, 12, 0, tzinfo=UTC))

        self.assertTrue(repo.players[0]["gap_flag"])
