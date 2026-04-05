from datetime import UTC, datetime, timedelta
import unittest

import niru.service as service_module
from niru.clients.raiderio import RaiderIONotFoundError
from niru.models import PlayerDataStatus
from niru.service import SyncService, build_summary_header


def make_settings():
    return type(
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
                    "active_interval_minutes": 5,
                    "active_start_delay_minutes": 20,
                    "active_idle_minutes": 40,
                    "gap_detection_cycles": 2,
                    "current_season": "season-mn-1",
                    "failure_backoff_seconds": 30.0,
                    "max_failure_backoff_seconds": 300.0,
                    "failure_backoff_jitter_seconds": 0.0,
                },
            )(),
        },
    )()


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
                "last_new_run_completed_at": None,
                "hot_ready_at": None,
                "hot_until_at": None,
            }
            for entry in entries
        ]

    def list_active_players(self, *, limit):
        return self.players[:limit]

    def list_players_due_for_base_sync(self, *, now, interval_minutes, limit):
        due = []
        for player in self.players[:limit]:
            if not player.get("is_valid", False):
                continue
            last_started = player.get("last_sync_started_at")
            if last_started is None or (now - last_started).total_seconds() >= interval_minutes * 60:
                due.append(player)
        return due[:limit]

    def list_players_due_for_hot_sync(self, *, now, interval_minutes, limit):
        due = []
        for player in self.players[:limit]:
            if not player.get("is_valid", False):
                continue
            hot_ready_at = player.get("hot_ready_at")
            hot_until_at = player.get("hot_until_at")
            if hot_ready_at is None or hot_until_at is None:
                continue
            if not hot_ready_at <= now < hot_until_at:
                continue
            last_started = player.get("last_sync_started_at")
            if last_started is None or (now - last_started).total_seconds() >= interval_minutes * 60:
                due.append(player)
        return due[:limit]

    def mark_sync_started(self, player_key, started_at):
        for player in self.players:
            if player["player_key"] == player_key:
                player["last_sync_started_at"] = started_at

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
                player["last_sync_completed_at"] = synced_at

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
                player["last_sync_completed_at"] = when

    def schedule_player_hot_window(
        self,
        *,
        player_key,
        last_new_run_completed_at,
        hot_ready_at,
        hot_until_at,
    ):
        for player in self.players:
            if player["player_key"] == player_key:
                player["last_new_run_completed_at"] = last_new_run_completed_at
                player["hot_ready_at"] = hot_ready_at
                player["hot_until_at"] = hot_until_at

    def clear_player_hot_window(self, *, player_key):
        for player in self.players:
            if player["player_key"] == player_key:
                player["hot_ready_at"] = None
                player["hot_until_at"] = None

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
        self.last_metadata_rows = None

    def read_roster_rows(self):
        return self.rows

    def write_output_rows(self, header, rows, metadata_rows=None):
        self.last_header = header
        self.last_rows = rows
        self.last_metadata_rows = metadata_rows
        return len(rows)


class FakeRaiderIO:
    def __init__(self):
        self.api_calls = 0
        self.cooldown_remaining = 0.0
        self.cooldown_reason = ""
        self.include_us_period = True
        self.profile_payload = {
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

    def get_periods(self):
        self.api_calls += 1
        periods = [
            {
                "region": "eu",
                "current": {
                    "period": 1056,
                    "start": "2026-03-25T04:00:00.000Z",
                    "end": "2026-04-01T04:00:00.000Z",
                },
            }
        ]
        if self.include_us_period:
            periods.insert(
                0,
                {
                    "region": "us",
                    "current": {
                        "period": 1056,
                        "start": "2026-03-24T15:00:00.000Z",
                        "end": "2026-03-31T15:00:00.000Z",
                    },
                },
            )
        return type("Result", (), {"payload": {"periods": periods}})()

    def get_character_profile(self, player):
        self.api_calls += 1
        if player.name == "Missing":
            raise RaiderIONotFoundError("missing")
        return type("Result", (), {"payload": self.profile_payload})()

    def get_cooldown_remaining_seconds(self):
        return self.cooldown_remaining

    def get_cooldown_reason(self):
        return self.cooldown_reason


class SyncServiceTests(unittest.TestCase):
    def test_run_cycle_writes_summary_rows(self) -> None:
        settings = make_settings()
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
        self.assertEqual(sheets.last_rows[0][5], 2)
        self.assertEqual(sheets.last_rows[0][6], 370.2)
        self.assertEqual(sheets.last_rows[0][7], 12)
        self.assertEqual(sheets.last_rows[0][9], 2)
        self.assertEqual(sheets.last_metadata_rows, [("unique_runs", 2)])
        self.assertEqual(repo.sync_docs[0]["api_calls"], 3)
        self.assertEqual(
            repo.sync_docs[0]["weekly_periods"]["us"],
            {
                "period": 1056,
                "start": "2026-03-24T15:00:00+00:00",
                "end": "2026-03-31T15:00:00+00:00",
            },
        )

    def test_missing_player_still_publishes_row(self) -> None:
        settings = make_settings()
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

    def test_missing_weekly_period_leaves_value_blank_and_warns(self) -> None:
        settings = make_settings()
        repo = FakeRepo()
        sheets = FakeSheets(["us/area-52/Mythics"])
        raider = FakeRaiderIO()
        raider.include_us_period = False
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=sheets,
            raiderio_client=raider,
        )

        service.run_cycle()

        self.assertIsNone(sheets.last_rows[0][5])
        self.assertTrue(repo.sync_docs[0]["partial"])
        self.assertIn(
            "Missing Raider.IO weekly period for region us; weekly 10+ counts left blank.",
            repo.sync_docs[0]["warnings"],
        )

    def test_stop_requested_breaks_sleep_wait(self) -> None:
        settings = make_settings()
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
        settings = make_settings()
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

    def test_run_forever_retries_failed_cycle_without_crashing(self) -> None:
        service = SyncService(
            settings=make_settings(),
            repository=FakeRepo(),
            sheets_client=FakeSheets([]),
            raiderio_client=FakeRaiderIO(),
        )
        attempts = {"count": 0}
        waits = []

        def fake_run_cycle():
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise RuntimeError("boom")
            service._stop_requested = True

        def fake_wait(timeout_seconds):
            waits.append(round(timeout_seconds, 1))
            return False

        service.install_signal_handlers = lambda: None  # type: ignore[method-assign]
        service.run_cycle = fake_run_cycle  # type: ignore[method-assign]
        service._wait_for_stop = fake_wait  # type: ignore[method-assign]

        service.run_forever()

        self.assertEqual(attempts["count"], 2)
        self.assertEqual(waits, [30.0])

    def test_run_cycle_skips_player_sync_when_cooldown_is_active(self) -> None:
        repo = FakeRepo()
        repo.season_dungeons = [
            {
                "season": "season-mn-1",
                "slug": "darkflame-cleft",
                "name": "Darkflame Cleft",
                "short_name": "DFC",
            }
        ]
        raider = FakeRaiderIO()
        raider.cooldown_remaining = 120.0
        raider.cooldown_reason = "Raider.IO rate limit hit"
        service = SyncService(
            settings=make_settings(),
            repository=repo,
            sheets_client=FakeSheets(["us/area-52/Mythics"]),
            raiderio_client=raider,
        )

        service.run_cycle()

        self.assertEqual(raider.api_calls, 0)
        self.assertTrue(repo.sync_docs[0]["partial"])
        self.assertIn("Raider.IO rate limit hit", repo.sync_docs[0]["warnings"][0])

    def test_new_run_schedules_delayed_hot_window(self) -> None:
        now = datetime(2026, 3, 26, 12, 30, tzinfo=UTC)
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
                "last_successful_sync_at": now - timedelta(minutes=15),
            }
        ]
        raider = FakeRaiderIO()
        raider.profile_payload["mythic_plus_recent_runs"] = [
            {
                "keystone_run_id": 777,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "score": 200.0,
                "mythic_level": 13,
                "num_keystone_upgrades": 1,
                "completed_at": "2026-03-26T12:00:00+00:00",
            }
        ]
        service = SyncService(
            settings=make_settings(),
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=raider,
        )

        stats = type(
            "Stats",
            (),
            {
                "partial": False,
                "warnings": [],
                "new_runs": 0,
                "detail_fetches": 0,
                "players_scheduled_for_hot": 0,
            },
        )()
        service._sync_player(player=repo.players[0], stats=stats, now=now)

        self.assertEqual(repo.players[0]["last_new_run_completed_at"], datetime(2026, 3, 26, 12, 0, tzinfo=UTC))
        self.assertEqual(repo.players[0]["hot_ready_at"], datetime(2026, 3, 26, 12, 20, tzinfo=UTC))
        self.assertEqual(repo.players[0]["hot_until_at"], datetime(2026, 3, 26, 13, 0, tzinfo=UTC))
        self.assertEqual(stats.players_scheduled_for_hot, 1)

    def test_hot_player_not_selected_before_ready_time(self) -> None:
        now = datetime(2026, 3, 26, 12, 15, tzinfo=UTC)
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
                "last_sync_started_at": now - timedelta(minutes=4),
                "hot_ready_at": now + timedelta(minutes=5),
                "hot_until_at": now + timedelta(minutes=45),
            }
        ]
        service = SyncService(
            settings=make_settings(),
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=FakeRaiderIO(),
        )

        players, _, hot_keys = service._select_players_for_sync(now=now)

        self.assertEqual(players, [])
        self.assertEqual(hot_keys, set())

    def test_hot_player_selected_once_ready_time_is_reached(self) -> None:
        now = datetime(2026, 3, 26, 12, 25, tzinfo=UTC)
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
                "last_sync_started_at": now - timedelta(minutes=5),
                "hot_ready_at": now,
                "hot_until_at": now + timedelta(minutes=40),
            }
        ]
        service = SyncService(
            settings=make_settings(),
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=FakeRaiderIO(),
        )

        players, _, hot_keys = service._select_players_for_sync(now=now)

        self.assertEqual([player["player_key"] for player in players], ["us/area-52/mythics"])
        self.assertEqual(hot_keys, {"us/area-52/mythics"})

    def test_expired_hot_window_is_cleared(self) -> None:
        now = datetime(2026, 3, 26, 13, 5, tzinfo=UTC)
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
                "hot_ready_at": datetime(2026, 3, 26, 12, 20, tzinfo=UTC),
                "hot_until_at": datetime(2026, 3, 26, 13, 0, tzinfo=UTC),
            }
        ]
        service = SyncService(
            settings=make_settings(),
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=FakeRaiderIO(),
        )

        service._expire_hot_windows(active_players=repo.players, now=now)

        self.assertIsNone(repo.players[0]["hot_ready_at"])
        self.assertIsNone(repo.players[0]["hot_until_at"])

    def test_next_cycle_delay_uses_future_hot_ready_time(self) -> None:
        now = datetime(2026, 3, 26, 12, 0, tzinfo=UTC)
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
                "last_sync_started_at": now - timedelta(minutes=1),
                "hot_ready_at": now + timedelta(minutes=4),
                "hot_until_at": now + timedelta(minutes=44),
            }
        ]
        service = SyncService(
            settings=make_settings(),
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=FakeRaiderIO(),
        )
        original_utc_now = service_module.utc_now
        service_module.utc_now = lambda: now
        try:
            delay = service._next_cycle_delay_seconds()
        finally:
            service_module.utc_now = original_utc_now

        self.assertEqual(delay, 240.0)

    def test_mixed_selection_combines_base_due_and_hot_due_players_once_each(self) -> None:
        now = datetime(2026, 3, 26, 12, 25, tzinfo=UTC)
        repo = FakeRepo()
        repo.players = [
            {
                "player_key": "us/area-52/baseplayer",
                "region": "us",
                "realm": "area-52",
                "name": "Baseplayer",
                "is_valid": True,
                "status": PlayerDataStatus.OK.value,
                "status_message": "",
                "gap_flag": False,
                "current_dungeon_scores": {},
                "last_sync_started_at": now - timedelta(minutes=16),
            },
            {
                "player_key": "us/area-52/hotplayer",
                "region": "us",
                "realm": "area-52",
                "name": "Hotplayer",
                "is_valid": True,
                "status": PlayerDataStatus.OK.value,
                "status_message": "",
                "gap_flag": False,
                "current_dungeon_scores": {},
                "last_sync_started_at": now - timedelta(minutes=5),
                "hot_ready_at": now - timedelta(minutes=2),
                "hot_until_at": now + timedelta(minutes=38),
            },
        ]
        service = SyncService(
            settings=make_settings(),
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=FakeRaiderIO(),
        )

        players, base_keys, hot_keys = service._select_players_for_sync(now=now)

        self.assertEqual(
            [player["player_key"] for player in players],
            ["us/area-52/baseplayer", "us/area-52/hotplayer"],
        )
        self.assertEqual(base_keys, {"us/area-52/baseplayer"})
        self.assertEqual(hot_keys, {"us/area-52/hotplayer"})
