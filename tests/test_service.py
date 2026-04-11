from datetime import UTC, datetime, timedelta
import unittest

from niru.clients.blizzard import BlizzardError
import niru.service as service_module
from niru.clients.raiderio import RaiderIONotFoundError
from niru.models import NormalizedRunCandidate, PlayerDataStatus
from niru.play_profile import (
    build_play_profile,
    current_week_hour_key,
)
from niru.service import SyncService, build_summary_header
from niru.storage import _build_key_run_metric_updates, _summarize_run_differences


def _current_batch_start(now, *, interval_minutes):
    if interval_minutes <= 0:
        return now
    interval_seconds = interval_minutes * 60
    timestamp = int(now.timestamp())
    bucket_start_timestamp = timestamp - (timestamp % interval_seconds)
    return datetime.fromtimestamp(bucket_start_timestamp, tz=UTC)


def _next_batch_at_or_after(moment, *, interval_minutes):
    batch_start = _current_batch_start(moment, interval_minutes=interval_minutes)
    if batch_start == moment:
        return batch_start
    return batch_start + timedelta(minutes=interval_minutes)


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
                    "active_idle_minutes": 40,
                    "predictive_hot_enabled": True,
                    "predictive_hot_threshold": 0.5,
                    "current_season": "season-mn-1",
                    "failure_backoff_seconds": 30.0,
                    "max_failure_backoff_seconds": 300.0,
                    "failure_backoff_jitter_seconds": 0.0,
                    "current_season": "season-mn-1",
                },
            )(),
            "blizzard": type(
                "Blizzard",
                (),
                {
                    "enabled": False,
                    "run_fingerprint_fuzz_seconds": 2,
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
        self.weekly_periods = {}

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
                "last_base_sync_started_at": None,
                "hot_ready_at": None,
                "hot_until_at": None,
                "play_profile_timezone": "America/Los_Angeles",
                "play_profile_first_week_start_at": None,
                "play_profile_last_seeded_at": None,
                "play_profile_weeks_observed": 0,
                "play_profile_hour_counts": [0] * 168,
                "play_profile_hour_probabilities": [0.0] * 168,
                "play_profile_seen_week_hours": [],
                "play_profile_last_enqueued_week_hour": "",
                "score_source": "",
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
            last_started = player.get("last_base_sync_started_at") or player.get(
                "last_sync_started_at"
            )
            if last_started is None:
                due.append(player)
                continue
            next_due_at = _current_batch_start(
                last_started,
                interval_minutes=interval_minutes,
            ) + timedelta(
                minutes=interval_minutes
            )
            if next_due_at <= now:
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
            if last_started is None:
                due.append(player)
                continue
            next_hot_ready_batch = _next_batch_at_or_after(
                hot_ready_at,
                interval_minutes=interval_minutes,
            )
            next_due_at = max(
                next_hot_ready_batch,
                _current_batch_start(
                    last_started,
                    interval_minutes=interval_minutes,
                )
                + timedelta(minutes=interval_minutes),
            )
            if next_due_at <= now:
                due.append(player)
        return due[:limit]

    def mark_sync_started(self, player_key, started_at, *, sync_kind):
        for player in self.players:
            if player["player_key"] == player_key:
                player["last_sync_started_at"] = started_at
                if sync_kind == "base":
                    player["last_base_sync_started_at"] = started_at

    def update_player_profile(
        self, player_key, *, current_dungeon_scores, current_total_score, score_source, synced_at
    ):
        for player in self.players:
            if player["player_key"] == player_key:
                player["status"] = PlayerDataStatus.OK.value
                player["status_message"] = ""
                player["current_dungeon_scores"] = current_dungeon_scores
                player["current_total_score"] = current_total_score
                player["score_source"] = score_source
                player["last_successful_sync_at"] = synced_at
                player["last_sync_completed_at"] = synced_at

    def get_known_run_ids(self, run_ids):
        return {run["keystone_run_id"] for run in self.runs if run["keystone_run_id"] in run_ids}

    def attach_player_to_run(self, run_id, player_key):
        for run in self.runs:
            if run["keystone_run_id"] == run_id:
                run.setdefault("discovered_from_player_keys", []).append(player_key)

    def upsert_normalized_run(self, candidate, *, player_key, season, synced_at, fuzz_seconds):
        for run in self.runs:
            if (
                run.get("keystone_run_id") is not None
                and candidate.keystone_run_id is not None
                and run.get("keystone_run_id") == candidate.keystone_run_id
            ) or (
                run.get("dungeon_id") == candidate.dungeon_id
                and run.get("mythic_level") == candidate.mythic_level
                and run.get("completed_at") is not None
                and candidate.completed_at is not None
                and abs((run["completed_at"] - candidate.completed_at).total_seconds()) <= fuzz_seconds
                and run.get("clear_time_ms") is not None
                and candidate.clear_time_ms is not None
                and abs(int(run["clear_time_ms"]) - int(candidate.clear_time_ms)) <= fuzz_seconds * 1000
            ):
                existing_short_name = str(run.get("short_name", "") or "")
                candidate_short_name = str(candidate.short_name or "")
                if candidate_short_name and candidate_short_name != candidate.dungeon:
                    short_name = candidate_short_name
                elif existing_short_name:
                    short_name = existing_short_name
                else:
                    short_name = candidate_short_name
                run.update(
                    {
                        "dungeon": candidate.dungeon,
                        "mythic_level": candidate.mythic_level,
                        "completed_at": candidate.completed_at,
                        "season": season,
                    }
                )
                run.update(
                    _build_key_run_metric_updates(
                        existing_run=run,
                        incoming_source=candidate.source,
                        metric_values={
                            "score": candidate.score,
                            "clear_time_ms": candidate.clear_time_ms,
                            "num_keystone_upgrades": candidate.num_keystone_upgrades,
                            "is_completed_within_time": candidate.is_completed_within_time,
                        },
                    )
                )
                if candidate.participants:
                    run["participants"] = candidate.participants
                if short_name:
                    run["short_name"] = short_name
                if candidate.keystone_run_id is not None:
                    run["keystone_run_id"] = candidate.keystone_run_id
                run.setdefault("sources", [])
                if candidate.source not in run["sources"]:
                    run["sources"].append(candidate.source)
                run.setdefault("discovered_from_player_keys", [])
                if player_key not in run["discovered_from_player_keys"]:
                    run["discovered_from_player_keys"].append(player_key)
                return False

        self.runs.append(
            {
                "keystone_run_id": candidate.keystone_run_id,
                "dungeon": candidate.dungeon,
                "short_name": candidate.short_name,
                "score": candidate.score,
                "mythic_level": candidate.mythic_level,
                "num_keystone_upgrades": candidate.num_keystone_upgrades,
                "completed_at": candidate.completed_at,
                "clear_time_ms": candidate.clear_time_ms,
                "dungeon_id": candidate.dungeon_id,
                "is_completed_within_time": candidate.is_completed_within_time,
                "discovered_from_player_keys": [player_key],
                "participants": candidate.participants,
                "season": season,
                "sources": [candidate.source],
            }
        )
        self.runs[-1].update(
            _build_key_run_metric_updates(
                existing_run=None,
                incoming_source=candidate.source,
                metric_values={
                    "score": candidate.score,
                    "clear_time_ms": candidate.clear_time_ms,
                    "num_keystone_upgrades": candidate.num_keystone_upgrades,
                    "is_completed_within_time": candidate.is_completed_within_time,
                },
            )
        )
        return True

    def find_run_by_fuzzy_fields(
        self,
        *,
        dungeon_id,
        mythic_level,
        completed_at,
        clear_time_ms,
        fuzz_seconds,
    ):
        for run in self.runs:
            run_dungeon_id = (
                run.get("map_challenge_mode_id") or run.get("dungeon_id") or run.get("zone_id")
            )
            if (
                run_dungeon_id == dungeon_id
                and run.get("mythic_level") == mythic_level
                and run.get("completed_at") is not None
                and completed_at is not None
                and abs((run["completed_at"] - completed_at).total_seconds()) <= fuzz_seconds
                and run.get("clear_time_ms") is not None
                and clear_time_ms is not None
                and abs(int(run["clear_time_ms"]) - int(clear_time_ms)) <= fuzz_seconds * 1000
            ):
                return run
        return None

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

    def clear_player_hot_window(self, *, player_key):
        for player in self.players:
            if player["player_key"] == player_key:
                player["hot_ready_at"] = None
                player["hot_until_at"] = None

    def upsert_player_play_profile(self, *, player_key, profile):
        for player in self.players:
            if player["player_key"] == player_key:
                player.update(profile)

    def mark_predictive_hot_enqueue(self, *, player_key, week_hour_key, hot_ready_at, hot_until_at):
        for player in self.players:
            if player["player_key"] != player_key:
                continue
            existing_hot_ready = player.get("hot_ready_at")
            existing_hot_until = player.get("hot_until_at")
            player["hot_ready_at"] = (
                hot_ready_at
                if existing_hot_ready is None
                else min(existing_hot_ready, hot_ready_at)
            )
            player["hot_until_at"] = (
                hot_until_at
                if existing_hot_until is None
                else max(existing_hot_until, hot_until_at)
            )
            player["play_profile_last_enqueued_week_hour"] = week_hour_key

    def get_runs_for_players(self, player_keys):
        return self.runs

    def get_runs_for_player(self, *, player_key, season):
        return [
            run
            for run in self.runs
            if run.get("season") == season
            and (
                player_key in run.get("discovered_from_player_keys", [])
                or any(
                    participant.get("player_key") == player_key
                    for participant in run.get("participants", [])
                )
            )
        ]

    def list_season_dungeons(self, *, season):
        return [d for d in self.season_dungeons if d["season"] == season]

    def normalize_run_short_names(self, *, season, dungeons):
        short_name_by_id = {
            int(dungeon["dungeon_id"]): dungeon["short_name"]
            for dungeon in dungeons
            if dungeon.get("dungeon_id") is not None and dungeon.get("short_name")
        }
        for run in self.runs:
            if run.get("season") != season:
                continue
            dungeon_id = run.get("dungeon_id") or run.get("zone_id") or run.get("map_challenge_mode_id")
            if dungeon_id in short_name_by_id:
                run["short_name"] = short_name_by_id[dungeon_id]

    def get_current_weekly_periods(self, *, now, regions):
        cached = {}
        for region in regions:
            period = self.weekly_periods.get(region)
            if period is None:
                continue
            if period["start"] <= now < period["end"]:
                cached[region] = period
        return cached

    def replace_weekly_periods(self, *, periods_by_region, synced_at):
        self.weekly_periods = dict(periods_by_region)

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
                    "completed_at": "2026-03-25T12:00:00+00:00",
                    "clear_time_ms": 1800000,
                    "zone_id": 101,
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
                    "completed_at": "2026-03-26T12:00:00+00:00",
                    "clear_time_ms": 1860000,
                    "zone_id": 101,
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


class FakeBlizzard:
    def __init__(self):
        self.api_calls = 0
        self.raise_error = False
        self.season_index_calls = 0
        self.season_detail_calls = 0
        self.current_period_index_calls = 0
        self.period_detail_calls = 0
        self.current_profile_payload = {
            "current_mythic_rating": {"rating": 400.2},
            "current_period": {
                "best_runs": [
                    {
                        "completed_timestamp": int(datetime(2026, 3, 25, 12, 0, tzinfo=UTC).timestamp() * 1000),
                        "duration": 1800000,
                        "keystone_level": 12,
                        "is_completed_within_time": True,
                        "dungeon": {"id": 101, "name": "Darkflame Cleft"},
                        "map_rating": {"rating": 200.1},
                        "mythic_rating": {"rating": 200.1},
                        "members": [],
                    }
                ]
            },
        }
        self.season_profile_payload = {
            "mythic_rating": {"rating": 400.2},
            "best_runs": [
                {
                    "completed_timestamp": int(datetime(2026, 3, 25, 12, 0, tzinfo=UTC).timestamp() * 1000),
                    "duration": 1800000,
                    "keystone_level": 12,
                    "is_completed_within_time": True,
                    "dungeon": {"id": 101, "name": {"en_US": "Darkflame Cleft"}},
                    "map_rating": {"rating": 200.1},
                    "mythic_rating": {"rating": 200.1},
                    "members": [],
                },
                {
                    "completed_timestamp": int(datetime(2026, 3, 25, 12, 0, 1, tzinfo=UTC).timestamp() * 1000),
                    "duration": 1801000,
                    "keystone_level": 13,
                    "is_completed_within_time": True,
                    "dungeon": {"id": 101, "name": {"en_US": "Darkflame Cleft"}},
                    "map_rating": {"rating": 210.0},
                    "mythic_rating": {"rating": 210.0},
                    "members": [],
                },
            ],
        }
        self.season_index_payload = {"seasons": [{"id": 17}]}
        self.current_period_index_payload = {
            "current_period": {
                "id": 1058,
            }
        }
        self.period_detail_payload = {
            "id": 1058,
            "start_timestamp": int(datetime(2026, 4, 7, 15, 0, tzinfo=UTC).timestamp() * 1000),
            "end_timestamp": int(datetime(2026, 4, 14, 15, 0, tzinfo=UTC).timestamp() * 1000),
        }
        self.season_detail_payload = {
            "id": 17,
            "dungeons": [
                {
                    "id": 101,
                    "slug": "darkflame-cleft",
                    "name": {"en_US": "Darkflame Cleft"},
                    "short_name": "DFC",
                }
            ],
        }
        self.dungeon_detail_payloads = {
            101: {
                "id": 101,
                "keystone_upgrades": [
                    {"upgrade_level": 1, "qualifying_duration": 1800000},
                    {"upgrade_level": 2, "qualifying_duration": 1440000},
                    {"upgrade_level": 3, "qualifying_duration": 1080000},
                ],
            }
        }
        self.dungeon_detail_calls = 0

    def get_current_season_index(self):
        self.api_calls += 1
        self.season_index_calls += 1
        return type("Result", (), {"payload": self.season_index_payload})()

    def get_season_detail(self, season_id):
        self.api_calls += 1
        self.season_detail_calls += 1
        return type("Result", (), {"payload": self.season_detail_payload})()

    def get_current_period_index(self):
        self.api_calls += 1
        self.current_period_index_calls += 1
        return type("Result", (), {"payload": self.current_period_index_payload})()

    def get_period_detail(self, period_id):
        self.api_calls += 1
        self.period_detail_calls += 1
        return type("Result", (), {"payload": self.period_detail_payload})()

    def get_character_mythic_keystone_profile(self, player):
        self.api_calls += 1
        if self.raise_error:
            raise BlizzardError("blizzard failed")
        return type("Result", (), {"payload": self.current_profile_payload})()

    def get_character_mythic_keystone_profile_season(self, player, season_id):
        self.api_calls += 1
        if self.raise_error:
            raise BlizzardError("blizzard failed")
        return type("Result", (), {"payload": self.season_profile_payload})()

    def get_mythic_keystone_dungeon(self, dungeon_id):
        self.api_calls += 1
        self.dungeon_detail_calls += 1
        payload = self.dungeon_detail_payloads[int(dungeon_id)]
        return type("Result", (), {"payload": payload})()


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
        self.assertEqual(sheets.last_metadata_rows[0], ("unique_runs", 2))
        self.assertEqual(repo.sync_docs[0]["api_calls"], 3)
        self.assertEqual(repo.sync_docs[0]["raiderio_api_calls"], 3)
        self.assertEqual(repo.sync_docs[0]["blizzard_api_calls"], 0)
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
            "Missing Blizzard weekly period for region us; weekly 10+ counts left blank.",
            repo.sync_docs[0]["warnings"],
        )

    def test_run_cycle_reuses_cached_current_weekly_periods(self) -> None:
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
        first_cycle_api_calls = raider.api_calls

        service.run_cycle()

        self.assertEqual(first_cycle_api_calls, 3)
        self.assertEqual(raider.api_calls, 5)

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

    def test_new_run_does_not_schedule_hot_window(self) -> None:
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
                "current_dungeon_scores": {},
                "last_successful_sync_at": now - timedelta(minutes=15),
            }
        ]
        raider = FakeRaiderIO()
        raider.profile_payload["mythic_plus_best_runs"] = []
        raider.profile_payload["mythic_plus_alternate_runs"] = []
        raider.profile_payload["mythic_plus_recent_runs"] = [
            {
                "keystone_run_id": 777,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "score": 200.0,
                "mythic_level": 13,
                "num_keystone_upgrades": 1,
                "completed_at": "2026-03-26T12:00:00+00:00",
                "clear_time_ms": 1800000,
                "map_challenge_mode_id": 101,
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
            },
        )()
        service._sync_player(player=repo.players[0], stats=stats, now=now, sync_kind="base")

        self.assertIsNone(repo.players[0].get("hot_ready_at"))
        self.assertIsNone(repo.players[0].get("hot_until_at"))

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

        self.assertEqual([player["player_key"] for player in players], ["us/area-52/mythics"])
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

    def test_hot_player_waits_for_next_batch_boundary(self) -> None:
        now = datetime(2026, 3, 26, 12, 29, tzinfo=UTC)
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
                "current_dungeon_scores": {},
                "last_sync_started_at": datetime(2026, 3, 26, 12, 24, tzinfo=UTC),
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
                "current_dungeon_scores": {},
                "last_sync_started_at": now,
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

        self.assertEqual(delay, 300.0)

    def test_next_cycle_delay_does_not_immediately_rerun_for_unsynced_player(self) -> None:
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
                "current_dungeon_scores": {},
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

        self.assertEqual(delay, 900.0)

    def test_next_cycle_delay_uses_next_base_bucket_not_exact_last_attempt(self) -> None:
        now = datetime(2026, 3, 26, 12, 50, 2, tzinfo=UTC)
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
                "current_dungeon_scores": {},
                "last_sync_started_at": datetime(2026, 3, 26, 12, 46, 40, tzinfo=UTC),
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

        self.assertEqual(delay, 598.0)

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
                "current_dungeon_scores": {},
                "last_sync_started_at": now - timedelta(minutes=25),
            },
            {
                "player_key": "us/area-52/hotplayer",
                "region": "us",
                "realm": "area-52",
                "name": "Hotplayer",
                "is_valid": True,
                "status": PlayerDataStatus.OK.value,
                "status_message": "",
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

    def test_hot_sync_does_not_delay_next_base_bucket(self) -> None:
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
                "current_dungeon_scores": {},
                "last_base_sync_started_at": datetime(2026, 3, 26, 12, 0, tzinfo=UTC),
                "last_sync_started_at": datetime(2026, 3, 26, 12, 20, tzinfo=UTC),
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

        players, base_keys, hot_keys = service._select_players_for_sync(now=now)

        self.assertEqual([player["player_key"] for player in players], ["us/area-52/mythics"])
        self.assertEqual(base_keys, {"us/area-52/mythics"})
        self.assertEqual(hot_keys, set())

    def test_run_cycle_force_sync_all_ignores_due_selection(self) -> None:
        settings = make_settings()
        repo = FakeRepo()
        now = datetime(2026, 3, 26, 12, 0, tzinfo=UTC)
        sheets = FakeSheets(["us/area-52/Readyone", "us/area-52/Readytwo"])
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=sheets,
            raiderio_client=FakeRaiderIO(),
        )

        service.run_cycle(force_sync_all=True)

        self.assertEqual(repo.sync_docs[0]["base_due_players_synced"], 2)

    def test_run_cycle_force_sync_all_can_target_single_player(self) -> None:
        settings = make_settings()
        repo = FakeRepo()
        sheets = FakeSheets(["us/area-52/Readyone", "us/area-52/Readytwo"])
        raiderio = FakeRaiderIO()
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=sheets,
            raiderio_client=raiderio,
        )

        service.run_cycle(force_sync_all=True, player_key="us/area-52/readytwo")

        self.assertEqual(repo.sync_docs[0]["base_due_players_synced"], 1)
        self.assertEqual(raiderio.api_calls, 3)
        synced_players = [
            player["player_key"]
            for player in repo.players
            if player.get("last_successful_sync_at") is not None
        ]
        self.assertEqual(synced_players, ["us/area-52/readytwo"])

    def test_predictive_hot_queue_enqueues_player_for_current_hour(self) -> None:
        now = datetime(2026, 3, 26, 20, 10, tzinfo=UTC)
        profile = build_play_profile(completed_at_values=[now], now=now)
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
                "current_dungeon_scores": {},
                **profile,
            }
        ]
        service = SyncService(
            settings=make_settings(),
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=FakeRaiderIO(),
        )
        stats = type("Stats", (), {"predictive_hot_players_queued": 0})()

        service._queue_predictive_hot_players(active_players=repo.players, now=now, stats=stats)

        self.assertEqual(stats.predictive_hot_players_queued, 1)
        self.assertEqual(repo.players[0]["play_profile_last_enqueued_week_hour"], current_week_hour_key(now))
        self.assertIsNotNone(repo.players[0]["hot_ready_at"])
        self.assertIsNotNone(repo.players[0]["hot_until_at"])

    def test_predictive_hot_queue_does_not_repeat_same_week_hour(self) -> None:
        now = datetime(2026, 3, 26, 20, 10, tzinfo=UTC)
        current_key = current_week_hour_key(now)
        profile = build_play_profile(completed_at_values=[now], now=now)
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
                "current_dungeon_scores": {},
                **profile,
                "play_profile_last_enqueued_week_hour": current_key,
            }
        ]
        service = SyncService(
            settings=make_settings(),
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=FakeRaiderIO(),
        )
        stats = type("Stats", (), {"predictive_hot_players_queued": 0})()

        service._queue_predictive_hot_players(active_players=repo.players, now=now, stats=stats)

        self.assertEqual(stats.predictive_hot_players_queued, 0)

    def test_predictive_queue_preserves_later_run_triggered_hot_window(self) -> None:
        now = datetime(2026, 3, 26, 20, 10, tzinfo=UTC)
        profile = build_play_profile(completed_at_values=[now], now=now)
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
                "current_dungeon_scores": {},
                "hot_ready_at": now + timedelta(minutes=20),
                "hot_until_at": now + timedelta(minutes=60),
                **profile,
            }
        ]
        service = SyncService(
            settings=make_settings(),
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=FakeRaiderIO(),
        )
        stats = type("Stats", (), {"predictive_hot_players_queued": 0})()

        service._queue_predictive_hot_players(active_players=repo.players, now=now, stats=stats)

        self.assertEqual(repo.players[0]["hot_until_at"], now + timedelta(minutes=60))
        self.assertEqual(repo.players[0]["hot_ready_at"], datetime(2026, 3, 26, 20, 0, tzinfo=UTC))

    def test_sync_player_updates_play_profile_from_new_runs(self) -> None:
        now = datetime(2026, 3, 26, 12, 30, tzinfo=UTC)
        repo = FakeRepo()
        existing_profile = build_play_profile(
            completed_at_values=[datetime(2026, 3, 19, 12, 0, tzinfo=UTC)],
            now=now,
        )
        repo.players = [
            {
                "player_key": "us/area-52/mythics",
                "region": "us",
                "realm": "area-52",
                "name": "Mythics",
                "is_valid": True,
                "status": PlayerDataStatus.OK.value,
                "status_message": "",
                "current_dungeon_scores": {},
                **existing_profile,
            }
        ]
        raider = FakeRaiderIO()
        raider.profile_payload["mythic_plus_best_runs"] = []
        raider.profile_payload["mythic_plus_alternate_runs"] = []
        raider.profile_payload["mythic_plus_recent_runs"] = [
            {
                "keystone_run_id": 777,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "score": 200.0,
                "mythic_level": 13,
                "num_keystone_upgrades": 1,
                "completed_at": "2026-03-26T12:00:00+00:00",
                "clear_time_ms": 1800000,
                "map_challenge_mode_id": 101,
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
            },
        )()

        service._sync_player(player=repo.players[0], stats=stats, now=now, sync_kind="base")

        self.assertEqual(repo.players[0]["play_profile_weeks_observed"], 2)
        self.assertEqual(
            len(repo.players[0]["play_profile_seen_week_hours"]),
            2,
        )

    def test_old_new_run_does_not_create_hot_window(self) -> None:
        now = datetime(2026, 4, 5, 8, 15, tzinfo=UTC)
        repo = FakeRepo()
        repo.players = [
            {
                "player_key": "us/illidan/gebus",
                "region": "us",
                "realm": "illidan",
                "name": "Gëbus",
                "is_valid": True,
                "status": PlayerDataStatus.OK.value,
                "status_message": "",
                "current_dungeon_scores": {},
            }
        ]
        raider = FakeRaiderIO()
        raider.profile_payload["mythic_plus_recent_runs"] = [
            {
                "keystone_run_id": 888,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "score": 200.0,
                "mythic_level": 13,
                "num_keystone_upgrades": 1,
                "completed_at": "2026-04-04T19:09:30+00:00",
                "clear_time_ms": 1800000,
                "map_challenge_mode_id": 101,
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
            },
        )()

        service._sync_player(player=repo.players[0], stats=stats, now=now, sync_kind="base")

        self.assertIsNone(repo.players[0].get("hot_ready_at"))
        self.assertIsNone(repo.players[0].get("hot_until_at"))

    def test_blizzard_scores_override_raiderio_when_enabled(self) -> None:
        settings = make_settings()
        settings.blizzard.enabled = True
        repo = FakeRepo()
        sheets = FakeSheets(["us/area-52/Mythics"])
        raider = FakeRaiderIO()
        blizzard = FakeBlizzard()
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=sheets,
            raiderio_client=raider,
            blizzard_client=blizzard,
        )

        service.run_cycle()

        self.assertEqual(sheets.last_rows[0][3], 400.2)
        self.assertEqual(repo.players[0]["score_source"], "blizzard")
        self.assertEqual(repo.sync_docs[0]["raiderio_api_calls"], 1)
        self.assertEqual(repo.sync_docs[0]["blizzard_api_calls"], 7)
        self.assertEqual(repo.sync_docs[0]["api_calls"], 8)

    def test_raiderio_scores_used_when_blizzard_fails(self) -> None:
        settings = make_settings()
        settings.blizzard.enabled = True
        repo = FakeRepo()
        sheets = FakeSheets(["us/area-52/Mythics"])
        raider = FakeRaiderIO()
        blizzard = FakeBlizzard()
        blizzard.raise_error = True
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=sheets,
            raiderio_client=raider,
            blizzard_client=blizzard,
        )

        service.run_cycle()

        self.assertEqual(sheets.last_rows[0][3], 370.2)
        self.assertEqual(repo.players[0]["score_source"], "raiderio")

    def test_blizzard_and_raiderio_runs_merge_into_one_doc(self) -> None:
        settings = make_settings()
        settings.blizzard.enabled = True
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
                "current_dungeon_scores": {},
            }
        ]
        raider = FakeRaiderIO()
        raider.profile_payload["mythic_plus_best_runs"] = [
            {
                "keystone_run_id": 123,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "score": 200.1,
                "mythic_level": 12,
                "num_keystone_upgrades": 1,
                "completed_at": "2026-03-25T12:00:01+00:00",
                "clear_time_ms": 1801000,
                "zone_id": 101,
            }
        ]
        raider.profile_payload["mythic_plus_alternate_runs"] = []
        blizzard = FakeBlizzard()
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=raider,
            blizzard_client=blizzard,
        )
        stats = type(
            "Stats",
            (),
            {"partial": False, "warnings": [], "new_runs": 0, "detail_fetches": 0},
        )()

        service._sync_player(player=repo.players[0], stats=stats, now=datetime(2026, 3, 26, tzinfo=UTC), sync_kind="base")

        matching_runs = [run for run in repo.runs if run.get("dungeon") == "Darkflame Cleft"]
        self.assertEqual(len(matching_runs), 2)
        self.assertTrue(any(run.get("keystone_run_id") == 123 for run in matching_runs))
        self.assertTrue(all(run.get("dungeon_id") == 101 for run in matching_runs))

    def test_blizzard_periods_are_used_when_enabled(self) -> None:
        settings = make_settings()
        settings.blizzard.enabled = True
        repo = FakeRepo()
        sheets = FakeSheets(["us/area-52/Mythics"])
        blizzard = FakeBlizzard()
        blizzard.period_detail_payload["start_timestamp"] = int(
            datetime(2026, 4, 1, 15, 0, tzinfo=UTC).timestamp() * 1000
        )
        blizzard.period_detail_payload["end_timestamp"] = int(
            datetime(2026, 4, 15, 15, 0, tzinfo=UTC).timestamp() * 1000
        )
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=sheets,
            raiderio_client=FakeRaiderIO(),
            blizzard_client=blizzard,
        )

        service.run_cycle()

        self.assertEqual(repo.sync_docs[0]["weekly_periods"]["us"]["period"], 1058)
        self.assertEqual(blizzard.season_index_calls, 1)
        self.assertEqual(blizzard.season_detail_calls, 1)
        self.assertEqual(repo.sync_docs[0]["raiderio_api_calls"], 1)
        self.assertEqual(repo.sync_docs[0]["blizzard_api_calls"], 7)
        self.assertEqual(repo.sync_docs[0]["api_calls"], 8)

    def test_blizzard_enrichment_does_not_clear_raiderio_upgrade_count(self) -> None:
        settings = make_settings()
        settings.blizzard.enabled = True
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
                "current_dungeon_scores": {},
            }
        ]
        raider = FakeRaiderIO()
        raider.profile_payload["mythic_plus_best_runs"] = [
            {
                "keystone_run_id": 123,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "score": 200.1,
                "mythic_level": 12,
                "num_keystone_upgrades": 2,
                "completed_at": "2026-03-25T12:00:01+00:00",
                "clear_time_ms": 1801000,
                "map_challenge_mode_id": 101,
            }
        ]
        raider.profile_payload["mythic_plus_alternate_runs"] = []
        blizzard = FakeBlizzard()
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=raider,
            blizzard_client=blizzard,
        )
        stats = type(
            "Stats",
            (),
            {"partial": False, "warnings": [], "new_runs": 0, "detail_fetches": 0},
        )()

        service._sync_player(
            player=repo.players[0],
            stats=stats,
            now=datetime(2026, 3, 26, tzinfo=UTC),
            sync_kind="base",
        )

        matching_runs = [run for run in repo.runs if run.get("keystone_run_id") == 123]
        self.assertEqual(len(matching_runs), 1)
        self.assertEqual(matching_runs[0].get("num_keystone_upgrades"), 2)

    def test_blizzard_only_run_uses_cached_short_name(self) -> None:
        settings = make_settings()
        settings.blizzard.enabled = True
        repo = FakeRepo()
        repo.season_dungeons = [
            {
                "season": "season-mn-1",
                "dungeon_id": 101,
                "challenge_mode_id": 101,
                "slug": "darkflame-cleft",
                "name": "Darkflame Cleft",
                "short_name": "DFC",
            }
        ]
        repo.players = [
            {
                "player_key": "us/area-52/mythics",
                "region": "us",
                "realm": "area-52",
                "name": "Mythics",
                "is_valid": True,
                "status": PlayerDataStatus.OK.value,
                "status_message": "",
                "current_dungeon_scores": {},
            }
        ]
        raider = FakeRaiderIO()
        raider.profile_payload["mythic_plus_best_runs"] = []
        raider.profile_payload["mythic_plus_alternate_runs"] = []
        blizzard = FakeBlizzard()
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=raider,
            blizzard_client=blizzard,
        )
        stats = type(
            "Stats",
            (),
            {"partial": False, "warnings": [], "new_runs": 0, "detail_fetches": 0},
        )()

        service._sync_player(
            player=repo.players[0],
            stats=stats,
            now=datetime(2026, 3, 26, tzinfo=UTC),
            sync_kind="base",
        )

        self.assertTrue(any(run.get("short_name") == "DFC" for run in repo.runs))

    def test_cached_season_dungeons_repair_existing_run_short_names(self) -> None:
        settings = make_settings()
        repo = FakeRepo()
        repo.season_dungeons = [
            {
                "season": "season-mn-1",
                "dungeon_id": 101,
                "slug": "darkflame-cleft",
                "name": "Darkflame Cleft",
                "short_name": "DFC",
            }
        ]
        repo.runs = [
            {
                "season": "season-mn-1",
                "dungeon_id": 101,
                "short_name": "Darkflame Cleft",
            }
        ]
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=FakeRaiderIO(),
        )

        service._ensure_season_dungeons(now=datetime(2026, 3, 26, tzinfo=UTC))

        self.assertEqual(repo.runs[0]["short_name"], "DFC")

    def test_invalid_cached_short_names_refresh_from_raiderio_static_data(self) -> None:
        settings = make_settings()
        repo = FakeRepo()
        repo.season_dungeons = [
            {
                "season": "season-mn-1",
                "dungeon_id": 101,
                "slug": "darkflame-cleft",
                "name": "Darkflame Cleft",
                "short_name": "Darkflame Cleft",
            }
        ]
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=FakeRaiderIO(),
        )

        dungeons = service._ensure_season_dungeons(now=datetime(2026, 3, 26, tzinfo=UTC))

        self.assertEqual(dungeons[0]["short_name"], "DFC")

    def test_blizzard_long_short_name_does_not_overwrite_existing_raiderio_short_name(self) -> None:
        settings = make_settings()
        repo = FakeRepo()
        repo.runs = [
            {
                "keystone_run_id": 123,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "score": 312.4,
                "mythic_level": 10,
                "num_keystone_upgrades": 2,
                "completed_at": datetime(2026, 3, 25, 12, 0, 0, tzinfo=UTC),
                "clear_time_ms": 1500000,
                "dungeon_id": 101,
                "season": "season-mn-1",
                "sources": ["raiderio"],
            }
        ]

        blizzard_candidate = NormalizedRunCandidate(
            source="blizzard",
            keystone_run_id=None,
            completed_at=datetime(2026, 3, 25, 12, 0, 1, tzinfo=UTC),
            clear_time_ms=1501000,
            dungeon_id=101,
            dungeon="Darkflame Cleft",
            short_name="Darkflame Cleft",
            mythic_level=10,
            num_keystone_upgrades=None,
            score=312.8,
            is_completed_within_time=True,
            participants=[],
            raw_payload={},
        )

        inserted = repo.upsert_normalized_run(
            blizzard_candidate,
            player_key="us/area-52/mythics",
            season="season-mn-1",
            synced_at=datetime(2026, 3, 26, tzinfo=UTC),
            fuzz_seconds=2,
        )

        self.assertFalse(inserted)
        self.assertEqual(repo.runs[0]["short_name"], "DFC")

    def test_empty_participants_do_not_overwrite_existing_participants(self) -> None:
        repo = FakeRepo()
        repo.runs = [
            {
                "keystone_run_id": None,
                "dungeon": "Seat of the Triumvirate",
                "short_name": "SOTT",
                "score": 319.9,
                "mythic_level": 12,
                "num_keystone_upgrades": 1,
                "completed_at": datetime(2026, 4, 3, 20, 0, 0, tzinfo=UTC),
                "clear_time_ms": 1900000,
                "dungeon_id": 239,
                "map_challenge_mode_id": 239,
                "season": "season-mn-1",
                "sources": ["blizzard"],
                "participants": [{"player_key": "us/proudmoore/test", "name": "Test"}],
            }
        ]

        inserted = repo.upsert_normalized_run(
            NormalizedRunCandidate(
                source="raiderio",
                keystone_run_id=123,
                completed_at=datetime(2026, 4, 3, 20, 0, 1, tzinfo=UTC),
                clear_time_ms=1901000,
                dungeon_id=239,
                dungeon="Seat of the Triumvirate",
                short_name="SOTT",
                mythic_level=12,
                num_keystone_upgrades=1,
                score=319.9154,
                is_completed_within_time=True,
                participants=[],
                raw_payload={},
            ),
            player_key="us/proudmoore/test",
            season="season-mn-1",
            synced_at=datetime(2026, 4, 6, tzinfo=UTC),
            fuzz_seconds=2,
        )

        self.assertFalse(inserted)
        self.assertEqual(
            repo.runs[0]["participants"],
            [{"player_key": "us/proudmoore/test", "name": "Test"}],
        )

    def test_blizzard_score_replaces_existing_raiderio_score_once(self) -> None:
        repo = FakeRepo()
        repo.runs = [
            {
                "keystone_run_id": 123,
                "dungeon": "Seat of the Triumvirate",
                "short_name": "SOTT",
                "score": 319.9,
                "run_metrics_source": "raiderio",
                "mythic_level": 12,
                "num_keystone_upgrades": 1,
                "completed_at": datetime(2026, 4, 3, 20, 0, 0, tzinfo=UTC),
                "clear_time_ms": 1900000,
                "dungeon_id": 239,
                "map_challenge_mode_id": 239,
                "season": "season-mn-1",
                "sources": ["raiderio"],
                "participants": [],
            }
        ]

        inserted = repo.upsert_normalized_run(
            NormalizedRunCandidate(
                source="blizzard",
                keystone_run_id=123,
                completed_at=datetime(2026, 4, 3, 20, 0, 0, tzinfo=UTC),
                clear_time_ms=1900000,
                dungeon_id=239,
                dungeon="Seat of the Triumvirate",
                short_name="SOTT",
                mythic_level=12,
                num_keystone_upgrades=1,
                score=319.9154,
                is_completed_within_time=True,
                participants=[],
                raw_payload={},
            ),
            player_key="us/proudmoore/test",
            season="season-mn-1",
            synced_at=datetime(2026, 4, 6, tzinfo=UTC),
            fuzz_seconds=2,
        )

        self.assertFalse(inserted)
        self.assertEqual(repo.runs[0]["score"], 319.9154)
        self.assertEqual(repo.runs[0]["run_metrics_source"], "blizzard")

    def test_raiderio_score_does_not_replace_existing_blizzard_score(self) -> None:
        repo = FakeRepo()
        repo.runs = [
            {
                "keystone_run_id": 123,
                "dungeon": "Seat of the Triumvirate",
                "short_name": "SOTT",
                "score": 319.9154,
                "run_metrics_source": "blizzard",
                "mythic_level": 12,
                "num_keystone_upgrades": 1,
                "completed_at": datetime(2026, 4, 3, 20, 0, 0, tzinfo=UTC),
                "clear_time_ms": 1900000,
                "dungeon_id": 239,
                "map_challenge_mode_id": 239,
                "season": "season-mn-1",
                "sources": ["blizzard"],
                "participants": [],
            }
        ]

        inserted = repo.upsert_normalized_run(
            NormalizedRunCandidate(
                source="raiderio",
                keystone_run_id=123,
                completed_at=datetime(2026, 4, 3, 20, 0, 0, tzinfo=UTC),
                clear_time_ms=1900000,
                dungeon_id=239,
                dungeon="Seat of the Triumvirate",
                short_name="SOTT",
                mythic_level=12,
                num_keystone_upgrades=1,
                score=319.9,
                is_completed_within_time=True,
                participants=[],
                raw_payload={},
            ),
            player_key="us/proudmoore/test",
            season="season-mn-1",
            synced_at=datetime(2026, 4, 6, tzinfo=UTC),
            fuzz_seconds=2,
        )

        self.assertFalse(inserted)
        self.assertEqual(repo.runs[0]["score"], 319.9154)
        self.assertEqual(repo.runs[0]["run_metrics_source"], "blizzard")

    def test_raiderio_does_not_replace_existing_blizzard_key_metrics(self) -> None:
        repo = FakeRepo()
        repo.runs = [
            {
                "keystone_run_id": 123,
                "dungeon": "Seat of the Triumvirate",
                "short_name": "SOTT",
                "score": 319.9154,
                "run_metrics_source": "blizzard",
                "mythic_level": 12,
                "num_keystone_upgrades": 1,
                "completed_at": datetime(2026, 4, 3, 20, 0, 0, tzinfo=UTC),
                "clear_time_ms": 1900000,
                "dungeon_id": 239,
                "map_challenge_mode_id": 239,
                "is_completed_within_time": True,
                "season": "season-mn-1",
                "sources": ["blizzard"],
                "participants": [],
            }
        ]

        inserted = repo.upsert_normalized_run(
            NormalizedRunCandidate(
                source="raiderio",
                keystone_run_id=123,
                completed_at=datetime(2026, 4, 3, 20, 0, 0, tzinfo=UTC),
                clear_time_ms=1905000,
                dungeon_id=239,
                dungeon="Seat of the Triumvirate",
                short_name="SOTT",
                mythic_level=12,
                num_keystone_upgrades=0,
                score=319.9,
                is_completed_within_time=False,
                participants=[],
                raw_payload={},
            ),
            player_key="us/proudmoore/test",
            season="season-mn-1",
            synced_at=datetime(2026, 4, 6, tzinfo=UTC),
            fuzz_seconds=600,
        )

        self.assertFalse(inserted)
        self.assertEqual(repo.runs[0]["score"], 319.9154)
        self.assertEqual(repo.runs[0]["clear_time_ms"], 1900000)
        self.assertEqual(repo.runs[0]["num_keystone_upgrades"], 1)
        self.assertTrue(repo.runs[0]["is_completed_within_time"])
        self.assertEqual(repo.runs[0]["run_metrics_source"], "blizzard")

    def test_raiderio_can_fill_missing_blizzard_owned_key_metrics(self) -> None:
        repo = FakeRepo()
        repo.runs = [
            {
                "keystone_run_id": 123,
                "dungeon": "Seat of the Triumvirate",
                "short_name": "SOTT",
                "score": 319.9154,
                "run_metrics_source": "blizzard",
                "mythic_level": 12,
                "num_keystone_upgrades": None,
                "completed_at": datetime(2026, 4, 3, 20, 0, 0, tzinfo=UTC),
                "clear_time_ms": 1900000,
                "dungeon_id": 239,
                "map_challenge_mode_id": 239,
                "is_completed_within_time": True,
                "season": "season-mn-1",
                "sources": ["blizzard"],
                "participants": [],
            }
        ]

        inserted = repo.upsert_normalized_run(
            NormalizedRunCandidate(
                source="raiderio",
                keystone_run_id=123,
                completed_at=datetime(2026, 4, 3, 20, 0, 0, tzinfo=UTC),
                clear_time_ms=1905000,
                dungeon_id=239,
                dungeon="Seat of the Triumvirate",
                short_name="SOTT",
                mythic_level=12,
                num_keystone_upgrades=0,
                score=319.9,
                is_completed_within_time=False,
                participants=[],
                raw_payload={},
            ),
            player_key="us/proudmoore/test",
            season="season-mn-1",
            synced_at=datetime(2026, 4, 6, tzinfo=UTC),
            fuzz_seconds=600,
        )

        self.assertFalse(inserted)
        self.assertEqual(repo.runs[0]["num_keystone_upgrades"], 0)
        self.assertEqual(repo.runs[0]["clear_time_ms"], 1900000)
        self.assertTrue(repo.runs[0]["is_completed_within_time"])
        self.assertEqual(repo.runs[0]["run_metrics_source"], "blizzard")

    def test_blizzard_unmatched_run_infers_num_keystone_upgrades_from_dungeon_metadata(self) -> None:
        settings = make_settings()
        settings.blizzard.enabled = True
        repo = FakeRepo()
        repo.season_dungeons = [
            {
                "season": "season-mn-1",
                "dungeon_id": 101,
                "challenge_mode_id": 101,
                "slug": "darkflame-cleft",
                "name": "Darkflame Cleft",
                "short_name": "DFC",
            }
        ]
        repo.players = [
            {
                "player_key": "us/area-52/mythics",
                "region": "us",
                "realm": "area-52",
                "name": "Mythics",
                "is_valid": True,
                "status": PlayerDataStatus.OK.value,
                "status_message": "",
                "current_dungeon_scores": {},
            }
        ]
        raider = FakeRaiderIO()
        raider.profile_payload["mythic_plus_best_runs"] = []
        raider.profile_payload["mythic_plus_alternate_runs"] = []
        blizzard = FakeBlizzard()
        blizzard.current_profile_payload["current_period"]["best_runs"] = [
            {
                "completed_timestamp": int(
                    datetime(2026, 3, 25, 12, 0, tzinfo=UTC).timestamp() * 1000
                ),
                "duration": 1400000,
                "keystone_level": 12,
                "is_completed_within_time": True,
                "dungeon": {"id": 101, "name": {"en_US": "Darkflame Cleft"}},
                "map_rating": {"rating": 200.1},
                "mythic_rating": {"rating": 200.1},
                "members": [],
            }
        ]
        blizzard.season_profile_payload["best_runs"] = []
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=raider,
            blizzard_client=blizzard,
        )
        stats = type(
            "Stats",
            (),
            {"partial": False, "warnings": [], "new_runs": 0, "detail_fetches": 0},
        )()

        service._sync_player(
            player=repo.players[0],
            stats=stats,
            now=datetime(2026, 3, 26, tzinfo=UTC),
            sync_kind="base",
        )

        self.assertEqual(repo.runs[0]["num_keystone_upgrades"], 2)
        self.assertEqual(blizzard.dungeon_detail_calls, 1)

    def test_blizzard_matched_raiderio_run_skips_upgrade_inference(self) -> None:
        settings = make_settings()
        settings.blizzard.enabled = True
        repo = FakeRepo()
        repo.season_dungeons = [
            {
                "season": "season-mn-1",
                "dungeon_id": 101,
                "challenge_mode_id": 101,
                "slug": "darkflame-cleft",
                "name": "Darkflame Cleft",
                "short_name": "DFC",
            }
        ]
        repo.players = [
            {
                "player_key": "us/area-52/mythics",
                "region": "us",
                "realm": "area-52",
                "name": "Mythics",
                "is_valid": True,
                "status": PlayerDataStatus.OK.value,
                "status_message": "",
                "current_dungeon_scores": {},
            }
        ]
        repo.runs = [
            {
                "keystone_run_id": 123,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "score": 200.1,
                "mythic_level": 12,
                "num_keystone_upgrades": 1,
                "completed_at": datetime(2026, 3, 25, 12, 0, 0, tzinfo=UTC),
                "clear_time_ms": 1400000,
                "dungeon_id": 101,
                "map_challenge_mode_id": 101,
                "season": "season-mn-1",
                "sources": ["raiderio"],
            }
        ]
        raider = FakeRaiderIO()
        raider.profile_payload["mythic_plus_recent_runs"] = []
        raider.profile_payload["mythic_plus_best_runs"] = [
            {
                "keystone_run_id": 123,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "score": 200.1,
                "mythic_level": 12,
                "num_keystone_upgrades": 1,
                "completed_at": "2026-03-25T12:00:00+00:00",
                "clear_time_ms": 1400000,
                "map_challenge_mode_id": 101,
                "zone_id": 999,
            }
        ]
        raider.profile_payload["mythic_plus_alternate_runs"] = []
        blizzard = FakeBlizzard()
        blizzard.current_profile_payload["current_period"]["best_runs"] = [
            {
                "completed_timestamp": int(
                    datetime(2026, 3, 25, 12, 0, tzinfo=UTC).timestamp() * 1000
                ),
                "duration": 1400000,
                "keystone_level": 12,
                "is_completed_within_time": True,
                "dungeon": {"id": 101, "name": {"en_US": "Darkflame Cleft"}},
                "map_rating": {"rating": 200.1},
                "mythic_rating": {"rating": 200.1},
                "members": [],
            }
        ]
        blizzard.season_profile_payload["best_runs"] = []
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=FakeSheets([]),
            raiderio_client=raider,
            blizzard_client=blizzard,
        )
        stats = type(
            "Stats",
            (),
            {"partial": False, "warnings": [], "new_runs": 0, "detail_fetches": 0},
        )()

        service._sync_player(
            player=repo.players[0],
            stats=stats,
            now=datetime(2026, 3, 26, tzinfo=UTC),
            sync_kind="base",
        )

        self.assertEqual(repo.runs[0]["num_keystone_upgrades"], 1)
        self.assertEqual(blizzard.dungeon_detail_calls, 0)

    def test_surprising_run_differences_include_large_score_and_name_changes(self) -> None:
        differences = _summarize_run_differences(
            {
                "_id": "abc123",
                "keystone_run_id": 123,
                "map_challenge_mode_id": 101,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "mythic_level": 10,
                "completed_at": datetime(2026, 3, 25, 12, 0, 0, tzinfo=UTC),
                "clear_time_ms": 1500000,
                "score": 300.0,
                "is_completed_within_time": True,
            },
            NormalizedRunCandidate(
                source="blizzard",
                keystone_run_id=None,
                completed_at=datetime(2026, 3, 25, 12, 0, 5, tzinfo=UTC),
                clear_time_ms=1510000,
                dungeon_id=101,
                dungeon="Completely Different Name",
                short_name="CDF",
                mythic_level=10,
                num_keystone_upgrades=None,
                score=306.5,
                is_completed_within_time=False,
                participants=[],
                raw_payload={},
            ),
            fuzz_seconds=2,
        )

        self.assertTrue(any("score_delta=" in difference for difference in differences))
        self.assertTrue(any("dungeon_name" in difference for difference in differences))
        self.assertTrue(any("short_name" in difference for difference in differences))
        self.assertTrue(any("is_completed_within_time" in difference for difference in differences))

    def test_surprising_run_differences_ignore_blocked_key_metric_changes(self) -> None:
        differences = _summarize_run_differences(
            {
                "_id": "abc123",
                "keystone_run_id": 123,
                "map_challenge_mode_id": 101,
                "dungeon": "Darkflame Cleft",
                "short_name": "DFC",
                "mythic_level": 10,
                "completed_at": datetime(2026, 3, 25, 12, 0, 0, tzinfo=UTC),
                "clear_time_ms": 1500000,
                "score": 300.0,
                "run_metrics_source": "blizzard",
                "is_completed_within_time": True,
            },
            NormalizedRunCandidate(
                source="raiderio",
                keystone_run_id=None,
                completed_at=datetime(2026, 3, 25, 12, 0, 0, tzinfo=UTC),
                clear_time_ms=1510000,
                dungeon_id=101,
                dungeon="Darkflame Cleft",
                short_name="DFC",
                mythic_level=10,
                num_keystone_upgrades=None,
                score=306.5,
                is_completed_within_time=False,
                participants=[],
                raw_payload={},
            ),
            fuzz_seconds=2,
        )

        self.assertFalse(any("score_delta=" in difference for difference in differences))
        self.assertFalse(any("clear_time_delta_ms" in difference for difference in differences))
        self.assertFalse(any("is_completed_within_time" in difference for difference in differences))

    def test_blizzard_season_context_is_cached_across_cycles_until_period_changes(self) -> None:
        settings = make_settings()
        settings.blizzard.enabled = True
        repo = FakeRepo()
        sheets = FakeSheets(["us/area-52/Mythics"])
        blizzard = FakeBlizzard()
        blizzard.period_detail_payload["start_timestamp"] = int(
            datetime(2026, 4, 1, 15, 0, tzinfo=UTC).timestamp() * 1000
        )
        blizzard.period_detail_payload["end_timestamp"] = int(
            datetime(2026, 4, 15, 15, 0, tzinfo=UTC).timestamp() * 1000
        )
        raider = FakeRaiderIO()
        service = SyncService(
            settings=settings,
            repository=repo,
            sheets_client=sheets,
            raiderio_client=raider,
            blizzard_client=blizzard,
        )

        service.run_cycle()
        service.run_cycle()

        self.assertEqual(blizzard.season_index_calls, 1)
        self.assertEqual(blizzard.season_detail_calls, 1)
