"""MongoDB persistence layer."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import logging
from typing import Any

from pymongo import ASCENDING

from niru.config import MongoSettings
from niru.models import (
    NormalizedRunCandidate,
    PlayerDataStatus,
    RosterEntry,
    SeasonDungeon,
    ensure_utc, PlayerIdentity,
)

LOGGER = logging.getLogger(__name__)
KEY_RUN_METRIC_FIELDS = (
    "score",
    "clear_time_ms",
    "par_time_ms",
    "num_keystone_upgrades",
    "is_completed_within_time",
)


def _safe_isoformat(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    return None


def _safe_utc_datetime(value: Any) -> datetime | None:
    parsed = _safe_isoformat(value)
    if parsed is None:
        return None
    return ensure_utc(parsed)


def _candidate_short_name_for_update(
    candidate: NormalizedRunCandidate,
    existing_short_name: str,
) -> str | None:
    """Return the short name to persist, preserving known abbreviations."""

    candidate_short_name = str(candidate.short_name or "").strip()
    candidate_dungeon = str(candidate.dungeon or "").strip()
    existing_short_name = str(existing_short_name or "").strip()
    if candidate_short_name and candidate_short_name != candidate_dungeon:
        return candidate_short_name
    if existing_short_name:
        return existing_short_name
    if candidate_short_name:
        return candidate_short_name
    return None


def _resolved_run_metrics_source(existing_run: dict[str, Any] | None) -> str | None:
    """Return the authoritative source for key run metrics on a run document."""

    if not existing_run:
        return None
    explicit = str(existing_run.get("run_metrics_source", "") or "").strip().lower()
    if explicit in {"blizzard", "raiderio"}:
        return explicit
    sources = {
        str(source).strip().lower()
        for source in existing_run.get("sources", []) or []
        if str(source).strip()
    }
    if "blizzard" in sources:
        return "blizzard"
    if "raiderio" in sources:
        return "raiderio"
    return None


def _should_update_key_run_metric(
    *,
    existing_run: dict[str, Any] | None,
    incoming_source: str,
    field_name: str,
    incoming_value: Any,
) -> bool:
    """Decide whether an incoming key run metric should replace the stored value."""

    if incoming_value is None:
        return False
    if not existing_run:
        return True
    if existing_run.get(field_name) is None:
        return True

    existing_source = _resolved_run_metrics_source(existing_run)
    if existing_source is None:
        return True
    if existing_source == incoming_source:
        return True
    return existing_source == "raiderio" and incoming_source == "blizzard"


def _build_key_run_metric_updates(
    *,
    existing_run: dict[str, Any] | None,
    incoming_source: str,
    metric_values: dict[str, Any],
) -> dict[str, Any]:
    """Return the allowed key run metric updates for an incoming source."""

    updates: dict[str, Any] = {}
    wrote_metric = False
    for field_name, incoming_value in metric_values.items():
        if _should_update_key_run_metric(
            existing_run=existing_run,
            incoming_source=incoming_source,
            field_name=field_name,
            incoming_value=incoming_value,
        ):
            updates[field_name] = incoming_value
            wrote_metric = True

    if not wrote_metric:
        return updates

    existing_source = _resolved_run_metrics_source(existing_run)
    if (
        existing_source is None
        or existing_source == incoming_source
        or (existing_source == "raiderio" and incoming_source == "blizzard")
    ):
        updates["run_metrics_source"] = incoming_source
    return updates


def _summarize_run_differences(
    existing_run: dict[str, Any],
    candidate: NormalizedRunCandidate,
    *,
    fuzz_seconds: int,
) -> list[str]:
    """Describe surprising differences between a stored run and a matched candidate."""

    warnings: list[str] = []
    existing_dungeon_id = (
        existing_run.get("map_challenge_mode_id")
        or existing_run.get("dungeon_id")
        or existing_run.get("zone_id")
    )
    if existing_dungeon_id is not None and int(existing_dungeon_id) != int(candidate.dungeon_id):
        warnings.append(
            f"dungeon_id existing={int(existing_dungeon_id)} candidate={int(candidate.dungeon_id)}"
        )

    existing_level = existing_run.get("mythic_level")
    if existing_level is not None and int(existing_level) != int(candidate.mythic_level):
        warnings.append(
            f"mythic_level existing={int(existing_level)} candidate={int(candidate.mythic_level)}"
        )

    existing_completed_at = _safe_utc_datetime(existing_run.get("completed_at"))
    if existing_completed_at is not None:
        completed_delta_seconds = abs(
            (existing_completed_at - ensure_utc(candidate.completed_at)).total_seconds()
        )
        if completed_delta_seconds > max(fuzz_seconds, 0):
            warnings.append(
                "completed_at_delta_seconds="
                f"{round(completed_delta_seconds, 3)} exceeds fuzz={max(fuzz_seconds, 0)}"
            )

    existing_clear_time_ms = existing_run.get("clear_time_ms")
    if existing_clear_time_ms is not None:
        if _should_update_key_run_metric(
            existing_run=existing_run,
            incoming_source=candidate.source,
            field_name="clear_time_ms",
            incoming_value=candidate.clear_time_ms,
        ):
            duration_delta_ms = abs(int(existing_clear_time_ms) - int(candidate.clear_time_ms))
            if duration_delta_ms > max(fuzz_seconds, 0) * 1000:
                warnings.append(
                    "clear_time_delta_ms="
                    f"{duration_delta_ms} exceeds fuzz={max(fuzz_seconds, 0) * 1000}"
                )

    existing_score = existing_run.get("score")
    if existing_score is not None and candidate.score is not None:
        if _should_update_key_run_metric(
            existing_run=existing_run,
            incoming_source=candidate.source,
            field_name="score",
            incoming_value=candidate.score,
        ):
            score_delta = abs(float(existing_score) - float(candidate.score))
            if score_delta > 1.0:
                warnings.append(
                    f"score_delta={round(score_delta, 3)} existing={float(existing_score)} "
                    f"candidate={float(candidate.score)}"
                )

    existing_dungeon_name = str(existing_run.get("dungeon", "") or "").strip()
    candidate_dungeon_name = str(candidate.dungeon or "").strip()
    if (
        existing_dungeon_name
        and candidate_dungeon_name
        and existing_dungeon_name.casefold() != candidate_dungeon_name.casefold()
    ):
        warnings.append(
            f"dungeon_name existing={existing_dungeon_name!r} candidate={candidate_dungeon_name!r}"
        )

    existing_short_name = str(existing_run.get("short_name", "") or "").strip()
    candidate_short_name = str(candidate.short_name or "").strip()
    if (
        existing_short_name
        and candidate_short_name
        and existing_short_name != candidate_short_name
        and candidate_short_name != candidate_dungeon_name
    ):
        warnings.append(
            f"short_name existing={existing_short_name!r} candidate={candidate_short_name!r}"
        )

    existing_within_time = existing_run.get("is_completed_within_time")
    if (
        existing_within_time is not None
        and candidate.is_completed_within_time is not None
        and _should_update_key_run_metric(
            existing_run=existing_run,
            incoming_source=candidate.source,
            field_name="is_completed_within_time",
            incoming_value=candidate.is_completed_within_time,
        )
        and bool(existing_within_time) != bool(candidate.is_completed_within_time)
    ):
        warnings.append(
            "is_completed_within_time "
            f"existing={bool(existing_within_time)} candidate={bool(candidate.is_completed_within_time)}"
        )

    existing_keystone_run_id = existing_run.get("keystone_run_id")
    if (
        existing_keystone_run_id is not None
        and candidate.keystone_run_id is not None
        and int(existing_keystone_run_id) != int(candidate.keystone_run_id)
    ):
        warnings.append(
            "keystone_run_id "
            f"existing={int(existing_keystone_run_id)} candidate={int(candidate.keystone_run_id)}"
        )

    return warnings


def _warn_on_surprising_run_change(
    existing_run: dict[str, Any],
    candidate: NormalizedRunCandidate,
    *,
    fuzz_seconds: int,
) -> None:
    """Log a strong warning when a matched update changes unexpected run fields."""

    differences = _summarize_run_differences(
        existing_run,
        candidate,
        fuzz_seconds=fuzz_seconds,
    )
    if not differences:
        return
    LOGGER.warning(
        "Matched run candidate differs from existing run",
        extra={
            "candidate_source": candidate.source,
            "existing_run_id": str(existing_run.get("_id", "")),
            "existing_keystone_run_id": existing_run.get("keystone_run_id"),
            "candidate_keystone_run_id": candidate.keystone_run_id,
            "differences": differences,
        },
    )


def _current_batch_start(now: datetime, *, interval_minutes: int) -> datetime:
    """Return the current UTC-aligned batch boundary for an interval."""

    normalized_now = ensure_utc(now)
    if interval_minutes <= 0:
        return normalized_now
    interval_seconds = interval_minutes * 60
    timestamp = int(normalized_now.timestamp())
    bucket_start_timestamp = timestamp - (timestamp % interval_seconds)
    return datetime.fromtimestamp(bucket_start_timestamp, tz=UTC)


def _next_batch_at_or_after(moment: datetime, *, interval_minutes: int) -> datetime:
    """Return the next UTC-aligned batch boundary at or after a timestamp."""

    normalized = ensure_utc(moment)
    batch_start = _current_batch_start(normalized, interval_minutes=interval_minutes)
    if batch_start == normalized:
        return batch_start
    return batch_start + timedelta(minutes=interval_minutes)


class MongoRepository:
    """Mongo-backed store for players, runs, and sync metadata."""

    def __init__(self, settings: MongoSettings) -> None:
        from pymongo import ASCENDING, MongoClient

        self._client = MongoClient(settings.uri)
        self._db = self._client[settings.database]
        self.players = self._db[settings.players_collection]
        self.runs = self._db[settings.runs_collection]
        self.sync_cycles = self._db[settings.sync_cycles_collection]
        self.season_dungeons = self._db["season_dungeons"]
        self.weekly_periods = self._db["weekly_periods"]
        self.players.create_index([("player_key", ASCENDING)], unique=True)
        self._ensure_sparse_keystone_run_id_index()
        self.runs.create_index([("completed_at", ASCENDING)])
        self.runs.create_index([("participants.player_key", ASCENDING)])
        self.runs.create_index([("discovered_from_player_keys", ASCENDING)])
        self.players.create_index([("hot_ready_at", ASCENDING)])
        self.players.create_index([("hot_until_at", ASCENDING)])
        self.season_dungeons.create_index([("season", ASCENDING), ("slug", ASCENDING)], unique=True)
        self.season_dungeons.create_index([("season", ASCENDING), ("short_name", ASCENDING)])
        self.weekly_periods.create_index([("region", ASCENDING)], unique=True)

    def _ensure_sparse_keystone_run_id_index(self) -> None:
        """Replace the legacy keystone_run_id index with the sparse version when needed."""

        index_name = "keystone_run_id_1"
        index_info = self.runs.index_information().get(index_name)
        if index_info:
            key = index_info.get("key")
            if key == [("keystone_run_id", 1)] and not index_info.get("sparse", False):
                self.runs.drop_index(index_name)
        self.runs.create_index(
            [("keystone_run_id", ASCENDING)],
            unique=True,
            sparse=True,
        )

    def sync_roster(self, entries: list[RosterEntry], *, seen_at: datetime) -> None:
        """Upsert current roster entries and deactivate anything no longer listed."""

        active_keys = [entry.player_key for entry in entries]
        self.players.update_many({}, {"$set": {"is_active": False}})
        for entry in entries:
            base_doc: dict[str, Any] = {
                "player_key": entry.player_key,
                "sheet_row_number": entry.row_number,
                "sheet_value": entry.raw_value,
                "is_active": True,
                "is_valid": entry.is_valid,
                "status": entry.status.value,
                "status_message": entry.status_message,
                "last_seen_in_sheet_at": seen_at,
            }
            if entry.identity:
                base_doc.update(
                    {
                        "region": entry.identity.region,
                        "realm": entry.identity.realm,
                        "name": entry.identity.name,
                    }
                )
            else:
                base_doc.update({"region": "", "realm": "", "name": entry.raw_value.strip()})
            self.players.update_one(
                {"player_key": entry.player_key},
                {
                    "$set": base_doc,
                    "$setOnInsert": {
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
                        "requires_backfill": entry.is_valid,
                        "score_source": "",
                        "score_source_fetched_at": None,
                        "blizzard_last_successful_sync_at": None,
                        "created_at": seen_at,
                    },
                },
                upsert=True,
            )
        if active_keys:
            self.players.update_many(
                {"player_key": {"$nin": active_keys}},
                {"$set": {"is_active": False}},
            )

    def list_active_players(self, *, limit: int) -> list[dict[str, Any]]:
        """Return active roster documents."""

        return list(self.players.find({"is_active": True}).sort("player_key").limit(limit))

    def list_all_active_players(self) -> list[dict[str, Any]]:
        """Return all active roster documents for manual backfill operations."""

        return list(self.players.find({"is_active": True}).sort("player_key"))

    def list_players_due_for_base_sync(
        self,
        *,
        now: datetime,
        interval_minutes: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return valid active players due for the base sync cadence."""

        cutoff = ensure_utc(now)
        due_players: list[dict[str, Any]] = []
        for player in self.list_active_players(limit=limit):
            if not player.get("is_valid", False):
                continue
            last_attempt = _safe_utc_datetime(
                player.get("last_base_sync_started_at") or player.get("last_sync_started_at")
            )
            if last_attempt is None:
                due_players.append(player)
                continue
            next_due_at = _current_batch_start(
                last_attempt,
                interval_minutes=interval_minutes,
            ) + timedelta(minutes=interval_minutes)
            if next_due_at <= cutoff:
                due_players.append(player)
        return due_players[:limit]

    def list_players_due_for_hot_sync(
        self,
        *,
        now: datetime,
        interval_minutes: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return valid active players due for hot polling."""

        cutoff = ensure_utc(now)
        due_players: list[dict[str, Any]] = []
        for player in self.list_active_players(limit=limit):
            if not player.get("is_valid", False):
                continue
            hot_ready_at = _safe_utc_datetime(player.get("hot_ready_at"))
            hot_until_at = _safe_utc_datetime(player.get("hot_until_at"))
            if hot_ready_at is None or hot_until_at is None:
                continue
            if not hot_ready_at <= cutoff < hot_until_at:
                continue
            last_attempt = _safe_utc_datetime(player.get("last_sync_started_at"))
            if last_attempt is None:
                due_players.append(player)
                continue
            next_hot_ready_batch = _next_batch_at_or_after(
                hot_ready_at,
                interval_minutes=interval_minutes,
            )
            next_due_at = max(
                next_hot_ready_batch,
                _current_batch_start(
                    last_attempt,
                    interval_minutes=interval_minutes,
                )
                + timedelta(minutes=interval_minutes),
            )
            if next_due_at <= cutoff:
                due_players.append(player)
        return due_players[:limit]

    def mark_sync_started(
        self,
        player_key: str,
        started_at: datetime,
        *,
        sync_kind: str,
    ) -> None:
        """Record sync start time."""

        fields_to_set: dict[str, Any] = {"last_sync_started_at": started_at}
        if sync_kind == "base":
            fields_to_set["last_base_sync_started_at"] = started_at
        self.players.update_one(
            {"player_key": player_key},
            {"$set": fields_to_set},
        )

    def mark_invalid_player(self, player_key: str, message: str, *, when: datetime) -> None:
        """Persist an invalid player resolution failure."""

        self.players.update_one(
            {"player_key": player_key},
            {
                "$set": {
                    "status": PlayerDataStatus.INVALID_PLAYER.value,
                    "status_message": message,
                    "last_error": message,
                    "last_sync_completed_at": when,
                }
            },
        )

    def mark_sync_error(self, player_key: str, message: str, *, when: datetime) -> None:
        """Persist a sync error."""

        self.players.update_one(
            {"player_key": player_key},
            {
                "$set": {
                    "status": PlayerDataStatus.SYNC_ERROR.value,
                    "status_message": message,
                    "last_error": message,
                    "last_sync_completed_at": when,
                }
            },
        )

    def clear_player_hot_window(self, *, player_key: str) -> None:
        """Clear expired hot-polling timestamps for a player."""

        self.players.update_one(
            {"player_key": player_key},
            {
                "$set": {
                    "hot_ready_at": None,
                    "hot_until_at": None,
                }
            },
        )

    def upsert_player_play_profile(
        self,
        *,
        player_key: str,
        profile: dict[str, Any],
    ) -> None:
        """Persist predictive play-profile state for a player."""

        self.players.update_one(
            {"player_key": player_key},
            {"$set": profile},
        )

    def mark_predictive_hot_enqueue(
        self,
        *,
        player_key: str,
        week_hour_key: str,
        hot_ready_at: datetime,
        hot_until_at: datetime,
    ) -> None:
        """Record a predictive hot enqueue while preserving stronger existing coverage."""

        player = self.players.find_one(
            {"player_key": player_key},
            {"hot_ready_at": 1, "hot_until_at": 1},
        ) or {}
        existing_hot_ready = _safe_utc_datetime(player.get("hot_ready_at"))
        existing_hot_until = _safe_utc_datetime(player.get("hot_until_at"))
        resolved_hot_ready = hot_ready_at
        if existing_hot_ready is not None:
            resolved_hot_ready = min(existing_hot_ready, ensure_utc(hot_ready_at))
        resolved_hot_until = hot_until_at
        if existing_hot_until is not None:
            resolved_hot_until = max(existing_hot_until, ensure_utc(hot_until_at))

        self.players.update_one(
            {"player_key": player_key},
            {
                "$set": {
                    "hot_ready_at": resolved_hot_ready,
                    "hot_until_at": resolved_hot_until,
                    "play_profile_last_enqueued_week_hour": week_hour_key,
                }
            },
        )

    def update_player_profile(
        self,
        player_key: str,
        *,
        current_dungeon_scores: dict[str, float],
        current_total_score: float | None,
        score_source: str,
        synced_at: datetime,
    ) -> None:
        """Persist successful profile sync metadata."""

        self.players.update_one(
            {"player_key": player_key},
            {
                "$set": {
                    "status": PlayerDataStatus.OK.value,
                    "status_message": "",
                    "last_error": "",
                    "current_dungeon_scores": current_dungeon_scores,
                    "current_total_score": current_total_score,
                    "score_source": score_source,
                    "score_source_fetched_at": synced_at,
                    "blizzard_last_successful_sync_at": (
                        synced_at if score_source == "blizzard" else None
                    ),
                    "last_sync_completed_at": synced_at,
                    "last_successful_sync_at": synced_at,
                    "requires_backfill": False,
                }
            },
        )

    def get_known_run_ids(self, run_ids: list[int]) -> set[int]:
        """Return the run IDs already stored."""

        if not run_ids:
            return set()
        return {
            doc["keystone_run_id"]
            for doc in self.runs.find(
                {"keystone_run_id": {"$in": run_ids}},
                {"keystone_run_id": 1},
            )
        }

    def attach_player_to_run(self, run_id: int, player_key: str) -> None:
        """Ensure a player's key is attached to a known run."""

        self.runs.update_one(
            {"keystone_run_id": run_id},
            {"$addToSet": {"discovered_from_player_keys": player_key}},
        )

    def upsert_normalized_run(
        self,
        candidate: NormalizedRunCandidate,
        *,
        player_key: str,
        season: str,
        synced_at: datetime,
        fuzz_seconds: int,
    ) -> bool:
        """Store or enrich a unified run document."""

        participants = []
        for participant in candidate.participants:
            normalized = dict(participant)
            player_value = normalized.get("player_key")
            if isinstance(player_value, str):
                normalized["player_key"] = player_value
            participants.append(normalized)

        query: dict[str, Any] | None = None
        existing = None
        if candidate.keystone_run_id is not None:
            existing = self.runs.find_one(
                {"keystone_run_id": int(candidate.keystone_run_id)},
                {
                    "_id": 1,
                    "keystone_run_id": 1,
                    "map_challenge_mode_id": 1,
                    "dungeon_id": 1,
                    "zone_id": 1,
                    "dungeon": 1,
                    "short_name": 1,
                    "mythic_level": 1,
                    "completed_at": 1,
                    "clear_time_ms": 1,
                    "par_time_ms": 1,
                    "score": 1,
                    "run_metrics_source": 1,
                    "num_keystone_upgrades": 1,
                    "is_completed_within_time": 1,
                    "sources": 1,
                },
            )
            if existing:
                query = {"_id": existing["_id"]}
        if existing is None:
            existing = self.find_run_by_fuzzy_fields(
                dungeon_id=candidate.dungeon_id,
                mythic_level=candidate.mythic_level,
                completed_at=candidate.completed_at,
                clear_time_ms=candidate.clear_time_ms,
                fuzz_seconds=fuzz_seconds,
            )
            if existing is not None:
                query = {"_id": existing["_id"]}
        if query is None:
            query = {
                "map_challenge_mode_id": candidate.dungeon_id,
                "mythic_level": candidate.mythic_level,
                "completed_at": candidate.completed_at,
                "clear_time_ms": candidate.clear_time_ms,
                "keystone_run_id": {"$exists": False},
            }

        if existing is not None:
            _warn_on_surprising_run_change(
                existing,
                candidate,
                fuzz_seconds=fuzz_seconds,
            )

        short_name = _candidate_short_name_for_update(
            candidate,
            str(existing.get("short_name", "") if existing else ""),
        )
        update_doc = {
            "season": season,
            "dungeon": candidate.dungeon,
            "mythic_level": candidate.mythic_level,
            "completed_at": candidate.completed_at,
            "last_seen_at": synced_at,
        }
        update_doc.update(
            _build_key_run_metric_updates(
                existing_run=existing,
                incoming_source=candidate.source,
                metric_values={
                    "score": candidate.score,
                    "clear_time_ms": candidate.clear_time_ms,
                    "num_keystone_upgrades": candidate.num_keystone_upgrades,
                    "is_completed_within_time": candidate.is_completed_within_time,
                },
            )
        )
        if participants:
            update_doc["participants"] = participants
        if short_name is not None:
            update_doc["short_name"] = short_name
        if candidate.dungeon_id is not None:
            update_doc["dungeon_id"] = candidate.dungeon_id
            update_doc["map_challenge_mode_id"] = candidate.dungeon_id
        if candidate.keystone_run_id is not None:
            update_doc["keystone_run_id"] = int(candidate.keystone_run_id)
        if candidate.source == "blizzard":
            update_doc["blizzard_payload"] = candidate.raw_payload
        else:
            update_doc["raiderio_payload"] = candidate.raw_payload

        self.runs.update_one(
            query,
            {
                "$set": update_doc,
                "$addToSet": {
                    "discovered_from_player_keys": player_key,
                    "sources": candidate.source,
                },
                "$setOnInsert": {"created_at": synced_at},
            },
            upsert=True,
        )
        return existing is None

    def find_run_by_fuzzy_fields(
        self,
        *,
        dungeon_id: int | None,
        mythic_level: int | None,
        completed_at: datetime | None,
        clear_time_ms: int | None,
        fuzz_seconds: int,
    ) -> dict[str, Any] | None:
        """Find a single existing run doc that matches within the configured fuzz window."""

        if (
            dungeon_id is None
            or mythic_level is None
            or completed_at is None
            or clear_time_ms is None
        ):
            return None
        time_delta = timedelta(seconds=max(fuzz_seconds, 0))
        duration_delta = max(fuzz_seconds, 0) * 1000
        matches = list(
            self.runs.find(
                {
                    "map_challenge_mode_id": int(dungeon_id),
                    "mythic_level": int(mythic_level),
                    "completed_at": {
                        "$gte": ensure_utc(completed_at) - time_delta,
                        "$lte": ensure_utc(completed_at) + time_delta,
                    },
                    "clear_time_ms": {
                        "$gte": int(clear_time_ms) - duration_delta,
                        "$lte": int(clear_time_ms) + duration_delta,
                    },
                },
                {
                    "_id": 1,
                    "keystone_run_id": 1,
                    "map_challenge_mode_id": 1,
                    "dungeon_id": 1,
                    "zone_id": 1,
                    "dungeon": 1,
                    "short_name": 1,
                    "mythic_level": 1,
                    "completed_at": 1,
                    "clear_time_ms": 1,
                    "par_time_ms": 1,
                    "score": 1,
                    "run_metrics_source": 1,
                    "num_keystone_upgrades": 1,
                    "is_completed_within_time": 1,
                    "sources": 1,
                },
            )
        )
        if not matches:
            return None
        if len(matches) > 1:
            raise RuntimeError(
                "Multiple runs matched fuzzy run identity lookup for "
                f"dungeon_id={dungeon_id}, mythic_level={mythic_level}, "
                f"completed_at={completed_at.isoformat()}, clear_time_ms={clear_time_ms}"
            )
        return matches[0]

    def update_run_details(
        self,
        *,
        run_id: int,
        payload: dict[str, Any],
        player_key: str,
        synced_at: datetime,
    ) -> None:
        """Merge detailed run payload into MongoDB."""

        participants = []
        for raw_participant in payload.get("roster", []):
            character = raw_participant.get("character", raw_participant)
            region = (
                (character.get("region") or {}).get("slug")
                or character.get("region")
                or ""
            )
            realm = ((character.get("realm") or {}).get("slug")) or character.get("realm") or ""
            name = character.get("name", "")
            participant_key = ""
            if region and realm and name:
                participant_key = f"{str(region).lower()}/{str(realm).lower()}/{str(name).lower()}"
            participants.append(
                {
                    "player_key": participant_key,
                    "region": str(region).lower(),
                    "realm": str(realm).lower(),
                    "name": name,
                    "role": raw_participant.get("role") or character.get("role"),
                    "class": ((character.get("class") or {}).get("name")),
                    "spec": ((character.get("spec") or {}).get("name")),
                    "raw": raw_participant,
                }
            )

        dungeon = payload.get("dungeon") or payload.get("run", {}).get("dungeon") or {}
        season = payload.get("season") or payload.get("run", {}).get("season") or ""
        completed_at = _safe_isoformat(
            payload.get("completed_at") or payload.get("run", {}).get("completed_at")
        )
        clear_time_ms = payload.get("clear_time_ms")
        mythic_level = payload.get("mythic_level")
        dungeon_id = dungeon.get("map_challenge_mode_id") or dungeon.get("id")
        if completed_at is None or clear_time_ms is None or mythic_level is None or dungeon_id is None:
            raise ValueError(
                f"Raider.IO run details for {run_id} missing required UID fields: "
                f"map_challenge_mode_id/id={dungeon_id}, mythic_level={mythic_level}, "
                f"completed_at={completed_at}, clear_time_ms={clear_time_ms}"
            )

        existing = self.runs.find_one(
            {"keystone_run_id": run_id},
            {
                "_id": 1,
                "score": 1,
                "clear_time_ms": 1,
                "par_time_ms": 1,
                "num_keystone_upgrades": 1,
                "is_completed_within_time": 1,
                "run_metrics_source": 1,
                "sources": 1,
            },
        )
        update_doc = {
            "keystone_run_id": run_id,
            "season": season,
            "dungeon": dungeon.get("name", ""),
            "short_name": dungeon.get("short_name", ""),
            "mythic_level": mythic_level,
            "map_challenge_mode_id": dungeon.get("map_challenge_mode_id") or dungeon_id,
            "zone_id": dungeon.get("id"),
            "zone_expansion_id": dungeon.get("expansion_id"),
            "icon_url": dungeon.get("icon_url"),
            "detail_loaded": True,
            "detail_payload": payload,
            "participants": participants,
            "last_seen_at": synced_at,
            "completed_at": completed_at,
        }
        update_doc.update(
            _build_key_run_metric_updates(
                existing_run=existing,
                incoming_source="raiderio",
                metric_values={
                    "score": payload.get("score"),
                    "clear_time_ms": clear_time_ms,
                    "par_time_ms": payload.get("keystone_time_ms"),
                    "num_keystone_upgrades": payload.get("num_chests"),
                    "is_completed_within_time": payload.get("is_completed_within_time"),
                },
            )
        )

        self.runs.update_one(
            {"keystone_run_id": run_id},
            {
                "$set": update_doc,
                "$addToSet": {
                    "discovered_from_player_keys": player_key,
                    "sources": "raiderio",
                },
                "$setOnInsert": {"created_at": synced_at},
            },
            upsert=True,
        )

    def get_runs_for_players(self, player_keys: list[str]) -> list[dict[str, Any]]:
        """Fetch runs relevant to a set of players."""

        if not player_keys:
            return []
        return list(
            self.runs.find(
                {
                    "$or": [
                        {"participants.player_key": {"$in": player_keys}},
                        {"discovered_from_player_keys": {"$in": player_keys}},
                    ]
                }
            )
        )

    def get_runs_for_player(self, *, player_key: str, season: str) -> list[dict[str, Any]]:
        """Fetch current-season runs relevant to one player."""

        return list(
            self.runs.find(
                {
                    "season": season,
                    "$or": [
                        {"participants.player_key": player_key},
                        {"discovered_from_player_keys": player_key},
                    ],
                }
            )
        )

    def replace_season_dungeons(
        self, *, season: str, dungeons: list[SeasonDungeon], synced_at: datetime
    ) -> None:
        """Replace the dungeon catalog for a season."""

        self.season_dungeons.delete_many({"season": season})
        if not dungeons:
            return
        self.season_dungeons.insert_many(
            [
                {
                    "season": season,
                    "dungeon_id": dungeon.dungeon_id,
                    "slug": dungeon.slug,
                    "name": dungeon.name,
                    "short_name": dungeon.short_name,
                    "challenge_mode_id": dungeon.challenge_mode_id,
                    "keystone_timer_seconds": dungeon.keystone_timer_seconds,
                    "icon_url": dungeon.icon_url,
                    "background_image_url": dungeon.background_image_url,
                    "last_synced_at": synced_at,
                }
                for dungeon in dungeons
            ],
            ordered=True,
        )

    def list_season_dungeons(self, *, season: str) -> list[dict[str, Any]]:
        """Return season dungeon metadata in stable short-name order."""

        return list(self.season_dungeons.find({"season": season}).sort("short_name"))

    def normalize_run_short_names(
        self,
        *,
        season: str,
        dungeons: list[dict[str, Any]],
    ) -> None:
        """Align stored run short names with the current season dungeon catalog."""

        for dungeon in dungeons:
            short_name = str(dungeon.get("short_name", "") or "")
            if not short_name:
                continue
            dungeon_id = dungeon.get("dungeon_id")
            challenge_mode_id = dungeon.get("challenge_mode_id")
            filters: list[dict[str, Any]] = []
            if dungeon_id is not None:
                filters.append({"dungeon_id": int(dungeon_id)})
                filters.append({"zone_id": int(dungeon_id)})
            if challenge_mode_id is not None:
                filters.append({"map_challenge_mode_id": int(challenge_mode_id)})
            if not filters:
                continue
            self.runs.update_many(
                {
                    "season": season,
                    "$or": filters,
                },
                {"$set": {"short_name": short_name}},
            )

    def get_current_weekly_periods(
        self,
        *,
        now: datetime,
        regions: set[str],
    ) -> dict[str, dict[str, Any]]:
        """Return cached weekly periods that still cover the requested time."""

        if not regions:
            return {}
        current_time = ensure_utc(now)
        documents = self.weekly_periods.find({"region": {"$in": sorted(regions)}})
        periods_by_region: dict[str, dict[str, Any]] = {}
        for document in documents:
            region = str(document.get("region", "")).lower()
            start = _safe_utc_datetime(document.get("start"))
            end = _safe_utc_datetime(document.get("end"))
            period = document.get("period")
            if not region or start is None or end is None or period is None:
                continue
            if not start <= current_time < end:
                continue
            periods_by_region[region] = {
                "period": int(period),
                "start": start,
                "end": end,
            }
        return periods_by_region

    def replace_weekly_periods(
        self,
        *,
        periods_by_region: dict[str, dict[str, Any]],
        synced_at: datetime,
    ) -> None:
        """Persist the latest weekly periods by region."""

        if not periods_by_region:
            return
        for region, period in periods_by_region.items():
            self.weekly_periods.update_one(
                {"region": region},
                {
                    "$set": {
                        "region": region,
                        "period": period["period"],
                        "start": ensure_utc(period["start"]),
                        "end": ensure_utc(period["end"]),
                        "last_synced_at": ensure_utc(synced_at),
                    }
                },
                upsert=True,
            )

    def get_player_character_id(self, *, player_key: str) -> int | None:
        """Return a cached Raider.IO website character ID when available."""

        document = self.players.find_one(
            {"player_key": player_key},
            {"raiderio_character_id": 1},
        )
        if not document:
            return None
        value = document.get("raiderio_character_id")
        return int(value) if value is not None else None

    def cache_player_character_id(
        self,
        *,
        player_key: str,
        identity: PlayerIdentity,
        character_id: int,
        resolved_at: datetime,
    ) -> None:
        """Persist a Raider.IO website character ID for later backfills."""

        self.players.update_one(
            {"player_key": player_key},
            {
                "$set": {
                    "region": identity.region,
                    "realm": identity.realm,
                    "name": identity.name,
                    "raiderio_character_id": int(character_id),
                    "raiderio_character_id_resolved_at": resolved_at,
                }
            },
            upsert=True,
        )

    def store_sync_cycle(self, document: dict[str, Any]) -> None:
        """Record a sync cycle."""

        self.sync_cycles.insert_one(document)

    def close(self) -> None:
        """Close the MongoDB connection."""

        self._client.close()
