"""MongoDB persistence layer."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from niru.config import MongoSettings
from niru.models import PlayerDataStatus, RosterEntry, SeasonDungeon, ensure_utc


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
        self.runs.create_index([("keystone_run_id", ASCENDING)], unique=True)
        self.runs.create_index([("completed_at", ASCENDING)])
        self.runs.create_index([("participants.player_key", ASCENDING)])
        self.runs.create_index([("discovered_from_player_keys", ASCENDING)])
        self.players.create_index([("hot_ready_at", ASCENDING)])
        self.players.create_index([("hot_until_at", ASCENDING)])
        self.season_dungeons.create_index([("season", ASCENDING), ("slug", ASCENDING)], unique=True)
        self.season_dungeons.create_index([("season", ASCENDING), ("short_name", ASCENDING)])
        self.weekly_periods.create_index([("region", ASCENDING)], unique=True)

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

    def upsert_run_stub(
        self,
        run: dict[str, Any],
        *,
        player_key: str,
        season: str,
        synced_at: datetime,
    ) -> None:
        """Store a run stub discovered from a character profile."""

        run_id = int(run["keystone_run_id"])
        stub = {
            "keystone_run_id": run_id,
            "season": season,
            "dungeon": run.get("dungeon", ""),
            "short_name": run.get("short_name", ""),
            "mythic_level": run.get("mythic_level"),
            "completed_at": _safe_isoformat(run.get("completed_at")),
            "clear_time_ms": run.get("clear_time_ms"),
            "par_time_ms": run.get("par_time_ms"),
            "num_keystone_upgrades": run.get("num_keystone_upgrades"),
            "map_challenge_mode_id": run.get("map_challenge_mode_id"),
            "zone_id": run.get("zone_id"),
            "zone_expansion_id": run.get("zone_expansion_id"),
            "icon_url": run.get("icon_url"),
            "background_image_url": run.get("background_image_url"),
            "score": run.get("score"),
            "url": run.get("url"),
            "affixes": run.get("affixes", []),
            "spec": run.get("spec"),
            "role": run.get("role"),
            "detail_loaded": False,
            "last_seen_at": synced_at,
        }
        self.runs.update_one(
            {"keystone_run_id": run_id},
            {
                "$set": stub,
                "$addToSet": {"discovered_from_player_keys": player_key},
                "$setOnInsert": {"created_at": synced_at, "participants": []},
            },
            upsert=True,
        )

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

        self.runs.update_one(
            {"keystone_run_id": run_id},
            {
                "$set": {
                    "keystone_run_id": run_id,
                    "season": season,
                    "dungeon": dungeon.get("name", ""),
                    "short_name": dungeon.get("short_name", ""),
                    "mythic_level": payload.get("mythic_level"),
                    "score": payload.get("score"),
                    "clear_time_ms": payload.get("clear_time_ms"),
                    "par_time_ms": payload.get("keystone_time_ms"),
                    "num_keystone_upgrades": payload.get("num_chests"),
                    "map_challenge_mode_id": dungeon.get("map_challenge_mode_id"),
                    "zone_id": dungeon.get("id"),
                    "zone_expansion_id": dungeon.get("expansion_id"),
                    "icon_url": dungeon.get("icon_url"),
                    "detail_loaded": True,
                    "detail_payload": payload,
                    "participants": participants,
                    "last_seen_at": synced_at,
                    "completed_at": _safe_isoformat(
                        payload.get("completed_at")
                        or payload.get("run", {}).get("completed_at")
                    ),
                },
                "$addToSet": {"discovered_from_player_keys": player_key},
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
