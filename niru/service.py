"""Core sync service."""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
import logging
import random
import signal
import threading
import time
from typing import Any

from niru.clients.raiderio import RaiderIOClient, RaiderIOError, RaiderIONotFoundError
from niru.clients.sheets import GoogleSheetsClient
from niru.config import Settings
from niru.models import (
    PACIFIC_TZ,
    PlayerDataStatus,
    PlayerIdentity,
    SeasonDungeon,
    SummaryRow,
    SyncStats,
    ensure_utc,
    to_pacific_datetime,
    utc_now,
)
from niru.play_profile import (
    PLAY_PROFILE_HOURS_PER_WEEK,
    build_play_profile,
    current_week_hour_key,
    expected_weeks_observed,
    next_pacific_hour_start,
    pacific_hour_start,
    pacific_week_hour_index,
    update_play_profile,
)
from niru.roster import parse_roster_rows
from niru.storage import MongoRepository

LOGGER = logging.getLogger(__name__)
PACIFIC_DAY_START_HOUR = 0

PLAYER_COLUMNS = [
    "region",
    "realm",
    "name",
    "current_total_mythic_plus_rating",
    "last_successful_sync_time_pacific",
    "weekly_10_plus_run_count",
]

DUNGEON_FIELDS = [
    "current_score",
    "best_key_level",
    "best_upgrade_level",
    "total_runs",
]


def _safe_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return ensure_utc(value)
    if isinstance(value, str) and value:
        return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    return None


def _normalize_weekly_periods(periods_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    periods_by_region: dict[str, dict[str, Any]] = {}
    for period_entry in periods_payload.get("periods", []) or []:
        region = str(period_entry.get("region", "")).lower()
        current = period_entry.get("current") or {}
        start = _safe_datetime(current.get("start"))
        end = _safe_datetime(current.get("end"))
        period_id = current.get("period")
        if not region or start is None or end is None or period_id is None:
            continue
        periods_by_region[region] = {
            "period": int(period_id),
            "start": start,
            "end": end,
        }
    return periods_by_region


def _weekly_periods_for_metadata(
    periods_by_region: dict[str, dict[str, Any]],
) -> dict[str, dict[str, object]]:
    return {
        region: {
            "period": period["period"],
            "start": period["start"].isoformat(),
            "end": period["end"].isoformat(),
        }
        for region, period in periods_by_region.items()
    }


def _build_dungeon_scores(profile_payload: dict[str, Any]) -> dict[str, float]:
    best_by_dungeon: dict[str, dict[int, float]] = defaultdict(dict)
    for field in ("mythic_plus_best_runs", "mythic_plus_alternate_runs"):
        for run in profile_payload.get(field, []) or []:
            dungeon = run.get("dungeon")
            run_id = run.get("keystone_run_id")
            score = run.get("score")
            if dungeon and run_id and score is not None:
                best_by_dungeon[dungeon][int(run_id)] = float(score)
    return {dungeon: round(sum(scores.values()), 1) for dungeon, scores in best_by_dungeon.items()}


def _lag_minutes_for_run(run: dict[str, Any]) -> float | None:
    created_at = _safe_datetime(run.get("created_at"))
    completed_at = _safe_datetime(run.get("completed_at"))
    if created_at is None or completed_at is None:
        return None
    lag_seconds = max((created_at - completed_at).total_seconds(), 0.0)
    return round(lag_seconds / 60.0, 1)


def _pacific_day_bounds(now: datetime) -> tuple[datetime, datetime]:
    pacific_now = ensure_utc(now).astimezone(PACIFIC_TZ)
    day_start_pacific = pacific_now.replace(
        hour=PACIFIC_DAY_START_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )
    return day_start_pacific.astimezone(UTC), pacific_now.astimezone(UTC)


def _unique_runs_by_id(runs: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    unique_runs: dict[int, dict[str, Any]] = {}
    for run in runs:
        run_id = run.get("keystone_run_id")
        if run_id is None:
            continue
        unique_runs[int(run_id)] = run
    return unique_runs


def _build_total_score(profile_payload: dict[str, Any], *, season: str) -> float | None:
    seasons = profile_payload.get("mythic_plus_scores_by_season", []) or []
    for season_entry in seasons:
        if season_entry.get("season") != season:
            continue
        scores = season_entry.get("scores") or {}
        all_score = scores.get("all")
        if all_score is not None:
            return float(all_score)
    return None


def _display_total_score(player: dict[str, Any], players: list[dict[str, Any]]) -> float | None:
    score = player.get("current_total_score")
    if score is None:
        return None

    numeric_score = float(score)
    player_name = str(player.get("name", ""))
    if player_name.casefold() != "nyph":
        return round(numeric_score, 1)

    has_gr_tie = any(
        other is not player
        and other.get("current_total_score") is not None
        and float(other["current_total_score"]) == numeric_score
        and str(other.get("name", "")).casefold().startswith("gr")
        for other in players
    )
    if has_gr_tie:
        numeric_score += 0.1
    return round(numeric_score, 1)


def _collect_profile_run_candidates(profile_payload: dict[str, Any]) -> dict[int, dict[str, Any]]:
    candidates: dict[int, dict[str, Any]] = {}
    for field in (
        "mythic_plus_recent_runs",
        "mythic_plus_best_runs",
        "mythic_plus_alternate_runs",
    ):
        for run in profile_payload.get(field, []) or []:
            run_id = run.get("keystone_run_id")
            if run_id is None:
                continue
            candidates[int(run_id)] = run
    return candidates


def _last_attempted_sync_at(player: dict[str, Any]) -> datetime | None:
    return _safe_datetime(
        player.get("last_sync_started_at") or player.get("last_sync_completed_at")
    )


def _current_hot_batch_start(now: datetime, *, interval_minutes: int) -> datetime:
    """Return the current batch boundary for the configured interval."""

    normalized_now = ensure_utc(now)
    if interval_minutes <= 0:
        return normalized_now
    interval_seconds = interval_minutes * 60
    timestamp = int(normalized_now.timestamp())
    bucket_start_timestamp = timestamp - (timestamp % interval_seconds)
    return datetime.fromtimestamp(bucket_start_timestamp, tz=UTC)


def _next_hot_batch_at_or_after(moment: datetime, *, interval_minutes: int) -> datetime:
    """Return the next batch boundary at or after a timestamp."""

    normalized = ensure_utc(moment)
    batch_start = _current_hot_batch_start(normalized, interval_minutes=interval_minutes)
    if batch_start == normalized:
        return batch_start
    return batch_start + timedelta(minutes=interval_minutes)


def build_summary_header(dungeons: list[dict[str, Any]]) -> list[str]:
    """Build the sheet header for one-row-per-player output."""

    header = list(PLAYER_COLUMNS)
    for dungeon in dungeons:
        short_name = dungeon.get("short_name", "")
        for field in DUNGEON_FIELDS:
            header.append(f"{short_name}_{field}")
    return header


def _season_slug_to_expansion_id(season: str) -> int:
    prefix = season.removeprefix("season-").split("-", maxsplit=1)[0]
    mapping = {
        "mn": 11,
        "tww": 10,
        "df": 9,
        "sl": 8,
        "bfa": 7,
        "legion": 6,
    }
    if prefix not in mapping:
        raise ValueError(f"Unsupported season slug for expansion lookup: {season}")
    return mapping[prefix]


def build_summary_rows(
    players: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    season_dungeons: list[dict[str, Any]],
    *,
    weekly_periods: dict[str, dict[str, Any]] | None = None,
) -> list[SummaryRow]:
    """Build Google Sheets summary rows from Mongo state."""

    weekly_periods = weekly_periods or {}
    grouped_runs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        player_keys = set(run.get("discovered_from_player_keys", []))
        for participant in run.get("participants", []):
            player_key = participant.get("player_key")
            if player_key:
                player_keys.add(player_key)
        for player_key in player_keys:
            grouped_runs[player_key].append(run)

    rows: list[SummaryRow] = []
    players_in_roster_order = sorted(
        enumerate(players),
        key=lambda item: (
            item[1].get("sheet_row_number") is None,
            item[1].get("sheet_row_number", 0),
            item[0],
        ),
    )
    for _, player in players_in_roster_order:
        runs_for_player = grouped_runs.get(player["player_key"], [])
        unique_runs_for_player = {
            int(run_id): run
            for run in runs_for_player
            if (run_id := run.get("keystone_run_id")) is not None
        }
        by_dungeon: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for run in unique_runs_for_player.values():
            dungeon_key = run.get("short_name") or run.get("dungeon") or ""
            if dungeon_key:
                by_dungeon[dungeon_key].append(run)

        weekly_10_plus_run_count: int | None = None
        region = str(player.get("region", "")).lower()
        weekly_period = weekly_periods.get(region)
        if weekly_period:
            weekly_start = weekly_period["start"]
            weekly_end = weekly_period["end"]
            weekly_10_plus_run_count = sum(
                1
                for run in unique_runs_for_player.values()
                if (completed_at := _safe_datetime(run.get("completed_at"))) is not None
                and weekly_start <= completed_at < weekly_end
                and int(run.get("mythic_level") or 0) >= 10
            )

        current_scores: dict[str, float] = player.get("current_dungeon_scores", {})
        values: list[object] = [
            player.get("region", ""),
            player.get("realm", ""),
            player.get("name", ""),
            _display_total_score(player, players),
            to_pacific_datetime(player.get("last_successful_sync_at")),
            weekly_10_plus_run_count,
        ]
        for dungeon in season_dungeons:
            short_name = dungeon.get("short_name", "")
            dungeon_runs = by_dungeon.get(short_name, [])
            current_score = current_scores.get(dungeon.get("name")) or current_scores.get(short_name)
            if dungeon_runs:
                best_run = max(
                    dungeon_runs,
                    key=lambda run: (
                        run.get("score") or 0,
                        run.get("mythic_level") or 0,
                        run.get("completed_at") or 0,
                    ),
                )
                values.extend(
                    [
                        None if current_score is None else round(float(current_score), 1),
                        best_run.get("mythic_level"),
                        best_run.get("num_keystone_upgrades"),
                        len(dungeon_runs),
                    ]
                )
                continue
            values.extend(
                [
                    None if current_score is None else round(float(current_score), 1),
                    None,
                    None,
                    0 if player.get("is_valid", False) else None,
                ]
            )
        rows.append(SummaryRow(values=values))

    return rows


def build_summary_metadata_rows(
    *,
    header: list[str],
    runs: list[dict[str, Any]],
    now: datetime,
) -> list[tuple[object, object]]:
    """Build top-right metadata rows for the summary sheet."""

    if "last_successful_sync_time_pacific" not in header:
        return []

    unique_runs = _unique_runs_by_id(runs)
    metadata_rows: list[tuple[object, object]] = [("unique_runs", len(unique_runs))]

    lag_by_run_id: dict[int, tuple[datetime, float]] = {}
    today_lags: list[float] = []
    today_start, today_end = _pacific_day_bounds(now)
    for run_id, run in unique_runs.items():
        created_at = _safe_datetime(run.get("created_at"))
        lag_minutes = _lag_minutes_for_run(run)
        if created_at is None or lag_minutes is None:
            continue
        lag_by_run_id[run_id] = (created_at, lag_minutes)
        if today_start <= created_at <= today_end:
            today_lags.append(lag_minutes)

    now_lag = None
    if lag_by_run_id:
        _, now_lag = max(lag_by_run_id.values(), key=lambda value: value[0])

    metadata_rows.extend(
        [
            ("raiderio_lag_now_minutes", now_lag),
            (
                "raiderio_lag_today_avg_minutes",
                None if not today_lags else round(sum(today_lags) / len(today_lags), 1),
            ),
            (
                "raiderio_lag_today_max_minutes",
                None if not today_lags else round(max(today_lags), 1),
            ),
            ("raiderio_lag_today_run_count", len(today_lags)),
        ]
    )
    return metadata_rows


class SyncService:
    """Coordinates roster reads, Raider.IO syncs, Mongo updates, and Sheets writes."""

    def __init__(
        self,
        *,
        settings: Settings,
        repository: MongoRepository,
        sheets_client: GoogleSheetsClient,
        raiderio_client: RaiderIOClient,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._sheets_client = sheets_client
        self._raiderio_client = raiderio_client
        self._stop_requested = False
        self._stop_event = threading.Event()

    def install_signal_handlers(self) -> None:
        """Install SIGTERM/SIGINT handlers for graceful shutdown."""

        def _handler(signum: int, _frame: Any) -> None:
            LOGGER.info("Received stop signal", extra={"signal": signum})
            self._stop_requested = True
            self._stop_event.set()

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    def run_forever(self) -> None:
        """Run the sync loop until stopped."""

        self.install_signal_handlers()
        consecutive_failures = 0
        while not self._stop_requested:
            try:
                self.run_cycle()
                consecutive_failures = 0
            except Exception:
                consecutive_failures += 1
                retry_delay = self._failure_backoff_seconds(consecutive_failures)
                LOGGER.warning(
                    "Sync loop will retry after a failed cycle",
                    extra={
                        "consecutive_failures": consecutive_failures,
                        "retry_delay_seconds": round(retry_delay, 1),
                    },
                )
                if self._stop_requested or self._wait_for_stop(retry_delay):
                    break
                continue
            if self._stop_requested:
                break
            remaining = self._next_cycle_delay_seconds()
            if remaining > 0 and not self._stop_requested:
                LOGGER.info("Sleeping before next cycle", extra={"sleep_seconds": round(remaining, 1)})
                if self._wait_for_stop(remaining):
                    break

    def run_cycle(self) -> None:
        """Run one full sync cycle."""

        started_at = utc_now()
        stats = SyncStats()
        initial_api_calls = self._raiderio_client.api_calls
        LOGGER.info("Starting sync cycle", extra={"started_at": started_at.isoformat()})

        try:
            season_dungeons = self._ensure_season_dungeons(now=started_at)
            raw_roster_rows = self._sheets_client.read_roster_rows()
            roster_entries = parse_roster_rows(
                raw_roster_rows,
                start_row=self._settings.google.roster_start_row,
            )
            stats.roster_rows = len(roster_entries)
            stats.invalid_players = len([entry for entry in roster_entries if not entry.is_valid])

            self._repository.sync_roster(roster_entries, seen_at=started_at)
            active_players = self._repository.list_active_players(
                limit=self._settings.sync.max_players_per_cycle
            )
            self._expire_hot_windows(active_players=active_players, now=started_at)
            self._queue_predictive_hot_players(
                active_players=active_players,
                now=started_at,
                stats=stats,
            )
            active_players = self._repository.list_active_players(
                limit=self._settings.sync.max_players_per_cycle
            )
            stats.active_players = len(active_players)
            stats.valid_players = len([player for player in active_players if player.get("is_valid")])
            weekly_periods: dict[str, dict[str, Any]] = {}
            if self._skip_raiderio_sync_due_to_cooldown(stats=stats):
                refreshed_players = active_players
            else:
                players_to_sync, _, hot_due_keys = self._select_players_for_sync(
                    now=started_at
                )
                required_regions = {
                    str(player.get("region", "")).lower()
                    for player in active_players
                    if player.get("is_valid") and player.get("region")
                }
                weekly_periods = self._load_current_weekly_periods(
                    now=started_at,
                    required_regions=required_regions,
                )
                stats.weekly_periods = _weekly_periods_for_metadata(weekly_periods)
                for player in active_players:
                    player_region = str(player.get("region", "")).lower()
                    if player.get("is_valid") and player_region and player_region not in weekly_periods:
                        message = (
                            f"Missing Raider.IO weekly period for region {player_region}; "
                            "weekly 10+ counts left blank."
                        )
                        if message not in stats.warnings:
                            stats.warnings.append(message)
                        stats.partial = True
                for player in players_to_sync:
                    if self._stop_requested:
                        LOGGER.info("Stop requested during player sync; ending cycle early")
                        stats.partial = True
                        break
                    player_key = player["player_key"]
                    sync_kind = "base"
                    if player_key in hot_due_keys:
                        stats.hot_players_synced += 1
                        sync_kind = "hot"
                        hot_ready_at = _safe_datetime(player.get("hot_ready_at"))
                        last_attempt = _last_attempted_sync_at(player)
                        if hot_ready_at and (last_attempt is None or last_attempt < hot_ready_at):
                            LOGGER.info(
                                "Hot polling window reached",
                                extra={
                                    "player_key": player_key,
                                    "hot_ready_at": hot_ready_at.isoformat(),
                                },
                            )
                    else:
                        stats.base_due_players_synced += 1
                    self._sync_player(
                        player=player,
                        stats=stats,
                        now=started_at,
                        sync_kind=sync_kind,
                    )
                    if self._skip_raiderio_sync_due_to_cooldown(stats=stats):
                        break

                refreshed_players = self._repository.list_active_players(
                    limit=self._settings.sync.max_players_per_cycle
                )
            player_keys = [player["player_key"] for player in refreshed_players]
            runs = self._repository.get_runs_for_players(player_keys)
            summary_header = build_summary_header(season_dungeons)
            summary_rows = build_summary_rows(
                refreshed_players,
                runs,
                season_dungeons,
                weekly_periods=weekly_periods,
            )
            metadata_rows = build_summary_metadata_rows(
                header=summary_header,
                runs=runs,
                now=started_at,
            )
            stats.sheet_rows_written = self._sheets_client.write_output_rows(
                summary_header,
                [row.to_sheet_row() for row in summary_rows],
                metadata_rows=metadata_rows,
            )
        except Exception:
            LOGGER.exception("Sync cycle failed")
            stats.partial = True
            raise
        finally:
            finished_at = utc_now()
            stats.api_calls = self._raiderio_client.api_calls - initial_api_calls
            self._repository.store_sync_cycle(
                stats.to_document(started_at=started_at, finished_at=finished_at)
            )
            LOGGER.info(
                "Finished sync cycle",
                extra={
                    "finished_at": finished_at.isoformat(),
                    "api_calls": stats.api_calls,
                    "base_due_players_synced": stats.base_due_players_synced,
                    "hot_players_synced": stats.hot_players_synced,
                    "predictive_hot_players_queued": stats.predictive_hot_players_queued,
                    "new_runs": stats.new_runs,
                    "sheet_rows_written": stats.sheet_rows_written,
                    "partial": stats.partial,
                },
            )

    def _sync_player(
        self,
        *,
        player: dict[str, Any],
        stats: SyncStats,
        now: Any,
        sync_kind: str,
    ) -> None:
        player_key = player["player_key"]
        identity = PlayerIdentity(
            region=player["region"],
            realm=player["realm"],
            name=player["name"],
            player_key=player_key,
        )
        self._repository.mark_sync_started(player_key, now, sync_kind=sync_kind)

        try:
            result = self._raiderio_client.get_character_profile(identity)
            profile = result.payload
            run_candidates = _collect_profile_run_candidates(profile)
            current_scores = _build_dungeon_scores(profile)
            current_total_score = _build_total_score(
                profile, season=self._settings.sync.current_season
            )
            self._repository.update_player_profile(
                player_key,
                current_dungeon_scores=current_scores,
                current_total_score=current_total_score,
                synced_at=now,
            )

            known_run_ids = self._repository.get_known_run_ids(list(run_candidates))
            new_run_completed_at: list[datetime] = []
            player_new_runs = 0
            for run_id, run_stub in run_candidates.items():
                if self._stop_requested:
                    LOGGER.info(
                        "Stop requested during run discovery; ending player sync early",
                        extra={"player_key": player_key},
                    )
                    return
                self._repository.attach_player_to_run(run_id, player_key)
                if run_id in known_run_ids:
                    continue
                self._repository.upsert_run_stub(
                    run_stub,
                    player_key=player_key,
                    season=self._settings.sync.current_season,
                    synced_at=now,
                )
                stats.new_runs += 1
                player_new_runs += 1
                completed_at = _safe_datetime(run_stub.get("completed_at"))
                if completed_at is not None:
                    new_run_completed_at.append(completed_at)
            if player_new_runs > 0:
                profile_completed_at = (
                    new_run_completed_at if new_run_completed_at else [ensure_utc(now)]
                )
                existing_profile = {
                    "play_profile_first_week_start_at": player.get("play_profile_first_week_start_at"),
                    "play_profile_last_seeded_at": player.get("play_profile_last_seeded_at"),
                    "play_profile_weeks_observed": player.get("play_profile_weeks_observed", 0),
                    "play_profile_hour_counts": player.get("play_profile_hour_counts", []),
                    "play_profile_hour_probabilities": player.get(
                        "play_profile_hour_probabilities", []
                    ),
                    "play_profile_seen_week_hours": player.get("play_profile_seen_week_hours", []),
                    "play_profile_last_enqueued_week_hour": player.get(
                        "play_profile_last_enqueued_week_hour", ""
                    ),
                }
                if existing_profile.get("play_profile_seen_week_hours"):
                    profile = update_play_profile(
                        existing_profile=existing_profile,
                        completed_at_values=profile_completed_at,
                        now=ensure_utc(now),
                    )
                else:
                    profile = build_play_profile(
                        completed_at_values=profile_completed_at,
                        now=ensure_utc(now),
                        last_seeded_at=player.get("play_profile_last_seeded_at"),
                        last_enqueued_week_hour=str(
                            player.get("play_profile_last_enqueued_week_hour", "") or ""
                        ),
                    )
                self._repository.upsert_player_play_profile(player_key=player_key, profile=profile)
        except RaiderIONotFoundError:
            message = "Raider.IO could not find this player."
            LOGGER.warning(
                "Raider.IO could not resolve player %s (%s/%s/%s)",
                player_key,
                identity.region,
                identity.realm,
                identity.name,
            )
            self._repository.mark_invalid_player(player_key, message, when=now)
        except RaiderIOError as exc:
            message = str(exc)
            LOGGER.error(
                "Player sync failed for %s (%s/%s/%s): %s",
                player_key,
                identity.region,
                identity.realm,
                identity.name,
                message,
                exc_info=True,
            )
            self._repository.mark_sync_error(player_key, message, when=now)
            stats.partial = True
            stats.warnings.append(f"{player_key}: {message}")

    def _ensure_season_dungeons(self, *, now: Any) -> list[dict[str, Any]]:
        """Load season dungeon metadata from Mongo or Raider.IO."""

        season = self._settings.sync.current_season
        dungeons = self._repository.list_season_dungeons(season=season)
        if dungeons:
            return dungeons

        expansion_id = _season_slug_to_expansion_id(season)
        payload = self._raiderio_client.get_mythic_plus_static_data(
            expansion_id=expansion_id
        ).payload
        seasons = payload.get("seasons", []) or []
        season_payload = next((item for item in seasons if item.get("slug") == season), None)
        if season_payload is None:
            raise RaiderIOError(f"Raider.IO static data did not include season {season}")

        season_dungeons = [
            SeasonDungeon(
                season=season,
                dungeon_id=dungeon.get("id"),
                slug=str(dungeon.get("slug", "")),
                name=str(dungeon.get("name", "")),
                short_name=str(dungeon.get("short_name", "")),
                challenge_mode_id=dungeon.get("challenge_mode_id"),
                keystone_timer_seconds=dungeon.get("keystone_timer_seconds"),
                icon_url=str(dungeon.get("icon_url", "")),
                background_image_url=str(dungeon.get("background_image_url", "")),
            )
            for dungeon in season_payload.get("dungeons", []) or []
            if dungeon.get("slug") and dungeon.get("short_name")
        ]
        self._repository.replace_season_dungeons(
            season=season,
            dungeons=season_dungeons,
            synced_at=now,
        )
        LOGGER.info(
            "Cached season dungeon metadata",
            extra={"season": season, "dungeon_count": len(season_dungeons)},
        )
        return self._repository.list_season_dungeons(season=season)

    def _load_current_weekly_periods(
        self,
        *,
        now: datetime,
        required_regions: set[str],
    ) -> dict[str, dict[str, Any]]:
        """Load current weekly periods from cache when possible, else refresh from Raider.IO."""

        cached_periods = self._repository.get_current_weekly_periods(
            now=now,
            regions=required_regions,
        )
        if required_regions and required_regions.issubset(cached_periods):
            LOGGER.info(
                "Using cached Raider.IO weekly periods",
                extra={"regions": sorted(cached_periods)},
            )
            return cached_periods

        payload = self._raiderio_client.get_periods().payload
        periods_by_region = _normalize_weekly_periods(payload)
        self._repository.replace_weekly_periods(
            periods_by_region=periods_by_region,
            synced_at=now,
        )
        LOGGER.info(
            "Resolved Raider.IO weekly periods",
            extra={"regions": sorted(periods_by_region)},
        )
        if not required_regions:
            return periods_by_region
        return {
            region: period
            for region, period in periods_by_region.items()
            if region in required_regions
        }

    def _skip_raiderio_sync_due_to_cooldown(self, *, stats: SyncStats) -> bool:
        """Stop making Raider.IO calls while a persistent cooldown is active."""

        cooldown_remaining = self._raiderio_client.get_cooldown_remaining_seconds()
        if cooldown_remaining <= 0:
            return False

        cooldown_reason = self._raiderio_client.get_cooldown_reason() or "Raider.IO cooldown active"
        message = f"{cooldown_reason}; using cached data for {round(cooldown_remaining, 1)}s."
        if message not in stats.warnings:
            LOGGER.warning(
                "Skipping Raider.IO sync while cooldown is active",
                extra={
                    "cooldown_reason": cooldown_reason,
                    "cooldown_remaining_seconds": round(cooldown_remaining, 1),
                },
            )
            stats.warnings.append(message)
        stats.partial = True
        return True

    def _failure_backoff_seconds(self, consecutive_failures: int) -> float:
        """Compute capped exponential backoff with jitter for failed cycles."""

        base_delay = self._settings.sync.failure_backoff_seconds
        max_delay = self._settings.sync.max_failure_backoff_seconds
        jitter = self._settings.sync.failure_backoff_jitter_seconds
        exponential_delay = min(base_delay * (2 ** max(consecutive_failures - 1, 0)), max_delay)
        if jitter <= 0:
            return exponential_delay
        return min(exponential_delay + random.uniform(0, jitter), max_delay)

    def _wait_for_stop(self, timeout_seconds: float) -> bool:
        """Wait for either the timeout or a stop signal."""

        return self._stop_event.wait(timeout=max(timeout_seconds, 0.0))

    def _queue_predictive_hot_players(
        self,
        *,
        active_players: list[dict[str, Any]],
        now: datetime,
        stats: SyncStats,
    ) -> None:
        """Queue hot polling for players predicted to play in the current Pacific hour."""

        if not self._settings.sync.predictive_hot_enabled:
            return

        current_slot_index = pacific_week_hour_index(now)
        current_week_hour = current_week_hour_key(now)
        current_hour_start = pacific_hour_start(now)
        predictive_hot_until = current_hour_start + timedelta(
            minutes=self._settings.sync.active_idle_minutes
        )
        for player in active_players:
            if not player.get("is_valid", False):
                continue
            player_key = player["player_key"]
            refreshed_profile = self._refresh_play_profile_for_current_week(
                player=player,
                now=now,
            )
            probabilities = refreshed_profile.get("play_profile_hour_probabilities", []) or []
            if len(probabilities) != PLAY_PROFILE_HOURS_PER_WEEK:
                continue
            if refreshed_profile.get("play_profile_last_enqueued_week_hour") == current_week_hour:
                continue
            probability = float(probabilities[current_slot_index])
            if probability < self._settings.sync.predictive_hot_threshold:
                continue
            self._repository.mark_predictive_hot_enqueue(
                player_key=player_key,
                week_hour_key=current_week_hour,
                hot_ready_at=current_hour_start,
                hot_until_at=predictive_hot_until,
            )
            stats.predictive_hot_players_queued += 1
            LOGGER.info(
                "Queued predictive hot polling",
                extra={
                    "player_key": player_key,
                    "week_hour_index": current_slot_index,
                    "probability": round(probability, 4),
                    "hot_ready_at": current_hour_start.isoformat(),
                    "hot_until_at": predictive_hot_until.isoformat(),
                },
            )

    def _refresh_play_profile_for_current_week(
        self,
        *,
        player: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        """Refresh stored profile probabilities when a new calendar week changes the denominator."""

        first_week_start_at = _safe_datetime(player.get("play_profile_first_week_start_at"))
        seen_week_hours = player.get("play_profile_seen_week_hours", []) or []
        if first_week_start_at is None or not seen_week_hours:
            return player
        expected_weeks = expected_weeks_observed(first_week_start_at, now=now)
        if int(player.get("play_profile_weeks_observed", 0) or 0) == expected_weeks:
            return player
        profile = update_play_profile(
            existing_profile=player,
            completed_at_values=[],
            now=ensure_utc(now),
        )
        self._repository.upsert_player_play_profile(player_key=player["player_key"], profile=profile)
        refreshed_player = dict(player)
        refreshed_player.update(profile)
        return refreshed_player

    def _select_players_for_sync(
        self,
        *,
        now: datetime,
    ) -> tuple[list[dict[str, Any]], set[str], set[str]]:
        """Select valid active players due for either base or hot polling."""

        limit = self._settings.sync.max_players_per_cycle
        base_due_players = self._repository.list_players_due_for_base_sync(
            now=_current_hot_batch_start(
                now,
                interval_minutes=self._settings.sync.interval_minutes,
            ),
            interval_minutes=self._settings.sync.interval_minutes,
            limit=limit,
        )
        hot_due_players = self._repository.list_players_due_for_hot_sync(
            now=_current_hot_batch_start(
                now,
                interval_minutes=self._settings.sync.active_interval_minutes,
            ),
            interval_minutes=self._settings.sync.active_interval_minutes,
            limit=limit,
        )

        selected_players: list[dict[str, Any]] = []
        selected_keys: set[str] = set()
        base_due_keys = {player["player_key"] for player in base_due_players}
        hot_due_keys: set[str] = set()
        for player in base_due_players + hot_due_players:
            player_key = player["player_key"]
            if player_key in selected_keys:
                continue
            if len(selected_players) >= limit:
                break
            selected_players.append(player)
            selected_keys.add(player_key)
            if player_key not in base_due_keys:
                hot_due_keys.add(player_key)
        return selected_players, base_due_keys & selected_keys, hot_due_keys

    def _expire_hot_windows(self, *, active_players: list[dict[str, Any]], now: datetime) -> None:
        """Clear any expired hot windows so they are not reconsidered indefinitely."""

        for player in active_players:
            hot_until_at = _safe_datetime(player.get("hot_until_at"))
            if hot_until_at is None or hot_until_at > ensure_utc(now):
                continue
            hot_ready_at = _safe_datetime(player.get("hot_ready_at"))
            if hot_ready_at is None:
                continue
            LOGGER.info(
                "Hot polling window expired",
                extra={
                    "player_key": player["player_key"],
                    "hot_ready_at": hot_ready_at.isoformat(),
                    "hot_until_at": hot_until_at.isoformat(),
                },
            )
            self._repository.clear_player_hot_window(player_key=player["player_key"])

    def _next_cycle_delay_seconds(self) -> float:
        """Compute the next sleep duration from base cadence and hot windows."""

        active_players = self._repository.list_active_players(
            limit=self._settings.sync.max_players_per_cycle
        )
        valid_players = [player for player in active_players if player.get("is_valid", False)]
        if not valid_players:
            return float(self._settings.sync.interval_minutes * 60)

        now = utc_now()
        normalized_now = ensure_utc(now)
        next_due_at = _next_hot_batch_at_or_after(
            normalized_now + timedelta(minutes=self._settings.sync.interval_minutes),
            interval_minutes=self._settings.sync.interval_minutes,
        )

        for player in valid_players:
            last_attempt = _last_attempted_sync_at(player)
            if last_attempt is not None:
                next_base_batch_at = _current_hot_batch_start(
                    last_attempt,
                    interval_minutes=self._settings.sync.interval_minutes,
                ) + timedelta(minutes=self._settings.sync.interval_minutes)
                next_due_at = min(next_due_at, next_base_batch_at)
            hot_ready_at = _safe_datetime(player.get("hot_ready_at"))
            hot_until_at = _safe_datetime(player.get("hot_until_at"))
            if hot_ready_at is None or hot_until_at is None or hot_until_at <= normalized_now:
                continue
            last_hot_batch_at = None
            if last_attempt is not None:
                last_hot_batch_at = _current_hot_batch_start(
                    last_attempt,
                    interval_minutes=self._settings.sync.active_interval_minutes,
                ) + timedelta(minutes=self._settings.sync.active_interval_minutes)
            next_hot_batch_at = _next_hot_batch_at_or_after(
                max(normalized_now, hot_ready_at),
                interval_minutes=self._settings.sync.active_interval_minutes,
            )
            if last_hot_batch_at is not None:
                next_hot_batch_at = max(next_hot_batch_at, last_hot_batch_at)
            if next_hot_batch_at < hot_until_at:
                next_due_at = min(next_due_at, next_hot_batch_at)

        predictive_wake_at = self._next_predictive_wake_at(valid_players=valid_players, now=normalized_now)
        if predictive_wake_at is not None:
            next_due_at = min(next_due_at, predictive_wake_at)

        return max((next_due_at - normalized_now).total_seconds(), 0.0)

    def _next_predictive_wake_at(
        self,
        *,
        valid_players: list[dict[str, Any]],
        now: datetime,
    ) -> datetime | None:
        """Return the next top-of-hour wake-up needed for predictive hot scheduling."""

        if not self._settings.sync.predictive_hot_enabled:
            return None

        next_hour_start = next_pacific_hour_start(now)
        next_slot_index = pacific_week_hour_index(next_hour_start)
        next_week_hour = current_week_hour_key(next_hour_start)
        for player in valid_players:
            refreshed_profile = self._refresh_play_profile_for_current_week(player=player, now=now)
            probabilities = refreshed_profile.get("play_profile_hour_probabilities", []) or []
            if len(probabilities) != PLAY_PROFILE_HOURS_PER_WEEK:
                continue
            if refreshed_profile.get("play_profile_last_enqueued_week_hour") == next_week_hour:
                continue
            if float(probabilities[next_slot_index]) >= self._settings.sync.predictive_hot_threshold:
                return next_hour_start
        return None
