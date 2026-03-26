"""Core sync service."""

from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
import logging
import signal
import threading
import time
from typing import Any

from mplusbot.clients.raiderio import RaiderIOClient, RaiderIOError, RaiderIONotFoundError
from mplusbot.clients.sheets import GoogleSheetsClient
from mplusbot.config import Settings
from mplusbot.models import (
    PlayerDataStatus,
    PlayerIdentity,
    SeasonDungeon,
    SummaryRow,
    SyncStats,
    ensure_utc,
    format_pacific_time,
    utc_now,
)
from mplusbot.roster import parse_roster_rows
from mplusbot.storage import MongoRepository

LOGGER = logging.getLogger(__name__)

PLAYER_COLUMNS = [
    "region",
    "realm",
    "name",
    "current_total_mythic_plus_rating",
    "last_successful_sync_time_pacific",
]

DUNGEON_FIELDS = [
    "current_score",
    "best_key_level",
    "best_upgrade_level",
    "total_runs",
]


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
) -> list[SummaryRow]:
    """Build Google Sheets summary rows from Mongo state."""

    grouped_runs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        player_keys = set(run.get("discovered_from_player_keys", []))
        for participant in run.get("participants", []):
            player_key = participant.get("player_key")
            if player_key:
                player_keys.add(player_key)
        for player_key in player_keys:
            grouped_runs[player_key].append(run)

    sorted_players = sorted(
        players,
        key=lambda item: (
            -(float(item["current_total_score"]) if item.get("current_total_score") is not None else -1.0),
            item["player_key"],
        ),
    )

    rows: list[SummaryRow] = []
    for player in sorted_players:
        runs_for_player = grouped_runs.get(player["player_key"], [])
        by_dungeon: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for run in runs_for_player:
            dungeon_key = run.get("short_name") or run.get("dungeon") or ""
            if dungeon_key:
                by_dungeon[dungeon_key].append(run)

        current_scores: dict[str, float] = player.get("current_dungeon_scores", {})
        values: list[object] = [
            player.get("region", ""),
            player.get("realm", ""),
            player.get("name", ""),
            None
            if player.get("current_total_score") is None
            else round(float(player["current_total_score"]), 1),
            format_pacific_time(player.get("last_successful_sync_at")),
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
        interval_seconds = self._settings.sync.interval_minutes * 60
        while not self._stop_requested:
            cycle_started = time.monotonic()
            self.run_cycle()
            if self._stop_requested:
                break
            elapsed = time.monotonic() - cycle_started
            remaining = max(interval_seconds - elapsed, 0)
            if remaining > 0 and not self._stop_requested:
                LOGGER.info("Sleeping before next cycle", extra={"sleep_seconds": round(remaining, 1)})
                if self._stop_event.wait(timeout=remaining):
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
            stats.active_players = len(active_players)
            stats.valid_players = len([player for player in active_players if player.get("is_valid")])

            for player in active_players:
                if self._stop_requested:
                    LOGGER.info("Stop requested during player sync; ending cycle early")
                    stats.partial = True
                    break
                if not player.get("is_valid", False):
                    continue
                self._sync_player(player=player, stats=stats, now=started_at)

            refreshed_players = self._repository.list_active_players(
                limit=self._settings.sync.max_players_per_cycle
            )
            player_keys = [player["player_key"] for player in refreshed_players]
            runs = self._repository.get_runs_for_players(player_keys)
            summary_header = build_summary_header(season_dungeons)
            summary_rows = build_summary_rows(refreshed_players, runs, season_dungeons)
            stats.sheet_rows_written = self._sheets_client.write_output_rows(
                summary_header,
                [row.to_sheet_row() for row in summary_rows],
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
                    "new_runs": stats.new_runs,
                    "sheet_rows_written": stats.sheet_rows_written,
                    "partial": stats.partial,
                },
            )

    def _sync_player(self, *, player: dict[str, Any], stats: SyncStats, now: Any) -> None:
        player_key = player["player_key"]
        identity = PlayerIdentity(
            region=player["region"],
            realm=player["realm"],
            name=player["name"],
            player_key=player_key,
        )
        self._repository.mark_sync_started(player_key, now)

        last_success = player.get("last_successful_sync_at")
        if last_success:
            last_success_utc = ensure_utc(last_success)
            gap_threshold = timedelta(
                minutes=self._settings.sync.interval_minutes
                * self._settings.sync.gap_detection_cycles
            )
            if ensure_utc(now) - last_success_utc > gap_threshold:
                message = "Missed polling window; coverage may be incomplete."
                self._repository.mark_gap_flag(player_key, message)
                stats.partial = True
                if message not in stats.warnings:
                    stats.warnings.append(message)

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
