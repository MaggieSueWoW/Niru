"""Manual season backfill command."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import logging
from typing import Any

from niru.clients.raiderio import RaiderIOClient, RaiderIOError
from niru.clients.raiderio_internal import RaiderIOInternalClient
from niru.config import Settings, load_settings
from niru.control_state import RedisControlState
from niru.logging_utils import configure_logging
from niru.models import PlayerIdentity, SeasonDungeon, utc_now
from niru.roster import parse_roster_value
from niru.service import _season_slug_to_expansion_id
from niru.storage import MongoRepository

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class BackfillStats:
    """Counters for one manual backfill run."""

    players: int = 0
    dungeons: int = 0
    discovered_run_ids: int = 0
    known_run_ids: int = 0
    missing_run_ids: int = 0
    inserted_runs: int = 0
    warnings: list[str] = field(default_factory=list)


class BackfillService:
    """Discovers missing season runs and loads them via the public API."""

    def __init__(
        self,
        *,
        settings: Settings,
        repository: MongoRepository,
        public_client: RaiderIOClient,
        internal_client: RaiderIOInternalClient,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._public_client = public_client
        self._internal_client = internal_client

    def run(
        self,
        *,
        players: list[PlayerIdentity],
        season: str,
        dry_run: bool = False,
        limit_runs: int | None = None,
    ) -> BackfillStats:
        """Run one manual backfill pass."""

        now = utc_now()
        stats = BackfillStats(players=len(players))
        season_dungeons = self._ensure_season_dungeons(season=season, now=now)
        stats.dungeons = len(season_dungeons)
        all_discovered_run_ids: set[int] = set()
        remaining_limit = limit_runs
        for player in players:
            try:
                character_id = self._get_or_resolve_character_id(
                    player=player,
                    season=season,
                    now=now,
                )
                player_run_ids = self._discover_player_run_ids(
                    player=player,
                    season=season,
                    character_id=character_id,
                    season_dungeons=season_dungeons,
                )
                all_discovered_run_ids.update(player_run_ids)

                known_run_ids = self._repository.get_known_run_ids(sorted(player_run_ids))
                if not dry_run:
                    for run_id in player_run_ids:
                        if run_id in known_run_ids:
                            self._repository.attach_player_to_run(run_id, player.player_key)

                missing_run_ids = [
                    run_id for run_id in sorted(player_run_ids) if run_id not in known_run_ids
                ]
                if remaining_limit is not None:
                    missing_run_ids = missing_run_ids[:remaining_limit]

                stats.known_run_ids += len(known_run_ids)
                stats.missing_run_ids += len(missing_run_ids)

                LOGGER.info(
                    "Backfill player discovery complete",
                    extra={
                        "player_key": player.player_key,
                        "discovered_run_ids": len(player_run_ids),
                        "known_run_ids": len(known_run_ids),
                        "missing_run_ids": len(missing_run_ids),
                        "dry_run": dry_run,
                    },
                )

                if dry_run:
                    continue

                for run_id in missing_run_ids:
                    result = self._public_client.get_run_details(season=season, run_id=run_id)
                    payload = result.payload
                    canonical_payload = payload.get("mythic_plus_run") or payload.get("run") or payload
                    discovered_players = self._extract_player_keys(canonical_payload)
                    player_key = discovered_players[0] if discovered_players else ""
                    self._repository.update_run_details(
                        run_id=run_id,
                        payload=canonical_payload,
                        player_key=player_key,
                        synced_at=now,
                    )
                    stats.inserted_runs += 1

                if remaining_limit is not None:
                    remaining_limit = max(remaining_limit - len(missing_run_ids), 0)
                    if remaining_limit == 0:
                        LOGGER.info(
                            "Backfill run limit reached",
                            extra={"limit_runs": limit_runs, "player_key": player.player_key},
                        )
                        break
            except RaiderIOError as exc:
                message = f"{player.player_key}: {exc}"
                LOGGER.warning("Backfill discovery failed for %s: %s", player.player_key, exc)
                stats.warnings.append(message)

        stats.discovered_run_ids = len(all_discovered_run_ids)

        LOGGER.info(
            "Backfill discovery complete",
            extra={
                "players": stats.players,
                "dungeons": stats.dungeons,
                "discovered_run_ids": stats.discovered_run_ids,
                "known_run_ids": stats.known_run_ids,
                "missing_run_ids": stats.missing_run_ids,
                "dry_run": dry_run,
            },
        )

        LOGGER.info(
            "Backfill complete",
            extra={
                "players": stats.players,
                "dungeons": stats.dungeons,
                "discovered_run_ids": stats.discovered_run_ids,
                "known_run_ids": stats.known_run_ids,
                "missing_run_ids": stats.missing_run_ids,
                "inserted_runs": stats.inserted_runs,
                "public_api_calls": self._public_client.api_calls,
                "internal_api_calls": self._internal_client.api_calls,
            },
        )
        return stats

    def _get_or_resolve_character_id(
        self,
        *,
        player: PlayerIdentity,
        season: str,
        now: Any,
    ) -> int:
        cached = self._repository.get_player_character_id(player_key=player.player_key)
        if cached is not None:
            return cached

        result = self._internal_client.get_character_page(player, season=season)
        character_id = self._internal_client.extract_character_id(result.payload)
        self._repository.cache_player_character_id(
            player_key=player.player_key,
            identity=player,
            character_id=character_id,
            resolved_at=now,
        )
        return character_id

    def _discover_player_run_ids(
        self,
        *,
        player: PlayerIdentity,
        season: str,
        character_id: int,
        season_dungeons: list[dict[str, Any]],
    ) -> set[int]:
        discovered: set[int] = set()
        for dungeon in season_dungeons:
            dungeon_id = dungeon.get("dungeon_id")
            if dungeon_id is None:
                continue
            result = self._internal_client.get_character_dungeon_runs(
                season=season,
                character_id=character_id,
                dungeon_id=int(dungeon_id),
            )
            for run in result.payload.get("runs", []) or []:
                summary = run.get("summary") or {}
                run_id = summary.get("keystone_run_id")
                if run_id is None:
                    continue
                discovered.add(int(run_id))
            LOGGER.info(
                "Discovered dungeon runs",
                extra={
                    "player_key": player.player_key,
                    "dungeon_id": dungeon_id,
                    "dungeon_short_name": dungeon.get("short_name", ""),
                    "total_runs": len(result.payload.get("runs", []) or []),
                },
            )
        return discovered

    def _ensure_season_dungeons(self, *, season: str, now: Any) -> list[dict[str, Any]]:
        season_dungeons = self._repository.list_season_dungeons(season=season)
        if season_dungeons and all(dungeon.get("dungeon_id") is not None for dungeon in season_dungeons):
            return season_dungeons

        expansion_id = _season_slug_to_expansion_id(season)
        payload = self._public_client.get_mythic_plus_static_data(expansion_id=expansion_id).payload
        seasons = payload.get("seasons", []) or []
        season_payload = next((item for item in seasons if item.get("slug") == season), None)
        if season_payload is None:
            raise RaiderIOError(f"Raider.IO static data did not include season {season}")

        dungeons = [
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
            if dungeon.get("slug") and dungeon.get("short_name") and dungeon.get("id") is not None
        ]
        self._repository.replace_season_dungeons(season=season, dungeons=dungeons, synced_at=now)
        return self._repository.list_season_dungeons(season=season)

    @staticmethod
    def _extract_player_keys(payload: dict[str, Any]) -> list[str]:
        keys: list[str] = []
        for participant in payload.get("roster", []) or []:
            character = participant.get("character", participant)
            region = (
                (character.get("region") or {}).get("slug")
                or character.get("region")
                or ""
            )
            realm = ((character.get("realm") or {}).get("slug")) or character.get("realm") or ""
            name = character.get("name", "")
            if region and realm and name:
                keys.append(f"{str(region).lower()}/{str(realm).lower()}/{str(name).lower()}")
        return keys


def parse_args() -> argparse.Namespace:
    """Parse command line arguments for manual backfill."""

    parser = argparse.ArgumentParser(description="Manually backfill missing Mythic+ runs.")
    parser.add_argument("--config", default="config.yaml", help="Path to the YAML config file.")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--players",
        nargs="+",
        help="One or more roster-format players: region/realm/name",
    )
    source_group.add_argument(
        "--all-active-players",
        action="store_true",
        help="Backfill every active player currently stored in MongoDB.",
    )
    parser.add_argument(
        "--season",
        default=None,
        help="Season slug to backfill. Defaults to sync.current_season from config.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover missing run IDs without fetching or storing run details.",
    )
    parser.add_argument(
        "--limit-runs",
        type=int,
        default=None,
        help="Cap the number of missing run IDs fetched from the public API.",
    )
    return parser.parse_args()


def _parse_players(raw_players: list[str]) -> list[PlayerIdentity]:
    parsed: list[PlayerIdentity] = []
    for index, raw_player in enumerate(raw_players, start=2):
        entry = parse_roster_value(index, raw_player)
        if not entry.is_valid or entry.identity is None:
            raise ValueError(f"Invalid --players entry '{raw_player}': {entry.status_message}")
        parsed.append(entry.identity)
    return parsed


def _load_all_active_players(repository: MongoRepository) -> list[PlayerIdentity]:
    players: list[PlayerIdentity] = []
    for document in repository.list_all_active_players():
        if not document.get("is_valid", False):
            continue
        region = str(document.get("region", "")).strip().lower()
        realm = str(document.get("realm", "")).strip().lower()
        name = str(document.get("name", "")).strip()
        player_key = str(document.get("player_key", "")).strip().lower()
        if not (region and realm and name and player_key):
            continue
        players.append(
            PlayerIdentity(
                region=region,
                realm=realm,
                name=name,
                player_key=player_key,
            )
        )
    return players


def main() -> None:
    """Run the manual backfill command."""

    args = parse_args()
    settings = load_settings(args.config)
    configure_logging(settings.logging.level)
    season = args.season or settings.sync.current_season

    repository = MongoRepository(settings.mongodb)
    control_state = RedisControlState(settings.redis)
    public_client = RaiderIOClient(settings.raiderio, control_state=control_state)
    internal_client = RaiderIOInternalClient(settings.raiderio, control_state=control_state)
    service = BackfillService(
        settings=settings,
        repository=repository,
        public_client=public_client,
        internal_client=internal_client,
    )
    try:
        players = (
            _load_all_active_players(repository)
            if args.all_active_players
            else _parse_players(args.players)
        )
        if not players:
            raise ValueError("No active valid players found for backfill.")
        service.run(
            players=players,
            season=season,
            dry_run=args.dry_run,
            limit_runs=args.limit_runs,
        )
    finally:
        repository.close()


if __name__ == "__main__":
    main()
