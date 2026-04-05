"""Seed predictive play profiles from stored run history."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime
import logging

from niru.config import Settings, load_settings
from niru.logging_utils import configure_logging
from niru.models import PlayerIdentity, ensure_utc, utc_now
from niru.play_profile import build_play_profile
from niru.roster import parse_roster_value
from niru.storage import MongoRepository

LOGGER = logging.getLogger(__name__)


def _coerce_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return ensure_utc(value)
    if isinstance(value, str) and value:
        return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    return None


@dataclass(slots=True)
class PlayProfileSeedStats:
    """Counters for a play-profile seed run."""

    players: int = 0
    seeded_players: int = 0
    dry_run: bool = False
    warnings: list[str] = field(default_factory=list)


class PlayProfileSeedService:
    """Seeds predictive play profiles from stored run history."""

    def __init__(self, *, settings: Settings, repository: MongoRepository) -> None:
        self._settings = settings
        self._repository = repository

    def run(
        self,
        *,
        players: list[PlayerIdentity],
        dry_run: bool = False,
    ) -> PlayProfileSeedStats:
        """Seed predictive play profiles for the supplied players."""

        now = utc_now()
        stats = PlayProfileSeedStats(players=len(players), dry_run=dry_run)
        for player in players:
            runs = self._repository.get_runs_for_player(
                player_key=player.player_key,
                season=self._settings.sync.current_season,
            )
            completed_datetimes = [
                completed_at
                for run in runs
                if (completed_at := _coerce_datetime(run.get("completed_at"))) is not None
            ]
            profile = build_play_profile(
                completed_at_values=completed_datetimes,
                now=now,
                last_seeded_at=now,
            )
            LOGGER.info(
                "Prepared play profile seed",
                extra={
                    "player_key": player.player_key,
                    "season_runs": len(runs),
                    "weeks_observed": profile["play_profile_weeks_observed"],
                },
            )
            if dry_run:
                continue
            self._repository.upsert_player_play_profile(
                player_key=player.player_key,
                profile=profile,
            )
            stats.seeded_players += 1

        LOGGER.info(
            "Play profile seed complete",
            extra={
                "players": stats.players,
                "seeded_players": stats.seeded_players,
                "dry_run": stats.dry_run,
            },
        )
        return stats


def parse_args() -> argparse.Namespace:
    """Parse play-profile seed command-line arguments."""

    parser = argparse.ArgumentParser(description="Seed predictive play profiles from stored runs.")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to the YAML config file.",
    )
    parser.add_argument(
        "--player",
        action="append",
        dest="players",
        help="Roster-style player identity to seed, for example us/area-52/Mythics.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build profiles without writing them back to MongoDB.",
    )
    return parser.parse_args()


def _parse_players(raw_players: list[str]) -> list[PlayerIdentity]:
    parsed_players: list[PlayerIdentity] = []
    for raw_player in raw_players:
        entry = parse_roster_value(1, raw_player)
        if not entry.is_valid or entry.identity is None:
            raise ValueError(f"Invalid player value: {raw_player}")
        parsed_players.append(entry.identity)
    return parsed_players


def _load_all_active_players(repository: MongoRepository) -> list[PlayerIdentity]:
    players: list[PlayerIdentity] = []
    for document in repository.list_all_active_players():
        if not document.get("is_valid", False):
            continue
        players.append(
            PlayerIdentity(
                region=str(document.get("region", "")),
                realm=str(document.get("realm", "")),
                name=str(document.get("name", "")),
                player_key=str(document.get("player_key", "")),
            )
        )
    return players


def main() -> None:
    """Run the play-profile seed command."""

    args = parse_args()
    settings = load_settings(args.config)
    configure_logging(settings.logging.level)
    repository = MongoRepository(settings.mongodb)
    try:
        players = _parse_players(args.players) if args.players else _load_all_active_players(repository)
        service = PlayProfileSeedService(settings=settings, repository=repository)
        service.run(players=players, dry_run=args.dry_run)
    finally:
        repository.close()
