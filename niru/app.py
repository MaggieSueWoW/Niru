"""Application entrypoint."""

from __future__ import annotations

import argparse
import logging

from niru.clients.blizzard import BlizzardClient
from niru.clients.raiderio import RaiderIOClient
from niru.clients.sheets import GoogleSheetsClient
from niru.config import load_settings
from niru.control_state import RedisControlState
from niru.logging_utils import configure_logging
from niru.roster import parse_player_key
from niru.service import SyncService
from niru.storage import MongoRepository

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Run the Mythic+ roster bot.")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to the YAML config file.",
    )
    parser.add_argument(
        "--mode",
        choices=("loop", "once"),
        default="loop",
        help="Run forever on the configured cadence or execute a single sync cycle.",
    )
    parser.add_argument(
        "--player",
        help="Restrict --mode once to a single roster entry (region/realm/name).",
    )
    args = parser.parse_args()
    if args.player:
        if args.mode != "once":
            parser.error("--player can only be used with --mode once.")
        try:
            args.player = parse_player_key(args.player)
        except ValueError as exc:
            parser.error(f"--player must use region/realm/name format: {exc}")
    return args


def main() -> None:
    """Run the sync service."""

    args = parse_args()
    settings = load_settings(args.config)
    configure_logging(settings.logging.level)
    control_state = RedisControlState(settings.redis)
    repository = MongoRepository(settings.mongodb)
    sheets_client = GoogleSheetsClient(settings.google)
    raiderio_client = RaiderIOClient(settings.raiderio, control_state=control_state)
    blizzard_client = BlizzardClient(settings.blizzard)
    service = SyncService(
        settings=settings,
        repository=repository,
        sheets_client=sheets_client,
        raiderio_client=raiderio_client,
        blizzard_client=blizzard_client,
    )
    try:
        if args.mode == "once":
            service.install_signal_handlers()
            service.run_cycle(force_sync_all=True, player_key=args.player)
        else:
            service.run_forever()
    except Exception:
        LOGGER.exception("Fatal application error")
        raise
    finally:
        repository.close()
