"""Roster parsing helpers."""

from __future__ import annotations

import hashlib
import logging
import re
from collections import Counter

from niru.models import PlayerDataStatus, PlayerIdentity, RosterEntry

LOGGER = logging.getLogger(__name__)

ROSTER_SPLIT_RE = re.compile(r"^\s*([^/\s]+)\s*/\s*([^/]+?)\s*/\s*([^/]+?)\s*$")
VALID_REGIONS = {"us", "eu", "tw", "kr", "cn"}


def normalize_realm(realm: str) -> str:
    """Normalize a realm into Raider.IO-compatible slug form."""

    return "-".join(realm.strip().lower().replace("_", " ").split())


def normalize_name(name: str) -> str:
    """Normalize a character name for keying."""

    return name.strip().lower()


def build_player_key(region: str, realm: str, name: str) -> str:
    """Build the canonical player key."""

    return f"{region}/{normalize_realm(realm)}/{normalize_name(name)}"


def build_invalid_key(row_number: int, raw_value: str) -> str:
    """Build a stable synthetic key for invalid roster rows."""

    digest = hashlib.sha1(raw_value.encode("utf-8")).hexdigest()[:10]
    return f"invalid/{row_number}/{digest}"


def parse_roster_value(row_number: int, raw_value: str) -> RosterEntry:
    """Parse one raw roster value into a structured entry."""

    value = raw_value.strip()
    invalid_key = build_invalid_key(row_number, raw_value)
    if not value:
        return RosterEntry(
            row_number=row_number,
            raw_value=raw_value,
            player_key=invalid_key,
            identity=None,
            is_valid=False,
            status=PlayerDataStatus.INVALID_PLAYER,
            status_message="Empty roster entry.",
        )

    match = ROSTER_SPLIT_RE.match(value)
    if not match:
        return RosterEntry(
            row_number=row_number,
            raw_value=raw_value,
            player_key=invalid_key,
            identity=None,
            is_valid=False,
            status=PlayerDataStatus.INVALID_PLAYER,
            status_message="Expected region/realm/name format.",
        )

    region, realm, name = match.groups()
    region_normalized = region.strip().lower()
    if region_normalized not in VALID_REGIONS:
        return RosterEntry(
            row_number=row_number,
            raw_value=raw_value,
            player_key=invalid_key,
            identity=None,
            is_valid=False,
            status=PlayerDataStatus.INVALID_PLAYER,
            status_message=f"Unsupported region '{region.strip()}'.",
        )

    player_key = build_player_key(region_normalized, realm, name)
    return RosterEntry(
        row_number=row_number,
        raw_value=raw_value,
        player_key=player_key,
        identity=PlayerIdentity(
            region=region_normalized,
            realm=normalize_realm(realm),
            name=name.strip(),
            player_key=player_key,
        ),
        is_valid=True,
        status=PlayerDataStatus.OK,
        status_message="",
    )


def parse_roster_rows(raw_rows: list[str], *, start_row: int) -> list[RosterEntry]:
    """Parse roster rows and mark duplicate valid players as invalid entries."""

    parsed = [
        parse_roster_value(start_row + index, value)
        for index, value in enumerate(raw_rows)
        if value is not None
    ]
    valid_counts = Counter(entry.player_key for entry in parsed if entry.is_valid)
    finalized: list[RosterEntry] = []

    seen_valid: set[str] = set()
    for entry in parsed:
        if not entry.is_valid:
            finalized.append(entry)
            continue

        assert entry.identity is not None
        if valid_counts[entry.player_key] > 1:
            if entry.player_key in seen_valid:
                duplicate_key = build_invalid_key(entry.row_number, entry.raw_value)
                finalized.append(
                    RosterEntry(
                        row_number=entry.row_number,
                        raw_value=entry.raw_value,
                        player_key=duplicate_key,
                        identity=None,
                        is_valid=False,
                        status=PlayerDataStatus.INVALID_PLAYER,
                        status_message="Duplicate roster entry.",
                    )
                )
                LOGGER.warning(
                    "Duplicate roster row ignored",
                    extra={"row_number": entry.row_number, "player_key": entry.player_key},
                )
                continue
            seen_valid.add(entry.player_key)

        finalized.append(entry)

    return finalized
