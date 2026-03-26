"""Core data models used by the bot."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo


class PlayerDataStatus(StrEnum):
    """Allowed sheet-facing player statuses."""

    OK = "ok"
    INVALID_PLAYER = "invalid_player"
    PARTIAL_GAP = "partial_gap"
    SYNC_ERROR = "sync_error"


PACIFIC_TZ = ZoneInfo("America/Los_Angeles")


def utc_now() -> datetime:
    """Return the current UTC timestamp."""

    return datetime.now(UTC)


def ensure_utc(value: datetime) -> datetime:
    """Normalize a datetime into an aware UTC value."""

    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@dataclass(slots=True, frozen=True)
class PlayerIdentity:
    """Canonical character identity."""

    region: str
    realm: str
    name: str
    player_key: str


@dataclass(slots=True, frozen=True)
class RosterEntry:
    """One roster row from Google Sheets."""

    row_number: int
    raw_value: str
    player_key: str
    identity: PlayerIdentity | None
    is_valid: bool
    status: PlayerDataStatus
    status_message: str


@dataclass(slots=True, frozen=True)
class SeasonDungeon:
    """Static metadata for one dungeon in a specific season."""

    season: str
    slug: str
    name: str
    short_name: str
    challenge_mode_id: int | None = None
    keystone_timer_seconds: int | None = None
    icon_url: str = ""
    background_image_url: str = ""


@dataclass(slots=True)
class SummaryRow:
    """One player row written to the summary table."""

    values: list[object]

    def to_sheet_row(self) -> list[object]:
        """Convert the row into a Sheets-compatible string list."""

        return self.values


def format_pacific_time(value: datetime | None) -> str:
    """Format a datetime in Pacific time for sheet output."""

    if value is None:
        return ""
    return ensure_utc(value).astimezone(PACIFIC_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


@dataclass(slots=True)
class SyncStats:
    """Per-cycle counters recorded in logs and MongoDB."""

    roster_rows: int = 0
    active_players: int = 0
    valid_players: int = 0
    invalid_players: int = 0
    api_calls: int = 0
    new_runs: int = 0
    detail_fetches: int = 0
    sheet_rows_written: int = 0
    warnings: list[str] = field(default_factory=list)
    partial: bool = False

    def to_document(self, *, started_at: datetime, finished_at: datetime) -> dict[str, Any]:
        """Serialize stats for MongoDB."""

        return {
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
            "roster_rows": self.roster_rows,
            "active_players": self.active_players,
            "valid_players": self.valid_players,
            "invalid_players": self.invalid_players,
            "api_calls": self.api_calls,
            "new_runs": self.new_runs,
            "detail_fetches": self.detail_fetches,
            "sheet_rows_written": self.sheet_rows_written,
            "warnings": self.warnings,
            "partial": self.partial,
        }
