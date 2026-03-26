"""Configuration loading."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import re


CELL_RE = re.compile(r"^[A-Z]+[1-9][0-9]*$")


@dataclass(slots=True, frozen=True)
class GoogleSettings:
    sheet_id: str
    raw_tab_name: str
    roster_column: str
    roster_start_row: int
    output_start_cell: str
    service_account_file: str | None
    service_account_json: str | None


@dataclass(slots=True, frozen=True)
class SyncSettings:
    interval_minutes: int
    current_season: str
    max_players_per_cycle: int
    gap_detection_cycles: int


@dataclass(slots=True, frozen=True)
class RaiderIOSettings:
    base_url: str
    access_key_enabled: bool
    access_key: str | None
    requests_per_minute_cap: int
    timeout_seconds: int
    retry_attempts: int
    backoff_seconds: float


@dataclass(slots=True, frozen=True)
class MongoSettings:
    database: str
    players_collection: str
    runs_collection: str
    sync_cycles_collection: str
    uri: str


@dataclass(slots=True, frozen=True)
class LoggingSettings:
    level: str


@dataclass(slots=True, frozen=True)
class Settings:
    google: GoogleSettings
    sync: SyncSettings
    raiderio: RaiderIOSettings
    mongodb: MongoSettings
    logging: LoggingSettings


def _require_text(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def _require_int(value: object, *, name: str, minimum: int = 1) -> int:
    if not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _require_float(value: object, *, name: str, minimum: float = 0.0) -> float:
    if not isinstance(value, (int, float)) or float(value) < minimum:
        raise ValueError(f"{name} must be a number >= {minimum}")
    return float(value)


def load_settings(config_path: str = "config.yaml") -> Settings:
    """Load YAML config plus environment overrides."""

    from dotenv import load_dotenv
    import yaml

    load_dotenv()
    path = Path(config_path)
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    google_raw = raw.get("google", {})
    sync_raw = raw.get("sync", {})
    raiderio_raw = raw.get("raiderio", {})
    mongodb_raw = raw.get("mongodb", {})
    logging_raw = raw.get("logging", {})

    output_start_cell = _require_text(
        google_raw.get("output_start_cell"), name="google.output_start_cell"
    ).upper()
    if not CELL_RE.match(output_start_cell):
        raise ValueError("google.output_start_cell must be in A1 format")

    return Settings(
        google=GoogleSettings(
            sheet_id=_require_text(os.getenv("GOOGLE_SHEET_ID"), name="GOOGLE_SHEET_ID"),
            raw_tab_name=_require_text(
                google_raw.get("raw_tab_name"), name="google.raw_tab_name"
            ),
            roster_column=_require_text(
                google_raw.get("roster_column"), name="google.roster_column"
            ).upper(),
            roster_start_row=_require_int(
                google_raw.get("roster_start_row"), name="google.roster_start_row"
            ),
            output_start_cell=output_start_cell,
            service_account_file=os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE"),
            service_account_json=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"),
        ),
        sync=SyncSettings(
            interval_minutes=_require_int(
                sync_raw.get("interval_minutes"), name="sync.interval_minutes"
            ),
            current_season=_require_text(
                sync_raw.get("current_season"), name="sync.current_season"
            ),
            max_players_per_cycle=_require_int(
                sync_raw.get("max_players_per_cycle"),
                name="sync.max_players_per_cycle",
            ),
            gap_detection_cycles=_require_int(
                sync_raw.get("gap_detection_cycles"),
                name="sync.gap_detection_cycles",
            ),
        ),
        raiderio=RaiderIOSettings(
            base_url=_require_text(raiderio_raw.get("base_url"), name="raiderio.base_url"),
            access_key_enabled=bool(raiderio_raw.get("access_key_enabled", False)),
            access_key=os.getenv("RAIDERIO_ACCESS_KEY") or None,
            requests_per_minute_cap=_require_int(
                raiderio_raw.get("requests_per_minute_cap"),
                name="raiderio.requests_per_minute_cap",
            ),
            timeout_seconds=_require_int(
                raiderio_raw.get("timeout_seconds"), name="raiderio.timeout_seconds"
            ),
            retry_attempts=_require_int(
                raiderio_raw.get("retry_attempts"), name="raiderio.retry_attempts"
            ),
            backoff_seconds=_require_float(
                raiderio_raw.get("backoff_seconds"),
                name="raiderio.backoff_seconds",
                minimum=0.1,
            ),
        ),
        mongodb=MongoSettings(
            database=_require_text(mongodb_raw.get("database"), name="mongodb.database"),
            players_collection=_require_text(
                mongodb_raw.get("players_collection"),
                name="mongodb.players_collection",
            ),
            runs_collection=_require_text(
                mongodb_raw.get("runs_collection"), name="mongodb.runs_collection"
            ),
            sync_cycles_collection=_require_text(
                mongodb_raw.get("sync_cycles_collection"),
                name="mongodb.sync_cycles_collection",
            ),
            uri=_require_text(os.getenv("MONGODB_URI"), name="MONGODB_URI"),
        ),
        logging=LoggingSettings(
            level=_require_text(logging_raw.get("level", "INFO"), name="logging.level")
        ),
    )
