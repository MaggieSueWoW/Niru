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
    active_interval_minutes: int
    active_idle_minutes: int
    predictive_hot_enabled: bool
    predictive_hot_threshold: float
    current_season: str | None
    max_players_per_cycle: int
    failure_backoff_seconds: float
    max_failure_backoff_seconds: float
    failure_backoff_jitter_seconds: float


@dataclass(slots=True, frozen=True)
class RaiderIOSettings:
    base_url: str
    access_key_enabled: bool
    access_key: str | None
    requests_per_minute_cap: int
    timeout_seconds: int
    retry_attempts: int
    backoff_seconds: float
    circuit_breaker_threshold: int
    circuit_breaker_cooldown_seconds: int


@dataclass(slots=True, frozen=True)
class BlizzardSettings:
    enabled: bool
    base_url: str
    oauth_url: str
    client_id: str | None
    client_secret: str | None
    requests_per_hour_cap: int
    requests_per_second_cap: int
    timeout_seconds: int
    retry_attempts: int
    backoff_seconds: float
    locale: str
    namespace_profile: str
    namespace_dynamic: str
    run_fingerprint_fuzz_seconds: int


@dataclass(slots=True, frozen=True)
class RedisSettings:
    url: str
    key_prefix: str


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
    blizzard: BlizzardSettings
    redis: RedisSettings
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


def _require_float_range(
    value: object,
    *,
    name: str,
    minimum: float,
    maximum: float,
) -> float:
    if not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number between {minimum} and {maximum}")
    numeric = float(value)
    if numeric < minimum or numeric > maximum:
        raise ValueError(f"{name} must be a number between {minimum} and {maximum}")
    return numeric


def _require_bool(value: object, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("optional text value must be a string when provided")
    stripped = value.strip()
    return stripped or None


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
    blizzard_raw = raw.get("blizzard", {})
    redis_raw = raw.get("redis", {})
    mongodb_raw = raw.get("mongodb", {})
    logging_raw = raw.get("logging", {})

    output_start_cell = _require_text(
        google_raw.get("output_start_cell"), name="google.output_start_cell"
    ).upper()
    if not CELL_RE.match(output_start_cell):
        raise ValueError("google.output_start_cell must be in A1 format")

    current_season = _optional_text(sync_raw.get("current_season"))
    blizzard_enabled = _require_bool(
        blizzard_raw.get("enabled", False),
        name="blizzard.enabled",
    )
    if not blizzard_enabled and current_season is None:
        raise ValueError("sync.current_season must be a non-empty string when blizzard.enabled is false")

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
            active_interval_minutes=_require_int(
                sync_raw.get("active_interval_minutes", 5),
                name="sync.active_interval_minutes",
            ),
            active_idle_minutes=_require_int(
                sync_raw.get("active_idle_minutes", 40),
                name="sync.active_idle_minutes",
            ),
            predictive_hot_enabled=_require_bool(
                sync_raw.get("predictive_hot_enabled", True),
                name="sync.predictive_hot_enabled",
            ),
            predictive_hot_threshold=_require_float_range(
                sync_raw.get("predictive_hot_threshold", 0.5),
                name="sync.predictive_hot_threshold",
                minimum=0.0,
                maximum=1.0,
            ),
            current_season=current_season,
            max_players_per_cycle=_require_int(
                sync_raw.get("max_players_per_cycle"),
                name="sync.max_players_per_cycle",
            ),
            failure_backoff_seconds=_require_float(
                sync_raw.get("failure_backoff_seconds", 30.0),
                name="sync.failure_backoff_seconds",
                minimum=0.1,
            ),
            max_failure_backoff_seconds=_require_float(
                sync_raw.get("max_failure_backoff_seconds", 900.0),
                name="sync.max_failure_backoff_seconds",
                minimum=0.1,
            ),
            failure_backoff_jitter_seconds=_require_float(
                sync_raw.get("failure_backoff_jitter_seconds", 5.0),
                name="sync.failure_backoff_jitter_seconds",
                minimum=0.0,
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
            circuit_breaker_threshold=_require_int(
                raiderio_raw.get("circuit_breaker_threshold", 3),
                name="raiderio.circuit_breaker_threshold",
            ),
            circuit_breaker_cooldown_seconds=_require_int(
                raiderio_raw.get("circuit_breaker_cooldown_seconds", 300),
                name="raiderio.circuit_breaker_cooldown_seconds",
            ),
        ),
        blizzard=BlizzardSettings(
            enabled=blizzard_enabled,
            base_url=_require_text(
                blizzard_raw.get("base_url", "https://us.api.blizzard.com"),
                name="blizzard.base_url",
            ),
            oauth_url=_require_text(
                blizzard_raw.get("oauth_url", "https://oauth.battle.net/token"),
                name="blizzard.oauth_url",
            ),
            client_id=os.getenv("BLIZZARD_CLIENT_ID") or None,
            client_secret=os.getenv("BLIZZARD_CLIENT_SECRET") or None,
            requests_per_hour_cap=_require_int(
                blizzard_raw.get("requests_per_hour_cap", 36_000),
                name="blizzard.requests_per_hour_cap",
            ),
            requests_per_second_cap=_require_int(
                blizzard_raw.get("requests_per_second_cap", 100),
                name="blizzard.requests_per_second_cap",
            ),
            timeout_seconds=_require_int(
                blizzard_raw.get("timeout_seconds", 30),
                name="blizzard.timeout_seconds",
            ),
            retry_attempts=_require_int(
                blizzard_raw.get("retry_attempts", 4),
                name="blizzard.retry_attempts",
            ),
            backoff_seconds=_require_float(
                blizzard_raw.get("backoff_seconds", 2.0),
                name="blizzard.backoff_seconds",
                minimum=0.1,
            ),
            locale=_require_text(
                blizzard_raw.get("locale", "en_US"),
                name="blizzard.locale",
            ),
            namespace_profile=_require_text(
                blizzard_raw.get("namespace_profile", "profile-us"),
                name="blizzard.namespace_profile",
            ),
            namespace_dynamic=_require_text(
                blizzard_raw.get("namespace_dynamic", "dynamic-us"),
                name="blizzard.namespace_dynamic",
            ),
            run_fingerprint_fuzz_seconds=_require_int(
                blizzard_raw.get("run_fingerprint_fuzz_seconds", 2),
                name="blizzard.run_fingerprint_fuzz_seconds",
            ),
        ),
        redis=RedisSettings(
            url=_require_text(os.getenv("REDIS_URL", "redis://localhost:6379/0"), name="REDIS_URL"),
            key_prefix=_require_text(
                redis_raw.get("key_prefix", "niru"),
                name="redis.key_prefix",
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
