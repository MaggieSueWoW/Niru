"""Configuration loading."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import os
import re


CELL_RE = re.compile(r"^[A-Z]+[1-9][0-9]*$")
SEASON_SLUG_RE = re.compile(r"^season-[a-z0-9-]+$")


@dataclass(slots=True, frozen=True)
class SeasonTabSettings:
    """One scheduled season and its Google Sheets destination."""

    slug: str
    tab_name: str
    activates_at: datetime
    blizzard_season_id: int | None


@dataclass(slots=True, frozen=True)
class GoogleSettings:
    sheet_id: str
    season_tabs: tuple[SeasonTabSettings, ...]
    roster_column: str
    roster_start_row: int
    output_start_cell: str
    team_activity_output_start_cell: str
    service_account_file: str | None
    service_account_json: str | None


@dataclass(slots=True, frozen=True)
class TeamActivitySettings:
    enabled: bool
    window_weeks: int
    start_hour: int


@dataclass(slots=True, frozen=True)
class PredictedHourPollingSettings:
    enabled: bool
    probability_threshold: float


@dataclass(slots=True, frozen=True)
class CompletionFollowUpPollingSettings:
    enabled: bool
    start_after_completion_minutes: int
    stop_after_completion_minutes: int


@dataclass(slots=True, frozen=True)
class SyncSettings:
    baseline_poll_interval_minutes: int
    accelerated_poll_interval_minutes: int
    predicted_hour_polling: PredictedHourPollingSettings
    completion_follow_up_polling: CompletionFollowUpPollingSettings
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
    season_rosters_collection: str
    uri: str


@dataclass(slots=True, frozen=True)
class LoggingSettings:
    level: str


@dataclass(slots=True, frozen=True)
class Settings:
    google: GoogleSettings
    sync: SyncSettings
    team_activity: TeamActivitySettings
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


def _require_datetime(value: object, *, name: str) -> datetime:
    text = _require_text(value, name=name)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.astimezone(UTC)


def _load_season_tabs(raw: object) -> tuple[SeasonTabSettings, ...]:
    if not isinstance(raw, dict) or not raw:
        raise ValueError("google.season_tabs must be a non-empty mapping")

    season_tabs: list[SeasonTabSettings] = []
    tab_names: set[str] = set()
    activation_times: set[datetime] = set()
    for raw_slug, raw_settings in raw.items():
        slug = _require_text(raw_slug, name="google.season_tabs key")
        if not SEASON_SLUG_RE.match(slug):
            raise ValueError(f"Invalid season slug in google.season_tabs: {slug}")
        if not isinstance(raw_settings, dict):
            raise ValueError(f"google.season_tabs.{slug} must be a mapping")
        tab_name = _require_text(
            raw_settings.get("tab_name"),
            name=f"google.season_tabs.{slug}.tab_name",
        )
        activates_at = _require_datetime(
            raw_settings.get("activates_at"),
            name=f"google.season_tabs.{slug}.activates_at",
        )
        raw_blizzard_season_id = raw_settings.get("blizzard_season_id")
        blizzard_season_id = (
            None
            if raw_blizzard_season_id is None
            else _require_int(
                raw_blizzard_season_id,
                name=f"google.season_tabs.{slug}.blizzard_season_id",
            )
        )
        if tab_name in tab_names:
            raise ValueError(f"Duplicate season tab name: {tab_name}")
        if activates_at in activation_times:
            raise ValueError(
                f"Duplicate season activation time: {activates_at.isoformat()}"
            )
        tab_names.add(tab_name)
        activation_times.add(activates_at)
        season_tabs.append(
            SeasonTabSettings(
                slug=slug,
                tab_name=tab_name,
                activates_at=activates_at,
                blizzard_season_id=blizzard_season_id,
            )
        )
    return tuple(sorted(season_tabs, key=lambda season: season.activates_at))


def resolve_active_season(
    season_tabs: tuple[SeasonTabSettings, ...],
    *,
    now: datetime,
) -> SeasonTabSettings:
    """Return the configured season active at ``now``."""

    current_time = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    current_time = current_time.astimezone(UTC)
    active = [season for season in season_tabs if season.activates_at <= current_time]
    if not active:
        first = min(season_tabs, key=lambda season: season.activates_at)
        raise ValueError(
            "No configured season is active yet; first activation is "
            f"{first.activates_at.isoformat()}"
        )
    return max(active, key=lambda season: season.activates_at)


def next_season_transition(
    season_tabs: tuple[SeasonTabSettings, ...],
    *,
    now: datetime,
) -> datetime | None:
    """Return the next configured season activation after ``now``."""

    current_time = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    current_time = current_time.astimezone(UTC)
    future = [
        season.activates_at
        for season in season_tabs
        if season.activates_at > current_time
    ]
    return min(future) if future else None


def load_settings(config_path: str = "config.yaml") -> Settings:
    """Load YAML config plus environment overrides."""

    from dotenv import load_dotenv
    import yaml

    load_dotenv()
    path = Path(config_path)
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    google_raw = raw.get("google", {})
    sync_raw = raw.get("sync", {})
    team_activity_raw = raw.get("team_activity", {})
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
    team_activity_output_start_cell = _require_text(
        team_activity_raw.get("output_start_cell", "C101"),
        name="team_activity.output_start_cell",
    ).upper()
    if not CELL_RE.match(team_activity_output_start_cell):
        raise ValueError("team_activity.output_start_cell must be in A1 format")
    team_activity_start_hour = _require_int(
        team_activity_raw.get("start_hour", 7),
        name="team_activity.start_hour",
        minimum=0,
    )
    if team_activity_start_hour > 23:
        raise ValueError("team_activity.start_hour must be an integer between 0 and 23")

    season_tabs = _load_season_tabs(google_raw.get("season_tabs"))
    blizzard_enabled = _require_bool(
        blizzard_raw.get("enabled", False),
        name="blizzard.enabled",
    )
    if blizzard_enabled:
        missing_blizzard_ids = [
            season.slug for season in season_tabs if season.blizzard_season_id is None
        ]
        if missing_blizzard_ids:
            raise ValueError(
                "blizzard_season_id is required for configured seasons when Blizzard is enabled: "
                + ", ".join(missing_blizzard_ids)
            )

    baseline_poll_interval_minutes = _require_int(
        sync_raw.get("baseline_poll_interval_minutes"),
        name="sync.baseline_poll_interval_minutes",
    )
    accelerated_poll_interval_minutes = _require_int(
        sync_raw.get("accelerated_poll_interval_minutes", 5),
        name="sync.accelerated_poll_interval_minutes",
    )
    if baseline_poll_interval_minutes % accelerated_poll_interval_minutes != 0:
        raise ValueError(
            "sync.baseline_poll_interval_minutes must be divisible by "
            "sync.accelerated_poll_interval_minutes"
        )

    predicted_hour_raw = sync_raw.get("predicted_hour_polling", {})
    if not isinstance(predicted_hour_raw, dict):
        raise ValueError("sync.predicted_hour_polling must be a mapping")
    completion_follow_up_raw = sync_raw.get("completion_follow_up_polling", {})
    if not isinstance(completion_follow_up_raw, dict):
        raise ValueError("sync.completion_follow_up_polling must be a mapping")
    completion_follow_up_start = _require_int(
        completion_follow_up_raw.get("start_after_completion_minutes", 30),
        name="sync.completion_follow_up_polling.start_after_completion_minutes",
    )
    completion_follow_up_stop = _require_int(
        completion_follow_up_raw.get("stop_after_completion_minutes", 50),
        name="sync.completion_follow_up_polling.stop_after_completion_minutes",
    )
    if completion_follow_up_stop <= completion_follow_up_start:
        raise ValueError(
            "sync.completion_follow_up_polling.stop_after_completion_minutes "
            "must be greater than start_after_completion_minutes"
        )

    return Settings(
        google=GoogleSettings(
            sheet_id=_require_text(
                os.getenv("GOOGLE_SHEET_ID"), name="GOOGLE_SHEET_ID"
            ),
            season_tabs=season_tabs,
            roster_column=_require_text(
                google_raw.get("roster_column"), name="google.roster_column"
            ).upper(),
            roster_start_row=_require_int(
                google_raw.get("roster_start_row"), name="google.roster_start_row"
            ),
            output_start_cell=output_start_cell,
            team_activity_output_start_cell=team_activity_output_start_cell,
            service_account_file=os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE"),
            service_account_json=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"),
        ),
        sync=SyncSettings(
            baseline_poll_interval_minutes=baseline_poll_interval_minutes,
            accelerated_poll_interval_minutes=accelerated_poll_interval_minutes,
            predicted_hour_polling=PredictedHourPollingSettings(
                enabled=_require_bool(
                    predicted_hour_raw.get("enabled", True),
                    name="sync.predicted_hour_polling.enabled",
                ),
                probability_threshold=_require_float_range(
                    predicted_hour_raw.get("probability_threshold", 0.5),
                    name="sync.predicted_hour_polling.probability_threshold",
                    minimum=0.0,
                    maximum=1.0,
                ),
            ),
            completion_follow_up_polling=CompletionFollowUpPollingSettings(
                enabled=_require_bool(
                    completion_follow_up_raw.get("enabled", True),
                    name="sync.completion_follow_up_polling.enabled",
                ),
                start_after_completion_minutes=completion_follow_up_start,
                stop_after_completion_minutes=completion_follow_up_stop,
            ),
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
        team_activity=TeamActivitySettings(
            enabled=_require_bool(
                team_activity_raw.get("enabled", True),
                name="team_activity.enabled",
            ),
            window_weeks=_require_int(
                team_activity_raw.get("window_weeks", 2),
                name="team_activity.window_weeks",
            ),
            start_hour=team_activity_start_hour,
        ),
        raiderio=RaiderIOSettings(
            base_url=_require_text(
                raiderio_raw.get("base_url"), name="raiderio.base_url"
            ),
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
            url=_require_text(
                os.getenv("REDIS_URL", "redis://localhost:6379/0"), name="REDIS_URL"
            ),
            key_prefix=_require_text(
                redis_raw.get("key_prefix", "niru"),
                name="redis.key_prefix",
            ),
        ),
        mongodb=MongoSettings(
            database=_require_text(
                mongodb_raw.get("database"), name="mongodb.database"
            ),
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
            season_rosters_collection=_require_text(
                mongodb_raw.get("season_rosters_collection", "season_rosters"),
                name="mongodb.season_rosters_collection",
            ),
            uri=_require_text(os.getenv("MONGODB_URI"), name="MONGODB_URI"),
        ),
        logging=LoggingSettings(
            level=_require_text(logging_raw.get("level", "INFO"), name="logging.level")
        ),
    )
