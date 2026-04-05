"""Play-time profile helpers for predictive hot polling."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from niru.models import PACIFIC_TZ, ensure_utc


PLAY_PROFILE_TIMEZONE = "America/Los_Angeles"
PLAY_PROFILE_HOURS_PER_WEEK = 168


def _to_pacific(value: datetime) -> datetime:
    return ensure_utc(value).astimezone(PACIFIC_TZ)


def pacific_week_start(value: datetime) -> datetime:
    """Return the start of the Pacific calendar week as a UTC timestamp."""

    pacific_value = _to_pacific(value)
    week_start_pacific = (
        pacific_value.replace(hour=0, minute=0, second=0, microsecond=0)
        - timedelta(days=pacific_value.weekday())
    )
    return week_start_pacific.astimezone(UTC)


def pacific_hour_start(value: datetime) -> datetime:
    """Return the start of the Pacific hour as a UTC timestamp."""

    return _to_pacific(value).replace(minute=0, second=0, microsecond=0).astimezone(UTC)


def pacific_week_hour_index(value: datetime) -> int:
    """Return the 0-167 Pacific week-hour index for a timestamp."""

    pacific_value = _to_pacific(value)
    return (pacific_value.weekday() * 24) + pacific_value.hour


def current_week_hour_key(value: datetime) -> str:
    """Return the unique key for the Pacific week-hour containing a timestamp."""

    week_start = pacific_week_start(value)
    index = pacific_week_hour_index(value)
    return f"{week_start.isoformat()}|{index}"


def next_pacific_hour_start(value: datetime) -> datetime:
    """Return the next Pacific hour boundary as a UTC timestamp."""

    return pacific_hour_start(value) + timedelta(hours=1)


def expected_weeks_observed(first_week_start_at: datetime | None, *, now: datetime) -> int:
    """Return how many Pacific calendar weeks are covered through now."""

    if first_week_start_at is None:
        return 0
    current_week_start = _to_pacific(pacific_week_start(now))
    first_week_start = _to_pacific(ensure_utc(first_week_start_at))
    return max(((current_week_start.date() - first_week_start.date()).days // 7) + 1, 0)


def _parse_seen_week_hour_key(key: str) -> tuple[datetime, int]:
    week_start_raw, index_raw = key.split("|", maxsplit=1)
    return ensure_utc(datetime.fromisoformat(week_start_raw)), int(index_raw)


def _normalize_profile(
    *,
    seen_week_hours: set[str],
    now: datetime,
    last_seeded_at: datetime | None,
    last_enqueued_week_hour: str,
) -> dict[str, Any]:
    counts = [0] * PLAY_PROFILE_HOURS_PER_WEEK
    first_week_start_at: datetime | None = None
    for key in seen_week_hours:
        week_start, hour_index = _parse_seen_week_hour_key(key)
        counts[hour_index] += 1
        if first_week_start_at is None or week_start < first_week_start_at:
            first_week_start_at = week_start

    weeks_observed = expected_weeks_observed(first_week_start_at, now=now)
    probabilities = [
        round(count / weeks_observed, 4) if weeks_observed > 0 else 0.0 for count in counts
    ]
    return {
        "play_profile_timezone": PLAY_PROFILE_TIMEZONE,
        "play_profile_first_week_start_at": first_week_start_at,
        "play_profile_last_seeded_at": ensure_utc(last_seeded_at) if last_seeded_at else None,
        "play_profile_weeks_observed": weeks_observed,
        "play_profile_hour_counts": counts,
        "play_profile_hour_probabilities": probabilities,
        "play_profile_seen_week_hours": sorted(seen_week_hours),
        "play_profile_last_enqueued_week_hour": last_enqueued_week_hour,
    }


def build_play_profile(
    *,
    completed_at_values: list[datetime],
    now: datetime,
    last_seeded_at: datetime | None = None,
    last_enqueued_week_hour: str = "",
) -> dict[str, Any]:
    """Build a play profile from raw run completion times."""

    seen_week_hours = {current_week_hour_key(completed_at) for completed_at in completed_at_values}
    return _normalize_profile(
        seen_week_hours=seen_week_hours,
        now=now,
        last_seeded_at=last_seeded_at,
        last_enqueued_week_hour=last_enqueued_week_hour,
    )


def update_play_profile(
    *,
    existing_profile: dict[str, Any],
    completed_at_values: list[datetime],
    now: datetime,
) -> dict[str, Any]:
    """Incrementally update a play profile using newly observed run times."""

    seen_week_hours = {
        str(key)
        for key in existing_profile.get("play_profile_seen_week_hours", []) or []
        if isinstance(key, str) and key
    }
    for completed_at in completed_at_values:
        seen_week_hours.add(current_week_hour_key(completed_at))
    return _normalize_profile(
        seen_week_hours=seen_week_hours,
        now=now,
        last_seeded_at=existing_profile.get("play_profile_last_seeded_at"),
        last_enqueued_week_hour=str(
            existing_profile.get("play_profile_last_enqueued_week_hour", "") or ""
        ),
    )
