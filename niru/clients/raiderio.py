"""Raider.IO API client."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import ssl
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from niru.config import RaiderIOSettings
from niru.control_state import RedisControlState
from niru.models import PlayerIdentity

LOGGER = logging.getLogger(__name__)


class RaiderIOError(RuntimeError):
    """Generic Raider.IO client failure."""


class RaiderIONotFoundError(RaiderIOError):
    """Raised when Raider.IO cannot resolve a player or run."""


class RaiderIOCooldownError(RaiderIOError):
    """Raised when Raider.IO calls are paused by a circuit breaker."""


@dataclass(slots=True)
class RaiderIOResult:
    """A wrapper for payload + request metadata."""

    payload: dict[str, Any]
    request_url: str


class RaiderIOClient:
    """Tiny Raider.IO client with rate limiting and retries."""

    DEFAULT_HEADERS = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (compatible; niru/0.1; +https://raider.io)",
    }
    PROFILE_FIELDS = ",".join(
        [
            "mythic_plus_scores_by_season:current",
            "mythic_plus_recent_runs",
            "mythic_plus_best_runs:all",
            "mythic_plus_alternate_runs:all",
        ]
    )

    def __init__(self, settings: RaiderIOSettings, *, control_state: RedisControlState) -> None:
        self._settings = settings
        self._control_state = control_state
        self.api_calls = 0
        self._ssl_context = self._build_ssl_context()

    def get_cooldown_remaining_seconds(self) -> float:
        """Return the current Raider.IO cooldown, if any."""

        return self._control_state.get_cooldown_remaining_seconds()

    def get_cooldown_reason(self) -> str:
        """Return the current Raider.IO cooldown reason, if any."""

        return self._control_state.get_cooldown_reason()

    def get_character_profile(self, player: PlayerIdentity) -> RaiderIOResult:
        """Fetch a character profile with Mythic+ fields."""

        return self._get_json(
            "/characters/profile",
            {
                "region": player.region,
                "realm": player.realm,
                "name": player.name,
                "fields": self.PROFILE_FIELDS,
            },
        )

    def get_run_details(self, *, season: str, run_id: int) -> RaiderIOResult:
        """Fetch detailed run information."""

        return self._get_json("/mythic-plus/run-details", {"season": season, "id": run_id})

    def get_mythic_plus_static_data(self, *, expansion_id: int) -> RaiderIOResult:
        """Fetch static Mythic+ season and dungeon metadata for an expansion."""

        return self._get_json("/mythic-plus/static-data", {"expansion_id": expansion_id})

    def get_periods(self) -> RaiderIOResult:
        """Fetch current, previous, and next weekly period windows by region."""

        return self._get_json("/periods", {})

    def _get_json(self, path: str, params: dict[str, Any]) -> RaiderIOResult:
        query = dict(params)
        if self._settings.access_key_enabled and self._settings.access_key:
            query["access_key"] = self._settings.access_key
        encoded = urlencode(query)
        url = f"{self._settings.base_url}{path}?{encoded}"

        cooldown_remaining = self._control_state.get_cooldown_remaining_seconds()
        if cooldown_remaining > 0:
            reason = self._control_state.get_cooldown_reason() or "Raider.IO cooldown active"
            raise RaiderIOCooldownError(
                f"{reason}. Retrying after {round(cooldown_remaining, 1)}s."
            )

        for attempt in range(1, self._settings.retry_attempts + 1):
            self._control_state.acquire_request_slot(
                requests_per_minute=self._settings.requests_per_minute_cap
            )
            self.api_calls += 1
            request = Request(url, headers=self.DEFAULT_HEADERS)
            try:
                with urlopen(
                    request,
                    timeout=self._settings.timeout_seconds,
                    context=self._ssl_context,
                ) as response:
                    payload = json.load(response)
                    self._control_state.clear_upstream_failure_streak()
                    return RaiderIOResult(payload=payload, request_url=url)
            except HTTPError as exc:
                status = exc.code
                response_snippet = exc.read(400).decode("utf-8", errors="replace").strip()
                if status == 404:
                    raise RaiderIONotFoundError(f"Not found: {url}") from exc
                if status == 429:
                    self._control_state.open_cooldown(
                        seconds=self._settings.circuit_breaker_cooldown_seconds,
                        reason="Raider.IO rate limit hit",
                    )
                if status in {429, 500, 502, 503, 504} and attempt < self._settings.retry_attempts:
                    wait = self._settings.backoff_seconds * attempt
                    LOGGER.warning(
                        "Retrying Raider.IO request after HTTP error %s for %s (attempt %s/%s, sleep %.1fs)",
                        status,
                        url,
                        attempt,
                        self._settings.retry_attempts,
                        wait,
                    )
                    time.sleep(wait)
                    continue
                if status in {500, 502, 503, 504}:
                    streak = self._control_state.increment_upstream_failure_streak(
                        ttl_seconds=self._settings.circuit_breaker_cooldown_seconds
                    )
                    if streak >= self._settings.circuit_breaker_threshold:
                        self._control_state.open_cooldown(
                            seconds=self._settings.circuit_breaker_cooldown_seconds,
                            reason=f"Raider.IO upstream errors reached streak {streak}",
                        )
                detail = f"HTTP {status} from Raider.IO for {url}"
                if response_snippet:
                    detail = f"{detail}. Response: {response_snippet}"
                raise RaiderIOError(detail) from exc
            except URLError as exc:
                if isinstance(exc.reason, ssl.SSLCertVerificationError):
                    raise RaiderIOError(
                        "SSL certificate verification failed for Raider.IO. "
                        "Install the project dependencies so certifi is available, or update "
                        f"the system CA bundle. Underlying error: {exc.reason}"
                    ) from exc
                if isinstance(exc.reason, ssl.SSLError):
                    raise RaiderIOError(
                        f"SSL error calling Raider.IO for {url}: {exc.reason}"
                    ) from exc
                if attempt < self._settings.retry_attempts:
                    wait = self._settings.backoff_seconds * attempt
                    LOGGER.warning(
                        "Retrying Raider.IO request after network error for %s (attempt %s/%s, sleep %.1fs): %s",
                        url,
                        attempt,
                        self._settings.retry_attempts,
                        wait,
                        exc,
                    )
                    time.sleep(wait)
                    continue
                streak = self._control_state.increment_upstream_failure_streak(
                    ttl_seconds=self._settings.circuit_breaker_cooldown_seconds
                )
                if streak >= self._settings.circuit_breaker_threshold:
                    self._control_state.open_cooldown(
                        seconds=self._settings.circuit_breaker_cooldown_seconds,
                        reason=f"Raider.IO network errors reached streak {streak}",
                    )
                raise RaiderIOError(f"Network error calling Raider.IO for {url}") from exc

        raise RaiderIOError(f"Failed to fetch Raider.IO URL: {url}")

    @staticmethod
    def _build_ssl_context() -> ssl.SSLContext:
        """Build an SSL context using certifi when available."""

        try:
            import certifi
        except ImportError:
            return ssl.create_default_context()
        return ssl.create_default_context(cafile=certifi.where())
