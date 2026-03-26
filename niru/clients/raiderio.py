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
from niru.models import PlayerIdentity
from niru.rate_limit import RateLimiter

LOGGER = logging.getLogger(__name__)


class RaiderIOError(RuntimeError):
    """Generic Raider.IO client failure."""


class RaiderIONotFoundError(RaiderIOError):
    """Raised when Raider.IO cannot resolve a player or run."""


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

    def __init__(self, settings: RaiderIOSettings) -> None:
        self._settings = settings
        self._rate_limiter = RateLimiter(settings.requests_per_minute_cap)
        self.api_calls = 0
        self._ssl_context = self._build_ssl_context()

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

    def _get_json(self, path: str, params: dict[str, Any]) -> RaiderIOResult:
        query = dict(params)
        if self._settings.access_key_enabled and self._settings.access_key:
            query["access_key"] = self._settings.access_key
        encoded = urlencode(query)
        url = f"{self._settings.base_url}{path}?{encoded}"

        for attempt in range(1, self._settings.retry_attempts + 1):
            self._rate_limiter.acquire()
            self.api_calls += 1
            request = Request(url, headers=self.DEFAULT_HEADERS)
            try:
                with urlopen(
                    request,
                    timeout=self._settings.timeout_seconds,
                    context=self._ssl_context,
                ) as response:
                    payload = json.load(response)
                    return RaiderIOResult(payload=payload, request_url=url)
            except HTTPError as exc:
                status = exc.code
                response_snippet = exc.read(400).decode("utf-8", errors="replace").strip()
                if status == 404:
                    raise RaiderIONotFoundError(f"Not found: {url}") from exc
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
