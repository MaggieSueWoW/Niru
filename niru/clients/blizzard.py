"""Blizzard API client."""

from __future__ import annotations

from dataclasses import dataclass
from collections import deque
import json
import logging
import ssl
import threading
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from niru.config import BlizzardSettings
from niru.models import PlayerIdentity

LOGGER = logging.getLogger(__name__)


class BlizzardError(RuntimeError):
    """Generic Blizzard API failure."""


class BlizzardNotFoundError(BlizzardError):
    """Raised when Blizzard cannot resolve a player or endpoint."""


@dataclass(slots=True)
class BlizzardResult:
    """A wrapper for payload + request metadata."""

    payload: dict[str, Any]
    request_url: str


class BlizzardClient:
    """Tiny Blizzard API client with cached client-credentials auth."""

    TOKEN_GRACE_SECONDS = 60
    DEFAULT_HEADERS = {
        "Accept": "application/json",
        "User-Agent": "niru/0.1 (+https://blizzard.com)",
    }

    def __init__(self, settings: BlizzardSettings) -> None:
        self._settings = settings
        self.api_calls = 0
        self._ssl_context = self._build_ssl_context()
        self._access_token: str | None = None
        self._access_token_expires_at = 0.0
        self._rate_lock = threading.Lock()
        self._second_timestamps: deque[float] = deque()
        self._hour_timestamps: deque[float] = deque()

    def get_current_season_index(self) -> BlizzardResult:
        """Fetch the current season index."""

        return self._get_json(
            "/data/wow/mythic-keystone/season/index",
            namespace=self._settings.namespace_dynamic,
        )

    def get_season_detail(self, season_id: int) -> BlizzardResult:
        """Fetch one Mythic Keystone season."""

        return self._get_json(
            f"/data/wow/mythic-keystone/season/{int(season_id)}",
            namespace=self._settings.namespace_dynamic,
        )

    def get_character_mythic_keystone_profile(self, player: PlayerIdentity) -> BlizzardResult:
        """Fetch a character's current Mythic Keystone profile."""

        realm = quote(str(player.realm).lower(), safe="")
        name = quote(str(player.name).lower(), safe="")
        return self._get_json(
            (
                f"/profile/wow/character/{realm}/{name}"
                "/mythic-keystone-profile"
            ),
            namespace=self._settings.namespace_profile,
        )

    def get_character_mythic_keystone_profile_season(
        self, player: PlayerIdentity, season_id: int
    ) -> BlizzardResult:
        """Fetch a character's season Mythic Keystone profile."""

        realm = quote(str(player.realm).lower(), safe="")
        name = quote(str(player.name).lower(), safe="")
        return self._get_json(
            (
                f"/profile/wow/character/{realm}/{name}"
                f"/mythic-keystone-profile/season/{int(season_id)}"
            ),
            namespace=self._settings.namespace_profile,
        )

    def _get_json(self, path: str, *, namespace: str) -> BlizzardResult:
        token = self._get_access_token()
        params = {
            "namespace": namespace,
            "locale": self._settings.locale,
        }
        url = f"{self._settings.base_url}{path}?{urlencode(params)}"
        headers = dict(self.DEFAULT_HEADERS)
        headers["Authorization"] = f"Bearer {token}"

        for attempt in range(1, self._settings.retry_attempts + 1):
            self._acquire_request_slot()
            self.api_calls += 1
            request = Request(url, headers=headers)
            try:
                with urlopen(
                    request,
                    timeout=self._settings.timeout_seconds,
                    context=self._ssl_context,
                ) as response:
                    payload = json.load(response)
                    return BlizzardResult(payload=payload, request_url=url)
            except HTTPError as exc:
                status = exc.code
                response_snippet = exc.read(400).decode("utf-8", errors="replace").strip()
                if status == 404:
                    raise BlizzardNotFoundError(f"Not found: {url}") from exc
                if status in {429, 500, 502, 503, 504} and attempt < self._settings.retry_attempts:
                    wait = self._settings.backoff_seconds * attempt
                    LOGGER.warning(
                        "Retrying Blizzard request after HTTP error %s for %s (attempt %s/%s, sleep %.1fs)",
                        status,
                        url,
                        attempt,
                        self._settings.retry_attempts,
                        wait,
                    )
                    time.sleep(wait)
                    continue
                detail = f"HTTP {status} from Blizzard for {url}"
                if response_snippet:
                    detail = f"{detail}. Response: {response_snippet}"
                raise BlizzardError(detail) from exc
            except URLError as exc:
                if isinstance(exc.reason, ssl.SSLCertVerificationError):
                    raise BlizzardError(
                        "SSL certificate verification failed for Blizzard. "
                        "Install project dependencies so certifi is available, or update "
                        f"the system CA bundle. Underlying error: {exc.reason}"
                    ) from exc
                if isinstance(exc.reason, ssl.SSLError):
                    raise BlizzardError(f"SSL error calling Blizzard for {url}: {exc.reason}") from exc
                if attempt < self._settings.retry_attempts:
                    wait = self._settings.backoff_seconds * attempt
                    LOGGER.warning(
                        "Retrying Blizzard request after network error for %s (attempt %s/%s, sleep %.1fs): %s",
                        url,
                        attempt,
                        self._settings.retry_attempts,
                        wait,
                        exc,
                    )
                    time.sleep(wait)
                    continue
                raise BlizzardError(f"Network error calling Blizzard for {url}") from exc

        raise BlizzardError(f"Failed to fetch Blizzard URL: {url}")

    def _get_access_token(self) -> str:
        if not self._settings.enabled:
            raise BlizzardError("Blizzard API is disabled")
        client_id = self._settings.client_id
        client_secret = self._settings.client_secret
        if not client_id or not client_secret:
            raise BlizzardError(
                "Blizzard API credentials are missing. Set BLIZZARD_CLIENT_ID and BLIZZARD_CLIENT_SECRET."
            )

        now = time.time()
        if self._access_token and now < self._access_token_expires_at:
            return self._access_token

        payload = urlencode({"grant_type": "client_credentials"}).encode("utf-8")
        request = Request(
            self._settings.oauth_url,
            data=payload,
            headers={
                "Authorization": _build_basic_auth_header(client_id, client_secret),
                "Content-Type": "application/x-www-form-urlencoded",
                **self.DEFAULT_HEADERS,
            },
            method="POST",
        )
        self._acquire_request_slot()
        self.api_calls += 1
        try:
            with urlopen(
                request,
                timeout=self._settings.timeout_seconds,
                context=self._ssl_context,
            ) as response:
                token_payload = json.load(response)
        except HTTPError as exc:
            snippet = exc.read(400).decode("utf-8", errors="replace").strip()
            message = f"Failed to fetch Blizzard auth token: HTTP {exc.code}"
            if snippet:
                message = f"{message}. Response: {snippet}"
            raise BlizzardError(message) from exc
        except URLError as exc:
            raise BlizzardError("Network error fetching Blizzard auth token") from exc

        access_token = token_payload.get("access_token")
        expires_in = int(token_payload.get("expires_in") or 0)
        if not access_token or expires_in <= 0:
            raise BlizzardError("Blizzard auth response did not contain a usable access token")
        self._access_token = str(access_token)
        self._access_token_expires_at = (
            time.time() + max(expires_in - self.TOKEN_GRACE_SECONDS, 1)
        )
        return self._access_token

    def _acquire_request_slot(self) -> None:
        """Block until both Blizzard rate-limit windows have capacity."""

        per_second = self._settings.requests_per_second_cap
        per_hour = self._settings.requests_per_hour_cap
        if per_second <= 0 or per_hour <= 0:
            raise BlizzardError("Blizzard rate-limit caps must be positive")

        while True:
            with self._rate_lock:
                now = time.time()
                second_cutoff = now - 1.0
                hour_cutoff = now - 3600.0
                while self._second_timestamps and self._second_timestamps[0] <= second_cutoff:
                    self._second_timestamps.popleft()
                while self._hour_timestamps and self._hour_timestamps[0] <= hour_cutoff:
                    self._hour_timestamps.popleft()

                if len(self._second_timestamps) < per_second and len(self._hour_timestamps) < per_hour:
                    self._second_timestamps.append(now)
                    self._hour_timestamps.append(now)
                    return

                sleep_for = 0.01
                if len(self._second_timestamps) >= per_second and self._second_timestamps:
                    sleep_for = max(sleep_for, 1.0 - (now - self._second_timestamps[0]))
                if len(self._hour_timestamps) >= per_hour and self._hour_timestamps:
                    sleep_for = max(sleep_for, 3600.0 - (now - self._hour_timestamps[0]))
            time.sleep(sleep_for)

    @staticmethod
    def _build_ssl_context() -> ssl.SSLContext:
        try:
            import certifi
        except ImportError:
            return ssl.create_default_context()
        return ssl.create_default_context(cafile=certifi.where())


def _build_basic_auth_header(client_id: str, client_secret: str) -> str:
    from base64 import b64encode

    token = b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"
