"""Raider.IO website/internal JSON client used for manual backfill discovery."""

from __future__ import annotations

from dataclasses import replace
from typing import Any
from urllib.parse import quote

from niru.clients.raiderio import RaiderIOClient, RaiderIOError, RaiderIOResult
from niru.config import RaiderIOSettings
from niru.control_state import RedisControlState
from niru.models import PlayerIdentity


class RaiderIOInternalClient(RaiderIOClient):
    """Small client for website-facing JSON endpoints used by manual backfill."""

    DEFAULT_REQUESTS_PER_MINUTE = 30

    def __init__(self, settings: RaiderIOSettings, *, control_state: RedisControlState) -> None:
        website_settings = replace(
            settings,
            base_url=self._website_base_url(settings.base_url),
            requests_per_minute_cap=self.DEFAULT_REQUESTS_PER_MINUTE,
        )
        super().__init__(website_settings, control_state=control_state)

    def get_character_page(
        self,
        player: PlayerIdentity,
        *,
        season: str,
        tier: int = 35,
    ) -> RaiderIOResult:
        """Fetch the Raider.IO character page JSON payload."""

        path = "/characters/{region}/{realm}/{name}".format(
            region=quote(player.region, safe=""),
            realm=quote(player.realm, safe=""),
            name=quote(player.name, safe=""),
        )
        return self._get_json(path, {"season": season, "tier": tier})

    def get_character_dungeon_runs(
        self,
        *,
        season: str,
        character_id: int,
        dungeon_id: int,
    ) -> RaiderIOResult:
        """Fetch all scored runs for one character and dungeon."""

        return self._get_json(
            "/characters/mythic-plus-runs",
            {
                "season": season,
                "characterId": character_id,
                "dungeonId": dungeon_id,
                "role": "all",
                "specId": 0,
                "mode": "scored",
                "affixes": "all",
                "date": "all",
            },
        )

    @staticmethod
    def extract_character_id(payload: dict[str, Any]) -> int:
        """Extract a Raider.IO character ID from a character page payload."""

        possible_values = [
            ((payload.get("characterDetails") or {}).get("character") or {}).get("id"),
            (payload.get("ui") or {}).get("characterId"),
            ((payload.get("characterMythicPlusProgress") or {}).get("ui") or {}).get(
                "characterId"
            ),
        ]
        for value in possible_values:
            if value is not None:
                return int(value)
        raise RaiderIOError("Raider.IO character page response did not include characterId")

    @staticmethod
    def _website_base_url(base_url: str) -> str:
        """Convert a configured public API base into the website JSON base."""

        normalized = base_url.rstrip("/")
        if normalized.endswith("/api/v1"):
            return normalized.removesuffix("/v1")
        return normalized
