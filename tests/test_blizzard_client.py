import unittest
from unittest.mock import patch

from niru.clients.blizzard import BlizzardClient
from niru.models import PlayerIdentity


class BlizzardClientPathTests(unittest.TestCase):
    def test_character_profile_uses_lowercase_realm_and_name(self) -> None:
        settings = type(
            "BlizzardSettings",
            (),
            {
                "enabled": True,
                "base_url": "https://us.api.blizzard.com",
                "oauth_url": "https://oauth.battle.net/token",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "timeout_seconds": 30,
                "retry_attempts": 1,
                "backoff_seconds": 1.0,
                "locale": "en_US",
                "namespace_profile": "profile-us",
                "namespace_dynamic": "dynamic-us",
            },
        )()

        class RecordingBlizzardClient(BlizzardClient):
            def __init__(self, local_settings):
                super().__init__(local_settings)
                self.paths: list[str] = []

            def _get_access_token(self) -> str:
                return "token"

            def _get_json(self, path: str, *, namespace: str):
                self.paths.append(path)
                return type("Result", (), {"payload": {}, "request_url": path})()

        client = RecordingBlizzardClient(settings)
        player = PlayerIdentity(
            region="US",
            realm="Proudmoore",
            name="MaggieSue",
            player_key="us/proudmoore/maggiesue",
        )

        client.get_character_mythic_keystone_profile(player)
        client.get_character_mythic_keystone_profile_season(player, 17)

        self.assertEqual(
            client.paths,
            [
                "/profile/wow/character/proudmoore/maggiesue/mythic-keystone-profile",
                "/profile/wow/character/proudmoore/maggiesue/mythic-keystone-profile/season/17",
            ],
        )

    def test_character_profile_escapes_special_characters(self) -> None:
        settings = type(
            "BlizzardSettings",
            (),
            {
                "enabled": True,
                "base_url": "https://us.api.blizzard.com",
                "oauth_url": "https://oauth.battle.net/token",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "requests_per_hour_cap": 36000,
                "requests_per_second_cap": 100,
                "timeout_seconds": 30,
                "retry_attempts": 1,
                "backoff_seconds": 1.0,
                "locale": "en_US",
                "namespace_profile": "profile-us",
                "namespace_dynamic": "dynamic-us",
            },
        )()

        class RecordingBlizzardClient(BlizzardClient):
            def __init__(self, local_settings):
                super().__init__(local_settings)
                self.paths: list[str] = []

            def _get_access_token(self) -> str:
                return "token"

            def _get_json(self, path: str, *, namespace: str):
                self.paths.append(path)
                return type("Result", (), {"payload": {}, "request_url": path})()

        client = RecordingBlizzardClient(settings)
        player = PlayerIdentity(
            region="US",
            realm="Azjol-Nerub",
            name="Gëbus",
            player_key="us/azjol-nerub/gëbus",
        )

        client.get_character_mythic_keystone_profile(player)

        self.assertEqual(
            client.paths,
            ["/profile/wow/character/azjol-nerub/g%C3%ABbus/mythic-keystone-profile"],
        )

    def test_rate_limiter_waits_when_second_cap_is_hit(self) -> None:
        settings = type(
            "BlizzardSettings",
            (),
            {
                "enabled": True,
                "base_url": "https://us.api.blizzard.com",
                "oauth_url": "https://oauth.battle.net/token",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "requests_per_hour_cap": 36000,
                "requests_per_second_cap": 2,
                "timeout_seconds": 30,
                "retry_attempts": 1,
                "backoff_seconds": 1.0,
                "locale": "en_US",
                "namespace_profile": "profile-us",
                "namespace_dynamic": "dynamic-us",
            },
        )()
        client = BlizzardClient(settings)
        fake_now = {"value": 1000.0}
        sleeps: list[float] = []

        def fake_time() -> float:
            return fake_now["value"]

        def fake_sleep(duration: float) -> None:
            sleeps.append(duration)
            fake_now["value"] += duration

        with patch("niru.clients.blizzard.time.time", side_effect=fake_time), patch(
            "niru.clients.blizzard.time.sleep", side_effect=fake_sleep
        ):
            client._acquire_request_slot()
            client._acquire_request_slot()
            client._acquire_request_slot()

        self.assertEqual(len(sleeps), 1)
        self.assertGreaterEqual(sleeps[0], 1.0)

    def test_mythic_keystone_dungeon_detail_is_cached_for_one_day(self) -> None:
        settings = type(
            "BlizzardSettings",
            (),
            {
                "enabled": True,
                "base_url": "https://us.api.blizzard.com",
                "oauth_url": "https://oauth.battle.net/token",
                "client_id": "client-id",
                "client_secret": "client-secret",
                "requests_per_hour_cap": 36000,
                "requests_per_second_cap": 100,
                "timeout_seconds": 30,
                "retry_attempts": 1,
                "backoff_seconds": 1.0,
                "locale": "en_US",
                "namespace_profile": "profile-us",
                "namespace_dynamic": "dynamic-us",
            },
        )()

        class RecordingBlizzardClient(BlizzardClient):
            def __init__(self, local_settings):
                super().__init__(local_settings)
                self.paths: list[str] = []

            def _get_access_token(self) -> str:
                return "token"

            def _get_json(self, path: str, *, namespace: str):
                self.paths.append(path)
                return type("Result", (), {"payload": {"id": 239}, "request_url": path})()

        client = RecordingBlizzardClient(settings)

        with patch("niru.clients.blizzard.time.time", side_effect=[1000.0, 1000.0, 90000.0]):
            first = client.get_mythic_keystone_dungeon(239)
            second = client.get_mythic_keystone_dungeon(239)
            third = client.get_mythic_keystone_dungeon(239)

        self.assertEqual(first.payload["id"], 239)
        self.assertEqual(second.payload["id"], 239)
        self.assertEqual(third.payload["id"], 239)
        self.assertEqual(
            client.paths,
            [
                "/data/wow/mythic-keystone/dungeon/239",
                "/data/wow/mythic-keystone/dungeon/239",
            ],
        )


if __name__ == "__main__":
    unittest.main()
