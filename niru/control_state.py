"""Redis-backed ephemeral control state for rate limiting and cooldowns."""

from __future__ import annotations

import time
import uuid

from niru.config import RedisSettings


class RedisControlState:
    """Persist short-lived control state across process restarts."""

    def __init__(self, settings: RedisSettings) -> None:
        from redis import Redis
        from redis.exceptions import WatchError

        self._redis = Redis.from_url(settings.url, decode_responses=True)
        self._watch_error = WatchError
        self._prefix = settings.key_prefix
        self._redis.ping()

    def acquire_request_slot(self, *, requests_per_minute: int) -> None:
        """Block until a rolling one-minute request slot is available."""

        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")

        key = self._key("raiderio:request_timestamps")
        while True:
            cooldown_remaining = self.get_cooldown_remaining_seconds()
            if cooldown_remaining > 0:
                time.sleep(min(cooldown_remaining, 1.0))
                continue

            now_ms = int(time.time() * 1000)
            cutoff_ms = now_ms - 60_000
            member = f"{now_ms}:{uuid.uuid4().hex}"
            try:
                with self._redis.pipeline() as pipe:
                    pipe.watch(key)
                    pipe.zremrangebyscore(key, 0, cutoff_ms)
                    current_count = pipe.zcard(key)
                    if current_count < requests_per_minute:
                        pipe.multi()
                        pipe.zadd(key, {member: now_ms})
                        pipe.expire(key, 61)
                        pipe.execute()
                        return

                    oldest_entries = pipe.zrange(key, 0, 0, withscores=True)
                    pipe.unwatch()
            except self._watch_error:
                continue

            sleep_for = 0.05
            if oldest_entries:
                oldest_score = float(oldest_entries[0][1])
                sleep_for = max(((oldest_score + 60_000) - now_ms) / 1000.0, 0.05)
            time.sleep(sleep_for)

    def get_cooldown_remaining_seconds(self) -> float:
        """Return seconds left in the current cooldown window."""

        raw_until = self._redis.get(self._key("raiderio:cooldown_until"))
        if raw_until is None:
            return 0.0

        remaining = float(raw_until) - time.time()
        if remaining <= 0:
            self._redis.delete(
                self._key("raiderio:cooldown_until"),
                self._key("raiderio:cooldown_reason"),
            )
            return 0.0
        return remaining

    def get_cooldown_reason(self) -> str:
        """Return the human-readable cooldown reason when present."""

        return str(self._redis.get(self._key("raiderio:cooldown_reason")) or "")

    def open_cooldown(self, *, seconds: int, reason: str) -> None:
        """Open a cooldown window for Raider.IO requests."""

        cooldown_until = time.time() + max(seconds, 1)
        ttl_seconds = max(seconds, 1) + 1
        self._redis.set(self._key("raiderio:cooldown_until"), cooldown_until, ex=ttl_seconds)
        self._redis.set(self._key("raiderio:cooldown_reason"), reason, ex=ttl_seconds)

    def increment_upstream_failure_streak(self, *, ttl_seconds: int) -> int:
        """Increment and return the current Raider.IO failure streak."""

        key = self._key("raiderio:failure_streak")
        streak = int(self._redis.incr(key))
        self._redis.expire(key, ttl_seconds)
        return streak

    def clear_upstream_failure_streak(self) -> None:
        """Reset the Raider.IO failure streak after a successful request."""

        self._redis.delete(self._key("raiderio:failure_streak"))

    def _key(self, suffix: str) -> str:
        return f"{self._prefix}:{suffix}"
