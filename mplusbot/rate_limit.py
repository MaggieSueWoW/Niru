"""A tiny blocking rate limiter."""

from __future__ import annotations

import threading
import time
from collections import deque


class RateLimiter:
    """Limit requests within a rolling one-minute window."""

    def __init__(self, requests_per_minute: int) -> None:
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be positive")
        self._requests_per_minute = requests_per_minute
        self._lock = threading.Lock()
        self._timestamps: deque[float] = deque()

    def acquire(self) -> None:
        """Block until a request slot is available."""

        while True:
            with self._lock:
                now = time.monotonic()
                cutoff = now - 60.0
                while self._timestamps and self._timestamps[0] <= cutoff:
                    self._timestamps.popleft()
                if len(self._timestamps) < self._requests_per_minute:
                    self._timestamps.append(now)
                    return
                sleep_for = max(self._timestamps[0] + 60.0 - now, 0.05)
            time.sleep(sleep_for)
