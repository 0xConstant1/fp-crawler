from __future__ import annotations

import threading
import time


class RateLimiter:
    """Thread-safe rate limiter that spaces requests at a fixed average rate."""

    def __init__(self, *, max_requests_per_second: float) -> None:
        if max_requests_per_second <= 0:
            raise ValueError("max_requests_per_second must be greater than 0.")
        self._interval_seconds = 1.0 / max_requests_per_second
        self._next_allowed_at = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            scheduled_at = max(now, self._next_allowed_at)
            self._next_allowed_at = scheduled_at + self._interval_seconds

        sleep_seconds = scheduled_at - now
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
