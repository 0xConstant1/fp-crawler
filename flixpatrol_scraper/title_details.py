from __future__ import annotations

import heapq
import logging
import random
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable
from concurrent.futures import Future

from curl_cffi.requests import Response
from curl_cffi.requests.exceptions import RequestException

from .browser import TITLE_DETAIL_WORKERS, BrowserError, BrowserResponse
from .http_client import DEFAULT_RETRY_STATUS_CODES, retry_delay

TitleDetails = tuple[str, str] | None
TitleResponse = Response | BrowserResponse
TITLE_DETAIL_RETRIES = 2
TITLE_DETAIL_RETRY_STATUSES = DEFAULT_RETRY_STATUS_CODES | {408}
TITLE_DETAIL_START_JITTER_SECONDS = 0.5
logger = logging.getLogger(__name__)


class DetailPoolPaused(Exception):
    """An attempt must wait for capacity, a pause or a transport change."""


class TitleDetailPool:
    """Resolve each source link once, with delayed retries outside the workers."""

    def __init__(
        self,
        fetch: Callable[[str, Callable[[], None]], TitleResponse | None],
        parse: Callable[[str, TitleResponse], TitleDetails],
        *,
        start_interval_seconds: float,
        max_pause_seconds: float = 30.0,
    ) -> None:
        self._fetch = fetch
        self._parse = parse
        self._start_interval_seconds = start_interval_seconds
        self._max_pause_seconds = max_pause_seconds
        self._condition = threading.Condition()
        self._results: dict[str, Future[TitleDetails]] = {}
        self._queue: list[tuple[float, int, str, int]] = []
        self._workers: list[threading.Thread] = []
        self._starts: deque[float] = deque()
        self._order = 0
        self._paused_until = 0.0
        self._next_jittered_start = 0.0
        self._closed = False
        self._deferred_links: set[str] = set()

    def was_deferred(self, link: str) -> bool:
        with self._condition:
            return link in self._deferred_links

    def submit(self, links: Iterable[str]) -> dict[str, Future[TitleDetails]]:
        with self._condition:
            jobs = {}
            for link in links:
                if link not in self._results:
                    self._results[link] = Future()
                    if self._closed:
                        self._defer(link)
                    else:
                        self._enqueue(link, 0, 0.0)
                jobs[link] = self._results[link]
            if self._queue:
                while len(self._workers) < TITLE_DETAIL_WORKERS:
                    worker = threading.Thread(
                        target=self._work, name=f"title-detail-{len(self._workers) + 1}", daemon=True
                    )
                    worker.start()
                    self._workers.append(worker)
                self._condition.notify_all()
            return jobs

    def _enqueue(self, link: str, attempt: int, ready: float) -> None:
        self._order += 1
        heapq.heappush(self._queue, (ready, self._order, link, attempt))

    def _next_job(self) -> tuple[str, int] | None:
        with self._condition:
            while not self._closed:
                if not self._queue:
                    self._condition.wait()
                    continue
                ready, _, link, attempt = self._queue[0]
                delay = max(ready, self._next_start_time()) - time.monotonic()
                if delay > 0:
                    self._condition.wait(min(delay, threading.TIMEOUT_MAX))
                else:
                    heapq.heappop(self._queue)
                    return link, attempt
            return None

    def _next_start_time(self) -> float:
        earliest = max(self._paused_until, self._next_jittered_start)
        if len(self._starts) == TITLE_DETAIL_WORKERS:
            return max(earliest, self._starts[0] + self._start_interval_seconds)
        return earliest

    def _reserve_start(self) -> None:
        with self._condition:
            now = time.monotonic()
            if self._closed or now < self._next_start_time():
                raise DetailPoolPaused
            if len(self._starts) == TITLE_DETAIL_WORKERS:
                self._starts.popleft()
            self._starts.append(now)
            self._next_jittered_start = now + random.uniform(0.0, TITLE_DETAIL_START_JITTER_SECONDS)

    def _reschedule(self, link: str, attempt: int, ready: float, *, pause: bool = False) -> None:
        with self._condition:
            if pause:
                self._paused_until = max(self._paused_until, ready)
            remaining = ready - time.monotonic()
            defer = not self._closed and remaining > self._max_pause_seconds
            if defer:
                scope = "the title detail pool" if pause else f"title details for {link!r}"
                logger.warning(
                    "Deferring %s for this run because a %.1fs retry wait exceeds the budget",
                    scope,
                    remaining,
                )
                if pause:
                    self._finish_pending()
            if not self._closed and not defer and attempt <= TITLE_DETAIL_RETRIES:
                self._enqueue(link, attempt, ready)
            else:
                self._defer(link)
            self._condition.notify_all()

    def _work(self) -> None:
        while (job := self._next_job()) is not None:
            link, attempt = job
            try:
                response = self._fetch(link, self._reserve_start)
                if response is not None and response.status_code in TITLE_DETAIL_RETRY_STATUSES:
                    delay = retry_delay(response.headers.get("retry-after"), attempt, 2.0)
                    self._reschedule(
                        link, attempt + 1, time.monotonic() + delay,
                        pause=response.status_code == 429,
                    )
                else:
                    details = self._parse(link, response) if response is not None else None
                    self._results[link].set_result(details)
            except DetailPoolPaused:
                self._reschedule(link, attempt, 0.0)
            except (RequestException, BrowserError):
                delay = retry_delay(None, attempt, 2.0)
                self._reschedule(link, attempt + 1, time.monotonic() + delay)
            except Exception as exc:
                self._results[link].set_exception(exc)

    def _defer(self, link: str) -> None:
        self._deferred_links.add(link)
        self._results[link].set_result(None)

    def _finish_pending(self) -> None:
        self._closed = True
        for _, _, link, _ in self._queue:
            self._defer(link)
        self._queue.clear()

    def close(self) -> None:
        with self._condition:
            self._finish_pending()
            self._condition.notify_all()
        for worker in self._workers:
            worker.join()
