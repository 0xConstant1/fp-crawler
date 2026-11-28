from __future__ import annotations

import json
import logging
import math
import random
import re
import threading
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit

from bs4 import BeautifulSoup, Tag
from curl_cffi.requests import Response, Session
from curl_cffi.requests.exceptions import HTTPError, RequestException

from .browser import BrowserClient, BrowserError, BrowserResponse
from .cf import (
    CF_CLEARANCE_COOKIE,
    DEFAULT_SOLVE_DEADLINE_SECONDS,
    ChallengeSolution,
    ChallengeSolverError,
    solve_challenge,
)
from .http_client import format_response_diagnostics, get_with_retries
from .rate_limit import RateLimiter
from .storage import atomic_write_text
from .title_details import DetailPoolPaused, TitleDetailPool, TitleResponse
from .tmdb import TMDBMatch, TMDBResolver, normalize_title

DEFAULT_TOP10_URL = "https://flixpatrol.com/top10/"
TOP10_LIMIT = 10
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_OUTPUT_PATH = Path("flixpatrol_top10.json")
DEFAULT_TMDB_MAX_WORKERS = 16
DEFAULT_FLIXPATROL_MAX_REQUESTS_PER_SECOND = 1.5
DEFAULT_FLIXPATROL_REQUEST_JITTER_RANGE = (0.1, 0.4)
DEFAULT_BROWSER_IMPERSONATE = "chrome146"

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)

CF_CHALLENGE_MARKERS = ("__cf_chl", "challenge-platform", "Just a moment...")

CHART_HEADING_PATTERN = re.compile(
    r"^TOP (?P<category>Movies|TV Shows) on (?P<platform>.+) on (?P<date>.+)$",
    re.IGNORECASE,
)
PAGE_TITLE_PATTERN = re.compile(
    r"^TOP 10 on Streaming in (?P<region>.+) on (?P<date>.+)$"
)
REGIONAL_SERVICE_HEADING_PATTERN = re.compile(
    r"^(?P<platform>.+) TOP 10 in (?P<region>.+) on (?P<date>.+)$",
    re.IGNORECASE,
)
REGIONAL_SUBHEADING_PATTERN = re.compile(
    r"^TOP 10 (?P<label>.+)$", re.IGNORECASE
)
QUALIFIER_PATTERN = re.compile(
    r"^(?P<base>.+?) *\( *(?P<qualifier>(?:in|from) [^)]+)\)$", re.IGNORECASE
)
HIDDEN_STYLE_PATTERN = re.compile(
    r"(?:^|;)\s*(?:display\s*:\s*none|visibility\s*:\s*(?:hidden|collapse))"
    r"\s*(?:!important\s*)?(?:;|$)",
    re.IGNORECASE,
)
ORIGINAL_PROVIDER_PATTERN = re.compile(r"This title is (.+) original", re.IGNORECASE)

logger = logging.getLogger(__name__)

# Heading qualifier -> (variant_id, variant_label). Languages use their ISO 639-1
# code; non-language qualifiers use a kebab-case slug of the phrase. A qualifier
# that is missing from this table is only fatal inside a colliding group, where
# there is no unique catalog_id to fall back to.
VARIANT_TABLE: dict[str, tuple[str, str]] = {
    "in english": ("en", "English"),
    "in hindi": ("hi", "Hindi"),
    "in marathi": ("mr", "Marathi"),
    "in japanese": ("ja", "Japanese"),
    "in korean": ("ko", "Korean"),
    "in indonesian": ("id", "Indonesian"),
    "in italian": ("it", "Italian"),
    "from amazon channels": ("amazon-channels", "Amazon Channels"),
}

CANONICAL_CATEGORIES: frozenset[str] = frozenset({"movies", "series", "overall"})
ChartIdentity = tuple[str, str, str | None]


def _is_cf_challenge(response: Response | BrowserResponse) -> bool:
    """Tell a CF challenge apart from an ordinary 403."""
    if response.headers.get("cf-mitigated") == "challenge":
        return True
    if response.status_code not in (403, 503):
        return False
    body = response.text or ""
    return any(marker in body for marker in CF_CHALLENGE_MARKERS)


def split_catalog_id(chart: dict[str, Any]) -> tuple[str, str, str | None]:
    """Return ``(service, category, variant_id)`` for a serialized chart.

    ``category`` is the slug used in the id (``series``, not ``tv shows``); the
    variant id is stripped off using the chart's own ``variant`` field rather
    than guessed from the string, so a variant id that contains a dash
    (``amazon-channels``) is handled correctly.
    """
    service, _, rest = chart["catalog_id"].partition(".")
    variant = chart.get("variant")
    if variant is None:
        return service, rest, None
    suffix = f"-{variant['id']}"
    category = rest.removesuffix(suffix)
    return service, category, variant["id"]


def chart_identity(chart: dict[str, Any]) -> ChartIdentity:
    """Identify a serialized chart even when a variant holds the bare catalog ID."""
    service, category, variant_id = split_catalog_id(chart)
    heading, platform = chart.get("heading"), chart.get("platform")
    if variant_id is None and isinstance(heading, str) and isinstance(platform, str):
        delimiter = f" on {_normalize_whitespace(platform).casefold()} "
        label, separator, _ = _normalize_whitespace(heading).casefold().partition(delimiter)
        if separator:
            _, qualifier = _split_label_qualifier(label)
            variant_id = _qualifier_identity(qualifier)
    return service, category, variant_id


class ScraperError(RuntimeError):
    """Raised when the scraper cannot fetch or parse a FlixPatrol page."""


class NoChartsFoundError(ScraperError):
    """Raised when a valid FlixPatrol page exposes no TOP 10 charts."""


class CFChallengeError(ScraperError):
    """Raised when a CF challenge is served instead of the page.

    Distinct from a plain 403 because the remedy is different: a challenge needs
    fresh clearance, not a retry or a different rate limit.
    """


class RecoveryOpenError(CFChallengeError):
    """The shared recovery budget is exhausted; leave remaining work pending."""


@dataclass(frozen=True, slots=True)
class ChartEntry:
    rank: int
    title: str
    tmdb: TMDBMatch | None = None
    source_link: str = ""
    original_provider: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rank": self.rank,
            "title": self.title,
            "tmdb": self.tmdb.to_dict() if self.tmdb is not None else None,
        }


@dataclass(frozen=True, slots=True)
class ChartVariant:
    id: str
    label: str

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "label": self.label}


@dataclass(frozen=True, slots=True)
class Chart:
    heading: str
    catalog_id: str
    category: str
    platform: str
    date: str
    is_full_top10: bool
    entries: list[ChartEntry]
    variant: ChartVariant | None = None
    source_rank_count: int = TOP10_LIMIT

    @property
    def title_count(self) -> int:
        return len(self.entries)

    @property
    def titles(self) -> list[str]:
        return [entry.title for entry in self.entries]

    @property
    def repeated_title_ranks(self) -> dict[int, int]:
        first_ranks: dict[str, int] = {}
        repeats: dict[int, int] = {}
        for entry in self.entries:
            if entry.source_link:
                first_rank = first_ranks.setdefault(entry.source_link, entry.rank)
                if first_rank != entry.rank:
                    repeats[entry.rank] = first_rank
        return repeats

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"catalog_id": self.catalog_id}
        if self.variant is not None:
            payload["variant"] = self.variant.to_dict()
        payload.update(
            {
                "heading": self.heading,
                "category": _serialize_category(self.category),
                "platform": self.platform,
                "date": self.date,
                "title_count": self.title_count,
                "entries": [entry.to_dict() for entry in self.entries],
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class _PendingChart:
    """A scraped chart before its catalog_id is known.

    catalog_id assignment is collision-scoped, so it cannot happen until every
    chart on the page has been seen and bucketed by (service, category).
    """

    chart: Chart
    service_slug: str
    category_slug: str
    qualifier: str | None


@dataclass(frozen=True, slots=True)
class ScrapeResult:
    source: str
    page_title: str
    region: str
    date: str
    scraped_at_utc: str
    charts: list[Chart]
    max_titles_per_chart: int = TOP10_LIMIT
    source_complete: bool = False
    title_details_deferred: bool = False

    @property
    def expected_rank_counts(self) -> dict[str, int]:
        """Number of source ranks expected in each Top 10 catalog."""
        return {
            chart.catalog_id: min(chart.source_rank_count, self.max_titles_per_chart)
            for chart in self.charts
        }

    @property
    def source_rank_counts(self) -> dict[str, int]:
        return self.expected_rank_counts

    @property
    def source_title_repeats(self) -> dict[str, list[list[int]]]:
        return {
            chart.catalog_id: [[rank, first] for rank, first in repeats.items()]
            for chart in self.charts
            if (repeats := chart.repeated_title_ranks)
        }

    def to_dict(self, *, output_region: str | None = None) -> dict[str, Any]:
        return {
            "source": self.source,
            "page_title": self.page_title,
            "region": _serialize_region(output_region or self.region),
            "date": self.date,
            "scraped_at_utc": self.scraped_at_utc,
            "charts": [chart.to_dict() for chart in self.charts],
        }

    def to_json(self, *, indent: int = 2, output_region: str | None = None) -> str:
        return json.dumps(
            self.to_dict(output_region=output_region),
            indent=indent,
            ensure_ascii=False,
        )


class FlixPatrolScraper:
    """Scraper for FlixPatrol TOP 10 pages."""

    def __init__(
        self,
        *,
        session: Session | None = None,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        max_titles_per_chart: int = TOP10_LIMIT,
        tmdb_resolver: TMDBResolver | None = None,
        tmdb_max_workers: int = DEFAULT_TMDB_MAX_WORKERS,
        fetch_title_details: bool = False,
        max_requests_per_second: float = DEFAULT_FLIXPATROL_MAX_REQUESTS_PER_SECOND,
        request_jitter_range: tuple[float, float] = DEFAULT_FLIXPATROL_REQUEST_JITTER_RANGE,
        cf_clearance: str | None = None,
        user_agent: str | None = None,
        solve_cf: bool = False,
        proxy: str | None = None,
        solve_headless: bool = True,
        solve_deadline_seconds: float = DEFAULT_SOLVE_DEADLINE_SECONDS,
        challenge_solver: Callable[..., ChallengeSolution] = solve_challenge,
        transport: str = "http",
        max_recoveries: int = 2,
    ) -> None:
        self._shared_session = session
        self._thread_local = threading.local()
        self.cf_clearance = cf_clearance or None
        self.user_agent = user_agent or DEFAULT_USER_AGENT
        self.solve_cf = solve_cf
        self.proxy = proxy or None
        self.solve_headless = solve_headless
        self.solve_deadline_seconds = solve_deadline_seconds
        self._challenge_solver = challenge_solver
        self._challenge_lock = threading.RLock()
        self._stop_reason: str | None = None
        self._consecutive_blocks = 0
        self._clearance_rejected = False
        if transport not in {"auto", "http", "browser"}:
            raise ValueError(f"Unknown transport: {transport!r}")
        if not math.isfinite(solve_deadline_seconds) or solve_deadline_seconds <= 0 or max_recoveries < 0:
            raise ValueError("Solver deadline must be positive and recovery budget nonnegative.")
        if not math.isfinite(max_requests_per_second) or max_requests_per_second <= 0 or timeout_seconds <= 0:
            raise ValueError("Request rate and HTTP timeout must be positive and finite.")
        self.transport = transport
        self.max_recoveries = max_recoveries
        self.metrics = {"requests": 0, "challenges": 0, "recoveries": 0}
        self._request_count_lock = threading.Lock()
        self.timeout_seconds = timeout_seconds
        self._rate_limiter = RateLimiter(
            max_requests_per_second=max_requests_per_second
        )
        self._browser = self._new_browser() if transport == "browser" else None
        self._detail_browser: BrowserClient | None = None
        self._detail_http_session: Session | None = None
        self._detail_recovery_lock = threading.Lock()
        self._detail_recovery_pending = False
        self._detail_recovery_attempted = False
        self._detail_navigation = False
        self._title_detail_pool = TitleDetailPool(
            self._fetch_title_response,
            self._parse_title_details,
            start_interval_seconds=self._rate_limiter.interval_seconds,
            max_pause_seconds=timeout_seconds,
        )
        self._credentials_generation = 0
        if not 1 <= max_titles_per_chart <= TOP10_LIMIT:
            raise ValueError("max_titles_per_chart must be between 1 and 10")
        self.max_titles_per_chart = max_titles_per_chart
        self.tmdb_resolver = tmdb_resolver
        self.tmdb_max_workers = max(1, tmdb_max_workers)
        self.fetch_title_details = fetch_title_details
        self.request_jitter_range = request_jitter_range

    def _build_session(self) -> Session:
        return Session(
            impersonate=DEFAULT_BROWSER_IMPERSONATE,
            default_headers=True,
            default_encoding="utf-8",
            headers={"user-agent": self.user_agent},
            **({"proxy": self.proxy} if self.proxy else {}),
        )

    def _get_session(self) -> Session:
        if self._shared_session is not None:
            return self._shared_session

        session = getattr(self._thread_local, "session", None)
        generation = getattr(self._thread_local, "generation", None)
        if session is None or generation != self._credentials_generation:
            session = self._build_session()
            self._thread_local.session = session
            self._thread_local.generation = self._credentials_generation
        return session

    def _refresh_challenge_credentials(self, seen_generation: int, url: str) -> bool:
        """Solve the challenge once and adopt the credentials.

        ``seen_generation`` is the generation the caller held when it was
        challenged. If another thread already refreshed past it, this is a no-op
        and the caller retries with the newer credentials.
        """
        if not self.solve_cf:
            return False

        with self._challenge_lock:
            if self._credentials_generation != seen_generation:
                return True

            if self.metrics["recoveries"] >= self.max_recoveries:
                self._stop_reason = "Challenge recovery budget exhausted"
                raise RecoveryOpenError(self._stop_reason)
            self.metrics["recoveries"] += 1
            try:
                solution = self._challenge_solver(
                    url,
                    proxy=self.proxy,
                    headless=self.solve_headless,
                    deadline_seconds=self.solve_deadline_seconds,
                )
            except ChallengeSolverError as exc:
                self._stop_reason = f"Challenge recovery failed: {exc}"
                raise RecoveryOpenError(self._stop_reason) from exc
            self.cf_clearance = solution.cf_clearance
            self.user_agent = solution.user_agent
            self._detail_http_session = None
            self._credentials_generation += 1
            return True

    def _sleep_request_jitter(self) -> None:
        minimum, maximum = self.request_jitter_range
        if maximum <= 0:
            return
        time_to_sleep = random.uniform(max(minimum, 0.0), max(maximum, 0.0))
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)

    def scrape_url(self, url: str = DEFAULT_TOP10_URL) -> ScrapeResult:
        response = self._fetch(url)
        html = response.source_html if isinstance(response, BrowserResponse) else response.text
        if not html:
            raise ScraperError(f"Could not read the original HTML for {response.url!r}")
        return self.parse_html(
            html, source=response.url, fetch_title_details=self.fetch_title_details
        )

    def _new_browser(self) -> BrowserClient:
        return BrowserClient(
            headless=self.solve_headless,
            timeout_ms=self.timeout_seconds * 1000,
            proxy=self.proxy,
            pace_request=self._rate_limiter.acquire,
        )

    @property
    def recovery_open(self) -> bool:
        return self._stop_reason is not None

    @property
    def active_transport(self) -> str:
        return "browser" if self._browser is not None else "http"

    @property
    def stop_reason(self) -> str | None:
        return self._stop_reason

    def close(self) -> None:
        self._title_detail_pool.close()
        if self._browser is not None:
            self._browser.close()
        if self._detail_browser is not None:
            self._detail_browser.close()

    def _fetch(self, url: str) -> Response | BrowserResponse:
        if self._browser is not None:
            with self._challenge_lock:
                if self._stop_reason:
                    raise RecoveryOpenError(self._stop_reason)
                return self._fetch_browser(url)
        if self.transport == "auto":
            return self._fetch_auto(url)
        return self._fetch_http(url)

    def _fetch_auto(self, url: str) -> Response | BrowserResponse:
        response, _ = self._request(url)
        with self._challenge_lock:
            if self._stop_reason:
                raise RecoveryOpenError(self._stop_reason)
            if _is_cf_challenge(response):
                self.metrics["challenges"] += 1
                if self._browser is None:
                    if self.metrics["recoveries"] >= self.max_recoveries:
                        self._record_access_block(response, url)
                    self.metrics["recoveries"] += 1
                    self._browser = self._detail_browser or self._new_browser()
                    self._detail_browser = None
                    logger.warning("HTTP challenge at %s; switching to a persistent browser", url)
                return self._fetch_browser(url)
            if response.status_code in (403, 429):
                self._record_access_block(response, url)
            self._raise_for_bad_response(response, url)
            self._consecutive_blocks = 0
            return response

    def _fetch_browser(self, url: str) -> BrowserResponse:
        self._before_request(pace=False)
        try:
            response = self._browser.fetch(url, deadline_seconds=self.solve_deadline_seconds)
        except BrowserError as exc:
            self._stop_reason = f"Browser fetch failed: {exc}"
            raise RecoveryOpenError(self._stop_reason) from exc
        if _is_cf_challenge(response):
            self.metrics["challenges"] += 1
            self._record_access_block(response, url)
        if response.status_code in (403, 429):
            self._record_access_block(response, url)
        self._raise_for_bad_response(response, url)
        self._consecutive_blocks = 0
        return response

    def _fetch_http(self, url: str) -> Response:
        response, generation = self._request(url)
        # Publish refreshed credentials only after their validation request finishes.
        with self._challenge_lock:
            if self._stop_reason:
                raise RecoveryOpenError(self._stop_reason)
            if not _is_cf_challenge(response):
                if response.status_code in (403, 429):
                    self._record_access_block(response, url)
                self._raise_for_bad_response(response, url)
                self._consecutive_blocks = 0
                return response

            self.metrics["challenges"] += 1
            if generation == self._credentials_generation and (
                self._clearance_rejected
                or not self.solve_cf
                or self.metrics["recoveries"] >= self.max_recoveries
            ):
                self._record_access_block(response, url)
            if not self._refresh_challenge_credentials(generation, url):
                self._raise_for_bad_response(response, url)
                return response

            response, _ = self._request(url)
            if _is_cf_challenge(response):
                self.metrics["challenges"] += 1
                self._clearance_rejected = True
                self._record_access_block(response, url)
            if response.status_code in (403, 429):
                self._record_access_block(response, url)
            self._raise_for_bad_response(response, url)
            self._consecutive_blocks = 0
            return response

    def _record_access_block(self, response: Response | BrowserResponse, url: str) -> None:
        self._consecutive_blocks += 1
        if self._consecutive_blocks >= 3:
            self._stop_reason = (
                f"Stopped after three consecutive access blocks; last URL {url!r}: "
                f"{format_response_diagnostics(response)}"
            )
            raise RecoveryOpenError(self._stop_reason)
        self._raise_for_bad_response(response, url)

    def _before_request(self, *, pace: bool = True) -> None:
        self._sleep_request_jitter()
        if pace:
            self._rate_limiter.acquire()
        self._check_access()
        self._count_request()

    def _check_access(self) -> None:
        with self._challenge_lock:
            if self._stop_reason:
                raise RecoveryOpenError(self._stop_reason)

    def _count_request(self) -> None:
        with self._request_count_lock:
            self.metrics["requests"] += 1

    def _request(
        self, url: str, *, before_detail_send: Callable[[], None] | None = None
    ) -> tuple[Response, int]:
        with self._challenge_lock:
            if self._stop_reason:
                raise RecoveryOpenError(self._stop_reason)
            generation = self._credentials_generation
            session = (
                self._detail_http_session if before_detail_send is not None
                and self._detail_http_session is not None else self._get_session()
            )
            cookies = (
                {CF_CLEARANCE_COOKIE: self.cf_clearance} if self.cf_clearance else None
            )
        def before_attempt() -> None:
            if before_detail_send is None:
                self._before_request()
                return
            self._check_access()
            if generation != self._credentials_generation:
                raise DetailPoolPaused
            before_detail_send()
            self._count_request()

        detail_options = (
            {"total_retries": 0, "allow_redirects": False} if before_detail_send is not None else {}
        )
        try:
            response = get_with_retries(
                session,
                url,
                timeout_seconds=self.timeout_seconds,
                cookies=cookies,
                before_attempt=before_attempt,
                stop_retry=_is_cf_challenge,
                **detail_options,
            )
        except RequestException as exc:
            if before_detail_send is not None:
                raise
            raise ScraperError(f"Failed to fetch {url!r}: {exc}") from exc
        if (
            before_detail_send is None and response.status_code == 200
            and urlsplit(response.url).hostname in {"flixpatrol.com", "www.flixpatrol.com"}
        ):
            with self._challenge_lock:
                if generation == self._credentials_generation:
                    self._detail_http_session = session
        return response, generation

    def parse_html(
        self, html: str, *, source: str, fetch_title_details: bool = False
    ) -> ScrapeResult:
        soup = BeautifulSoup(html, "html.parser")
        page_title = self._extract_page_title(soup)
        page_region, page_date = self._extract_page_metadata(page_title)
        table_closures = _TableClosures()
        table_closures.feed(html)
        table_closures.close()
        charts = _assign_catalog_ids(
            self._extract_regional_charts(soup, table_closures.closed)
            or self._extract_global_charts(soup, table_closures.closed)
        )

        if not charts:
            raise NoChartsFoundError("No TOP 10 charts were found in the provided HTML.")

        validate_chart_dates(page_date, ((chart.heading, chart.date) for chart in charts))
        title_details_deferred = False
        if self.tmdb_resolver is not None:
            charts, title_details_deferred = self._enrich_charts_with_tmdb(
                charts, fetch_title_details=fetch_title_details
            )
            for chart in charts:
                repeats = chart.repeated_title_ranks
                validate_tmdb_ids(
                    (
                        (
                            entry.tmdb.media_type,
                            entry.tmdb.id,
                            entry.title,
                            repeats.get(entry.rank, entry.rank),
                        )
                        for entry in chart.entries
                        if entry.tmdb is not None
                    ),
                    chart.heading,
                )

        return ScrapeResult(
            source=source,
            page_title=page_title,
            region=page_region,
            date=page_date,
            scraped_at_utc=datetime.now(UTC).isoformat(),
            charts=charts,
            max_titles_per_chart=self.max_titles_per_chart,
            source_complete=table_closures.document_closed,
            title_details_deferred=title_details_deferred,
        )

    @staticmethod
    def _raise_for_bad_response(response: Response, url: str) -> None:
        diagnostics = format_response_diagnostics(response)
        if _is_cf_challenge(response):
            raise CFChallengeError(f"Failed to fetch {url!r}: {diagnostics}, challenged")
        try:
            response.raise_for_status()
        except HTTPError as exc:
            raise ScraperError(f"Failed to fetch {url!r}: {diagnostics}") from exc

    @staticmethod
    def _extract_page_title(soup: BeautifulSoup) -> str:
        heading = soup.find("h1")
        if heading is None:
            raise ScraperError("Page is missing the main <h1> heading.")
        return _normalize_whitespace(heading.get_text(" ", strip=True))

    @staticmethod
    def _extract_page_metadata(page_title: str) -> tuple[str, str]:
        match = PAGE_TITLE_PATTERN.fullmatch(page_title)
        if match is None:
            raise ScraperError(f"Unexpected page title format: {page_title!r}")
        return match.group("region"), match.group("date")

    def _extract_global_charts(
        self, soup: BeautifulSoup, closed_tables: set[tuple[int, int]]
    ) -> list[_PendingChart]:
        charts: list[_PendingChart] = []

        for heading_tag in soup.find_all("h2"):
            if _is_hidden(heading_tag):
                continue
            heading_text = _visible_text(heading_tag, separator=" ")
            heading_match = CHART_HEADING_PATTERN.fullmatch(heading_text)
            if heading_match is None:
                continue

            ranking_table = self._find_table_for_heading(heading_tag)
            if ranking_table is None:
                raise ScraperError(f"Could not find a table for chart {heading_text!r}.")

            entries = self._extract_entries_from_table(*ranking_table, heading_text, closed_tables)
            platform = heading_match.group("platform")
            base_label, qualifier = _split_label_qualifier(
                heading_match.group("category")
            )

            charts.append(
                _PendingChart(
                    chart=Chart(
                        heading=heading_text,
                        catalog_id="",
                        category=base_label,
                        platform=platform,
                        date=heading_match.group("date"),
                        is_full_top10=len(entries) >= self.max_titles_per_chart,
                        entries=self._select_chart_entries(entries, heading_text),
                        source_rank_count=len(entries),
                    ),
                    service_slug=_slugify_platform(platform),
                    category_slug=_category_slug(base_label),
                    qualifier=qualifier,
                )
            )

        return charts

    def _extract_regional_charts(
        self, soup: BeautifulSoup, closed_tables: set[tuple[int, int]]
    ) -> list[_PendingChart]:
        charts: list[_PendingChart] = []

        for heading_tag in soup.find_all("h2"):
            if _is_hidden(heading_tag):
                continue
            heading_text = _visible_text(heading_tag, separator=" ")
            service_match = REGIONAL_SERVICE_HEADING_PATTERN.fullmatch(heading_text)
            if service_match is None:
                continue

            section = heading_tag.find_parent("div", class_=lambda value: value and "content" in value)
            if section is None:
                raise ScraperError(
                    f"Could not find the section wrapper for service heading {heading_text!r}."
                )

            platform = service_match.group("platform")
            region = service_match.group("region")
            date = service_match.group("date")
            first_chart_index = len(charts)

            for subheading_tag in section.find_all("h3"):
                if _is_hidden(subheading_tag):
                    continue
                subheading_text = _visible_text(subheading_tag, separator=" ")
                subheading_match = REGIONAL_SUBHEADING_PATTERN.fullmatch(subheading_text)
                if subheading_match is None:
                    continue

                label = subheading_match.group("label")
                base_label, qualifier = _split_label_qualifier(label)

                ranking_table = self._find_table_for_heading(subheading_tag, section=section)
                if ranking_table is None:
                    raise ScraperError(
                        f"Could not find a table for subheading {subheading_text!r} "
                        f"under service {heading_text!r}."
                    )

                chart_heading = f"TOP {label} on {platform} in {region} on {date}"
                entries = self._extract_entries_from_table(
                    *ranking_table, chart_heading, closed_tables
                )

                charts.append(
                    _PendingChart(
                        chart=Chart(
                            heading=chart_heading,
                            catalog_id="",
                            category=base_label,
                            platform=platform,
                            date=date,
                            is_full_top10=len(entries) >= self.max_titles_per_chart,
                            entries=self._select_chart_entries(entries, chart_heading),
                            source_rank_count=len(entries),
                        ),
                        service_slug=_slugify_platform(platform),
                        category_slug=_category_slug(base_label),
                        qualifier=qualifier,
                    )
                )

            if len(charts) == first_chart_index:
                raise ScraperError(f"Service heading {heading_text!r} has no visible charts.")

        return charts

    @staticmethod
    def _find_table_for_heading(
        heading_tag: Tag, *, section: Tag | None = None
    ) -> tuple[Tag, list[Tag]] | None:
        boundaries = {"h1", "h2", "h3"} if section is not None else {"h1", "h2"}
        candidates = boundaries | {"table"}
        ranking_table = None
        for tag in heading_tag.next_elements:
            if not isinstance(tag, Tag) or tag.name not in candidates:
                continue
            if section is not None and not any(parent is section for parent in tag.parents):
                break
            if _is_hidden(tag) or tag.find_parent("table") is not None:
                continue
            if tag.name in boundaries:
                break
            rows = _ranking_rows(tag)
            if not rows:
                continue
            if ranking_table is not None:
                heading = heading_tag.get_text(" ", strip=True)
                raise ScraperError(f"Multiple ranking tables for {heading!r}")
            ranking_table = tag, rows
        return ranking_table

    @staticmethod
    def _extract_entries_from_table(
        table: Tag, rows: list[Tag], heading: str, closed_tables: set[tuple[int, int]]
    ) -> list[ChartEntry]:
        if (table.sourceline, table.sourcepos) not in closed_tables:
            raise ScraperError(f"Chart {heading!r} has an unclosed ranking table")
        entries: list[ChartEntry] = []
        for row in rows:
            cells = row.find_all("td", recursive=False)
            rank_text = _visible_text(cells[0]) if cells else ""
            rank_match = re.fullmatch(r"([0-9]{1,2})\s*\.?", rank_text)
            if rank_match is None:
                raise ScraperError(
                    f"Chart {heading!r} has an invalid source rank {rank_text!r}"
                )
            rank = int(rank_match[1])
            title_links = []
            if len(cells) >= 3:
                for link in cells[2].find_all("a", href=True):
                    if link.find_parent("tr") is not row:
                        continue
                    source_link = _title_link_path(link["href"])
                    title = _visible_text(link)
                    if source_link and title:
                        title_links.append((source_link, title))
            if len(title_links) != 1:
                raise ScraperError(
                    f"Chart {heading!r} rank {rank} needs exactly one visible title link"
                )
            source_link, title = title_links[0]
            providers = {
                match[1]
                for badge in cells[2].find_all("span", title=True)
                if badge.find_parent("tr") is row
                and not _is_hidden(badge)
                and (match := ORIGINAL_PROVIDER_PATTERN.fullmatch(badge["title"]))
            }
            entries.append(
                ChartEntry(
                    rank=rank,
                    title=title,
                    source_link=source_link,
                    original_provider=next(iter(providers)) if len(providers) == 1 else None,
                )
            )

        validate_chart_ranks(
            [entry.rank for entry in entries], heading, count=len(entries), max_count=None
        )
        return entries

    def _select_chart_entries(self, entries: list[ChartEntry], heading: str) -> list[ChartEntry]:
        count = min(len(entries), self.max_titles_per_chart)
        unique: dict[str, ChartEntry] = {}
        repeats: list[ChartEntry] = []
        for entry in entries:
            if entry.source_link in unique:
                repeats.append(entry)
            else:
                unique[entry.source_link] = entry
                if len(unique) == count:
                    break
        if not repeats:
            return list(unique.values())

        retained = repeats[: count - len(unique)]
        selected = sorted([*unique.values(), *retained], key=lambda entry: entry.rank)
        logger.warning(
            "Chart %r has repeated source title links. Selected source ranks %s, retaining %d repeats.",
            heading,
            [entry.rank for entry in selected],
            len(retained),
        )
        return [
            entry if entry.rank == rank else replace(entry, rank=rank)
            for rank, entry in enumerate(selected, start=1)
        ]

    def _fetch_title_response(
        self, source_link: str, before_send: Callable[[], None]
    ) -> TitleResponse | None:
        url = f"https://flixpatrol.com{quote(source_link)}/"
        try:
            while True:
                with self._detail_recovery_lock:
                    if self._detail_recovery_pending:
                        return self._recover_title_response(url, before_send)
                    with self._challenge_lock:
                        browser = self._browser or self._detail_browser
                        navigate = self._detail_navigation
                        recovered = self._detail_recovery_attempted
                if browser is None:
                    def before_http_send() -> None:
                        if (
                            self._detail_recovery_pending or self._browser is not None
                            or self._detail_browser is not None
                        ):
                            raise DetailPoolPaused
                        before_send()

                    response, _ = self._request(url, before_detail_send=before_http_send)
                else:
                    def before_browser_send() -> None:
                        if self._detail_recovery_pending or navigate != self._detail_navigation:
                            raise DetailPoolPaused
                        before_send()
                        self._count_request()

                    self._check_access()
                    response = browser.request(
                        url, timeout_seconds=self.timeout_seconds, before_send=before_browser_send,
                        navigate=navigate,
                    )
                if response.status_code == 429 or not _is_cf_challenge(response):
                    return response
                if browser is not None and not navigate:
                    with self._detail_recovery_lock:
                        self._detail_navigation = True
                    continue
                with self._detail_recovery_lock:
                    if recovered:
                        return None
                    if self._detail_recovery_attempted:
                        # Another title refreshed the shared session during this request.
                        continue
                    self._detail_recovery_pending = True
                    return self._recover_title_response(url, before_send)
        except RecoveryOpenError:
            return None

    def _recover_title_response(
        self, url: str, before_send: Callable[[], None]
    ) -> TitleResponse | None:
        with self._challenge_lock:
            browser = self._browser or self._detail_browser
            if browser is None:
                browser = self._detail_browser = self._new_browser()
        self._check_access()

        def start_recovery() -> None:
            before_send()
            self._detail_recovery_pending = False
            self._detail_recovery_attempted = True
            self._count_request()

        response = browser.fetch(
            url, deadline_seconds=self.solve_deadline_seconds, include_source=False,
            before_send=start_recovery, pace=False,
        )
        return None if response.status_code != 429 and _is_cf_challenge(response) else response

    @staticmethod
    def _parse_title_details(source_link: str, response: TitleResponse) -> tuple[str, str] | None:
        if (
            response.status_code != 200
            or _is_cf_challenge(response)
            or _title_link_path(response.url) != source_link
        ):
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        heading = soup.find("h1")
        if heading is None or _is_hidden(heading):
            return None
        metadata = heading.parent.find_next_sibling()
        if metadata is None or _is_hidden(metadata):
            return None
        kind = metadata.select_one(":scope > div:first-child > div:first-child")
        premieres = [
            tag for tag in metadata.select(':scope > div[title="Premiere"] > div:first-child')
            if not _is_hidden(tag)
        ]
        if kind is None or len(premieres) != 1:
            return None
        media_type = {"movie": "movie", "tv show": "tv"}.get(_visible_text(kind).casefold())
        if media_type is None:
            return None
        try:
            premiere = datetime.strptime(_visible_text(premieres[0]), "%m/%d/%Y")
        except ValueError:
            return None
        return media_type, f"{premiere.year:04d}"

    def _enrich_charts_with_tmdb(
        self, charts: list[Chart], *, fetch_title_details: bool = False
    ) -> tuple[list[Chart], bool]:
        work_items: dict[tuple[str, str, str | None, str | None, bool], list[tuple[int, int]]] = {}
        for chart_index, chart in enumerate(charts):
            media_hint = self._chart_media_type(chart.category)
            overall = chart.category.casefold() == "overall"
            repeats = chart.repeated_title_ranks
            for entry_index, entry in enumerate(chart.entries):
                if entry.rank in repeats:
                    entry = chart.entries[repeats[entry.rank] - 1]
                work_key = (entry.source_link, entry.title, media_hint, entry.original_provider, overall)
                work_items.setdefault(work_key, []).append((chart_index, entry_index))

        resolver = self.tmdb_resolver
        resolve = resolver.resolve
        native_resolver = (
            isinstance(resolver, TMDBResolver)
            and getattr(resolve, "__func__", None) is TMDBResolver.resolve
        )

        def match_hints(work_key: tuple[str, str, str | None, str | None, bool]) -> dict[str, Any]:
            source_link, title, media_hint, original_provider, _ = work_key
            hints = {"media_hint": media_hint}
            if original_provider is not None:
                hints["original_provider"] = original_provider
            year_match = re.fullmatch(r"/title/(.+)-([12][0-9]{3})", source_link)
            if year_match and normalize_title(year_match[1]) == normalize_title(title):
                hints["release_year"] = year_match[2]
            return hints

        def lookup(work_key):
            if native_resolver:
                return resolver.find_match(work_key[1], **match_hints(work_key))
            return resolve(work_key[1], media_hint=work_key[2])

        def typed_fallbacks(matches) -> dict[str, TMDBMatch]:
            typed_matches: dict[str, dict[tuple[str, int], TMDBMatch]] = {}
            for (source_link, _, media_hint, _, _), match in matches.items():
                if source_link and isinstance(match, TMDBMatch) and match.media_type == media_hint:
                    typed_matches.setdefault(source_link, {})[(match.media_type, match.id)] = match
            return {
                source_link: next(iter(options.values()))
                for source_link, options in typed_matches.items()
                if len(options) == 1
            }

        resolved_matches: dict[tuple[int, int], TMDBMatch | None] = {}
        details_deferred = threading.Event()
        max_workers = min(self.tmdb_max_workers, len(work_items))
        pool = ThreadPoolExecutor(max_workers=max_workers) if max_workers > 1 else nullcontext()
        with pool as executor:
            def run(action, keys):
                matches = executor.map(action, keys) if executor else map(action, keys)
                return dict(zip(keys, matches, strict=True))

            matches = run(lookup, work_items)
            overall_fallbacks = typed_fallbacks(matches)
            detail_jobs = self._title_detail_pool.submit(
                link for (link, _, _, _, overall), match in matches.items()
                if fetch_title_details and native_resolver and link and isinstance(match, tuple)
                and not (overall and link in overall_fallbacks)
            )

            def complete(work_key):
                link, title, _, _, overall = work_key
                match = matches[work_key]
                fallback = overall_fallbacks.get(link) if overall else None
                if not native_resolver:
                    return match or fallback
                detail = detail_jobs.get(link)

                def get_details():
                    result = detail.result()
                    if self._title_detail_pool.was_deferred(link):
                        details_deferred.set()
                    return result

                return resolver.complete_match(
                    title, match, **match_hints(work_key), fallback=fallback,
                    title_details=get_details if detail is not None else None,
                )

            typed = [key for key in work_items if key[2] is not None]
            finalized = run(complete, typed)
            overall_fallbacks = typed_fallbacks(finalized)
            untyped = [key for key in work_items if key[2] is None]
            finalized.update(run(complete, untyped))
            for key, match in finalized.items():
                resolved_matches.update(dict.fromkeys(work_items[key], match))

        enriched_charts: list[Chart] = []
        for chart_index, chart in enumerate(charts):
            enriched_entries = [
                replace(entry, tmdb=resolved_matches[(chart_index, entry_index)])
                for entry_index, entry in enumerate(chart.entries)
            ]
            enriched_charts.append(replace(chart, entries=enriched_entries))

        return enriched_charts, details_deferred.is_set()

    @staticmethod
    def _chart_media_type(category: str) -> str | None:
        category = category.casefold()
        if category in {"movies", "kids movies"}:
            return "movie"
        if category in {"tv shows", "kids tv shows"}:
            return "tv"
        return None


class _TableClosures(HTMLParser):
    """Record explicit closures before BeautifulSoup repairs truncated HTML."""

    def __init__(self) -> None:
        super().__init__()
        self._open: list[tuple[int, int]] = []
        self.closed: set[tuple[int, int]] = set()
        self._structure: list[str] = []
        self._inert: list[str] = []
        self._valid_structure = True
        self._body_seen = False
        self._html_closed = False

    @property
    def document_closed(self) -> bool:
        return self._valid_structure and self._body_seen and self._html_closed and not self._inert

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"noscript", "template"}:
            self._inert.append(tag)
            return
        if self._inert:
            return
        if tag == "table":
            self._open.append(self.getpos())
        if tag == "html":
            self._valid_structure &= not self._structure and not self._html_closed
        elif tag == "body":
            self._valid_structure &= self._structure == ["html"] and not self._body_seen
            self._body_seen = True
        elif tag in {"div", "table"}:
            self._valid_structure &= "body" in self._structure
        else:
            return
        self._structure.append(tag)

    def handle_endtag(self, tag: str) -> None:
        if self._inert:
            if tag == self._inert[-1]:
                self._inert.pop()
            return
        if tag == "table" and self._open:
            self.closed.add(self._open.pop())
        if tag in {"html", "body", "div", "table"}:
            if not self._structure or self._structure[-1] != tag:
                self._valid_structure = False
                return
            self._structure.pop()
            if tag == "html":
                self._html_closed = True


def _is_hidden(tag: Tag) -> bool:
    for node in (tag, *tag.parents):
        if (
            node.name in {"template", "script", "style", "noscript"}
            or node.has_attr("hidden")
            or node.get("aria-hidden", "").strip().lower() == "true"
            or {"hidden", "invisible"}.intersection(node.get("class", ()))
            or HIDDEN_STYLE_PATTERN.search(node.get("style", ""))
        ):
            return True
    return False


def _visible_text(tag: Tag, *, separator: str = "") -> str:
    return _normalize_whitespace(
        separator.join(text for text in tag.strings if not _is_hidden(text.parent))
    )


def _ranking_rows(table: Tag) -> list[Tag]:
    rows = []
    for row in table.select(":scope > tr, :scope > tbody > tr"):
        if _is_hidden(row):
            continue
        rank_cell = row.find("td", recursive=False)
        if "table-group" in row.get("class", ()) or (
            rank_cell is not None
            and {"table-td", "w-12"}.issubset(rank_cell.get("class", ()))
        ):
            rows.append(row)
    return rows


def _title_link_path(href: str) -> str | None:
    try:
        link = urlsplit(href)
        hostname = link.hostname
    except ValueError:
        return None
    if link.scheme not in {"", "http", "https"} or hostname not in {
        None, "flixpatrol.com", "www.flixpatrol.com"
    }:
        return None
    path = unquote(link.path).rstrip("/")
    return path if re.fullmatch(r"/title/[^/]+", path) else None


def parse_chart_date(value: str) -> date:
    return datetime.strptime(value, "%B %d, %Y").date()


def validate_chart_dates(page_date: str, charts: Iterable[tuple[str, str]]) -> None:
    try:
        expected = parse_chart_date(page_date)
        for heading, chart_date in charts:
            if parse_chart_date(chart_date) != expected:
                raise ScraperError(f"Chart {heading!r} does not match the catalog date")
    except ValueError as exc:
        raise ScraperError("Invalid chart date") from exc


def validate_chart_ranks(
    ranks: list[int],
    heading: str,
    *,
    count: int = TOP10_LIMIT,
    max_count: int | None = TOP10_LIMIT,
) -> None:
    if type(count) is not int or count < 1 or (max_count is not None and count > max_count):
        raise ScraperError(f"Chart {heading!r} has an invalid source rank count {count!r}")
    if any(type(rank) is not int for rank in ranks) or ranks != list(range(1, count + 1)):
        raise ScraperError(
            f"Chart {heading!r} requires {count} ordered, consecutive ranks from 1, got {ranks}"
        )


def validate_tmdb_ids(entries: Iterable[tuple[str, int, str, int]], heading: str) -> None:
    titles: dict[tuple[str, int], tuple[str, int]] = {}
    for media_type, tmdb_id, title, first_rank in entries:
        previous_title, previous_rank = titles.setdefault(
            (media_type, tmdb_id), (title, first_rank)
        )
        if previous_rank != first_rank:
            raise ScraperError(
                f"Chart {heading!r} has duplicate TMDB ID {media_type}:{tmdb_id} "
                f"for {previous_title!r} and {title!r}"
            )


def _normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def _serialize_region(region: str) -> str:
    if region.casefold() == "the world":
        return "global"
    return region


def _serialize_category(category: str) -> str:
    return category.lower()


def _assign_catalog_ids(pending: list[_PendingChart]) -> list[Chart]:
    """Assign collision-scoped catalog_ids to every scraped chart.

    A chart is suffixed only when its (service, category) bucket has siblings;
    a qualifier alone is not enough. Each bucket keeps exactly one bare-ID
    member -- its genuine unqualified chart if it has one, otherwise the English
    variant promoted to the bare ID. This preserves the ID older consumers
    already resolve, without emitting a duplicate of the promoted chart under a
    redundant suffix, and without a hand-maintained alias list.
    """
    buckets: dict[tuple[str, str], list[_PendingChart]] = defaultdict(list)
    identities: set[ChartIdentity] = set()
    for item in pending:
        key = (item.service_slug, item.category_slug)
        identity = (*key, _qualifier_identity(item.qualifier))
        if identity in identities:
            raise ScraperError(f"Duplicate chart heading {item.chart.heading!r}")
        identities.add(identity)
        buckets[key].append(item)

    bare_holder = {key: _bare_id_holder(members) for key, members in buckets.items()}

    charts: list[Chart] = []
    for item in pending:
        key = (item.service_slug, item.category_slug)
        bare_id = f"{item.service_slug}.{item.category_slug}"

        if item is bare_holder[key]:
            charts.append(_with_catalog_id(item.chart, bare_id))
            continue

        variant = _lookup_variant(item.qualifier) if item.qualifier else None
        if variant is None:
            logger.error(
                "Dropping chart %r: unrecognized qualifier %r in colliding group %s. "
                "Add it to VARIANT_TABLE.",
                item.chart.heading,
                item.qualifier,
                bare_id,
            )
            continue

        charts.append(
            _with_catalog_id(item.chart, f"{bare_id}-{variant.id}", variant=variant)
        )

    return charts


def _bare_id_holder(members: list[_PendingChart]) -> _PendingChart:
    """The bucket member that keeps the unsuffixed catalog_id.

    A genuine unqualified chart wins; otherwise the English variant, falling back
    to the first scraped. Promotion is by variant *identity*, not scrape position,
    so a reordered feed cannot silently change which content the bare ID serves.
    """
    for member in members:
        if member.qualifier is None:
            return member
    for member in members:
        variant = _lookup_variant(member.qualifier) if member.qualifier else None
        if variant is not None and variant.id == "en":
            return member
    return members[0]


def _with_catalog_id(
    chart: Chart,
    catalog_id: str,
    *,
    variant: ChartVariant | None = None,
) -> Chart:
    return replace(chart, catalog_id=catalog_id, variant=variant)


def _split_label_qualifier(label: str) -> tuple[str, str | None]:
    """Split a heading label into its category text and trailing qualifier.

    Runs for every chart regardless of whether the category is recognized, so
    qualifier extraction is no longer coupled to category recognition.
    """
    match = QUALIFIER_PATTERN.fullmatch(label)
    if match is None:
        return label, None
    return match.group("base"), match.group("qualifier")


def _lookup_variant(qualifier: str) -> ChartVariant | None:
    entry = VARIANT_TABLE.get(_normalize_whitespace(qualifier).casefold())
    if entry is None:
        return None
    variant_id, variant_label = entry
    return ChartVariant(id=variant_id, label=variant_label)


def _qualifier_identity(qualifier: str | None) -> str | None:
    if qualifier is None:
        return None
    variant = _lookup_variant(qualifier)
    return variant.id if variant is not None else _normalize_whitespace(qualifier).casefold()


def _slugify_platform(platform: str) -> str:
    normalized = platform.casefold().replace("+", "")
    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    return normalized.strip("-")


# Canonical alias map, not a whitelist: only categories whose slug differs from
# their heading text need an entry. Anything absent is slugified as-is, so a
# category FlixPatrol invents tomorrow needs no change here.
KNOWN_CATEGORIES = {
    "movies": "movies",
    "tv shows": "series",
    "kids movies": "kids-movies",
    "overall": "overall",
}


def _category_slug(base_label: str) -> str:
    return KNOWN_CATEGORIES.get(base_label.casefold(), _slugify_text(base_label))


def _slugify_text(value: str) -> str:
    normalized = value.casefold().replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    return normalized.strip("-")


def write_result(
    result: ScrapeResult,
    output_path: str | Path,
    *,
    output_region: str | None = None,
) -> Path:
    output = Path(output_path)
    validate_chart_dates(result.date, ((chart.heading, chart.date) for chart in result.charts))
    atomic_write_text(output, result.to_json(output_region=output_region) + "\n")
    return output
