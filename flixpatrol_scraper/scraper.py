from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
import json
import logging
import random
import re
from pathlib import Path
import threading
import time
from typing import Any

from bs4 import BeautifulSoup, Tag
from curl_cffi.requests import Response, Session
from curl_cffi.requests.exceptions import HTTPError, RequestException

from .http_client import format_response_diagnostics, get_with_retries
from .rate_limit import RateLimiter
from .tmdb import TMDBMatch, TMDBResolver, normalize_title

DEFAULT_TOP10_URL = "https://flixpatrol.com/top10/"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_OUTPUT_PATH = Path("flixpatrol_top10.json")
DEFAULT_TMDB_MAX_WORKERS = 16
DEFAULT_FLIXPATROL_MAX_REQUESTS_PER_SECOND = 1.5
DEFAULT_FLIXPATROL_REQUEST_JITTER_RANGE = (0.1, 0.4)
DEFAULT_BROWSER_IMPERSONATE = "chrome136"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/136.0.0.0 Safari/537.36"
)

CHART_HEADING_PATTERN = re.compile(
    r"^TOP (?P<category>Movies|TV Shows) on (?P<platform>.+) on (?P<date>.+)$"
)
PAGE_TITLE_PATTERN = re.compile(
    r"^TOP 10 on Streaming in (?P<region>.+) on (?P<date>.+)$"
)
REGIONAL_SERVICE_HEADING_PATTERN = re.compile(
    r"^(?P<platform>.+) TOP 10 in (?P<region>.+) on (?P<date>.+)$"
)
REGIONAL_SUBHEADING_PATTERN = re.compile(
    r"^TOP 10 (?P<label>.+?)(?: \((?P<qualifier>.+)\))?$"
)
QUALIFIER_PATTERN = re.compile(
    r"^(?P<base>.+?) \((?P<qualifier>(?:in|from) [^)]+)\)$"
)

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
    category = rest[: -len(suffix)] if rest.endswith(suffix) else rest
    return service, category, variant["id"]


class ScraperError(RuntimeError):
    """Raised when the scraper cannot fetch or parse a FlixPatrol page."""


class NoChartsFoundError(ScraperError):
    """Raised when a valid FlixPatrol page exposes no TOP 10 charts."""


@dataclass(frozen=True, slots=True)
class ChartEntry:
    rank: int
    title: str
    tmdb: TMDBMatch | None = None

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

    @property
    def title_count(self) -> int:
        return len(self.entries)

    @property
    def titles(self) -> list[str]:
        return [entry.title for entry in self.entries]

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
        max_titles_per_chart: int = 10,
        tmdb_resolver: TMDBResolver | None = None,
        tmdb_max_workers: int = DEFAULT_TMDB_MAX_WORKERS,
        max_requests_per_second: float = DEFAULT_FLIXPATROL_MAX_REQUESTS_PER_SECOND,
        request_jitter_range: tuple[float, float] = DEFAULT_FLIXPATROL_REQUEST_JITTER_RANGE,
    ) -> None:
        self._shared_session = session
        self._thread_local = threading.local()
        self.timeout_seconds = timeout_seconds
        self.max_titles_per_chart = max_titles_per_chart
        self.tmdb_resolver = tmdb_resolver
        self.tmdb_max_workers = max(1, tmdb_max_workers)
        self._rate_limiter = RateLimiter(
            max_requests_per_second=max_requests_per_second
        )
        self.request_jitter_range = request_jitter_range

    @staticmethod
    def _build_session() -> Session:
        return Session(
            impersonate=DEFAULT_BROWSER_IMPERSONATE,
            default_headers=True,
            default_encoding="utf-8",
            headers={
                "User-Agent": DEFAULT_USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )

    def _get_session(self) -> Session:
        if self._shared_session is not None:
            return self._shared_session

        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = self._build_session()
            self._thread_local.session = session
        return session

    def _sleep_request_jitter(self) -> None:
        minimum, maximum = self.request_jitter_range
        if maximum <= 0:
            return
        time_to_sleep = random.uniform(max(minimum, 0.0), max(maximum, 0.0))
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)

    def scrape_url(self, url: str = DEFAULT_TOP10_URL) -> ScrapeResult:
        self._sleep_request_jitter()
        self._rate_limiter.acquire()
        try:
            response = get_with_retries(
                self._get_session(),
                url,
                timeout_seconds=self.timeout_seconds,
            )
        except RequestException as exc:
            raise ScraperError(f"Failed to fetch {url!r}: {exc}") from exc
        self._raise_for_bad_response(response, url)
        return self.parse_html(response.text, source=response.url)

    def parse_html(self, html: str, *, source: str) -> ScrapeResult:
        soup = BeautifulSoup(html, "html.parser")
        page_title = self._extract_page_title(soup)
        page_region, page_date = self._extract_page_metadata(page_title)
        charts = _assign_catalog_ids(
            self._extract_charts(soup, page_region=page_region)
        )

        if not charts:
            raise NoChartsFoundError("No TOP 10 charts were found in the provided HTML.")

        if self.tmdb_resolver is not None:
            charts = self._enrich_charts_with_tmdb(charts)

        return ScrapeResult(
            source=source,
            page_title=page_title,
            region=page_region,
            date=page_date,
            scraped_at_utc=datetime.now(UTC).isoformat(),
            charts=charts,
        )

    @staticmethod
    def _raise_for_bad_response(response: Response, url: str) -> None:
        try:
            response.raise_for_status()
        except HTTPError as exc:
            raise ScraperError(
                f"Failed to fetch {url!r}: {format_response_diagnostics(response)}"
            ) from exc

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

    def _extract_charts(
        self, soup: BeautifulSoup, *, page_region: str
    ) -> list[_PendingChart]:
        regional_charts = self._extract_regional_charts(soup)
        if regional_charts:
            return regional_charts
        return self._extract_global_charts(soup, region=page_region)

    def _extract_global_charts(
        self, soup: BeautifulSoup, *, region: str
    ) -> list[_PendingChart]:
        charts: list[_PendingChart] = []

        for heading_tag in soup.find_all("h2"):
            heading_text = _normalize_whitespace(heading_tag.get_text(" ", strip=True))
            heading_match = CHART_HEADING_PATTERN.fullmatch(heading_text)
            if heading_match is None:
                continue

            table = self._find_table_for_heading(heading_tag)
            if table is None:
                raise ScraperError(f"Could not find a table for chart {heading_text!r}.")

            entries = self._extract_entries_from_table(table, heading_text)
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
                        is_full_top10=len(entries) == self.max_titles_per_chart,
                        entries=entries,
                    ),
                    service_slug=_slugify_platform(platform),
                    category_slug=_category_slug(base_label),
                    qualifier=qualifier,
                )
            )

        return charts

    def _extract_regional_charts(self, soup: BeautifulSoup) -> list[_PendingChart]:
        charts: list[_PendingChart] = []

        for heading_tag in soup.find_all("h2"):
            heading_text = _normalize_whitespace(heading_tag.get_text(" ", strip=True))
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

            for subheading_tag in section.find_all("h3"):
                subheading_text = _normalize_whitespace(
                    subheading_tag.get_text(" ", strip=True)
                )
                subheading_match = REGIONAL_SUBHEADING_PATTERN.fullmatch(subheading_text)
                if subheading_match is None:
                    continue

                label = _normalize_whitespace(subheading_text.removeprefix("TOP 10 "))
                base_label, qualifier = _split_label_qualifier(label)

                table = self._find_table_for_subheading(subheading_tag, section)
                if table is None:
                    raise ScraperError(
                        f"Could not find a table for subheading {subheading_text!r} "
                        f"under service {heading_text!r}."
                    )

                entries = self._extract_entries_from_table(table, subheading_text)
                chart_heading = f"TOP {label} on {platform} in {region} on {date}"

                charts.append(
                    _PendingChart(
                        chart=Chart(
                            heading=chart_heading,
                            catalog_id="",
                            category=base_label,
                            platform=platform,
                            date=date,
                            is_full_top10=len(entries) == self.max_titles_per_chart,
                            entries=entries,
                        ),
                        service_slug=_slugify_platform(platform),
                        category_slug=_category_slug(base_label),
                        qualifier=qualifier,
                    )
                )

        return charts

    @staticmethod
    def _find_table_for_heading(heading_tag: Tag) -> Tag | None:
        next_table = heading_tag.find_next("table")
        if next_table is None:
            return None

        previous_heading = next_table.find_previous("h2")
        if previous_heading is heading_tag:
            return next_table

        return None

    @staticmethod
    def _find_table_for_subheading(subheading_tag: Tag, section: Tag) -> Tag | None:
        next_table = subheading_tag.find_next("table")
        if next_table is None:
            return None

        if section not in next_table.parents:
            return None

        previous_subheading = next_table.find_previous("h3")
        if previous_subheading is subheading_tag:
            return next_table

        return None

    def _extract_entries_from_table(self, table: Tag, heading: str) -> list[ChartEntry]:
        entries: list[ChartEntry] = []

        for row in table.find_all("tr"):
            title = self._extract_title_from_row(row)
            if title:
                entries.append(
                    ChartEntry(
                        rank=len(entries) + 1,
                        title=title,
                    )
                )
            if len(entries) == self.max_titles_per_chart:
                break

        if not entries:
            raise ScraperError(
                f"Chart {heading!r} did not contain any title rows."
            )

        return entries

    def _enrich_charts_with_tmdb(self, charts: list[Chart]) -> list[Chart]:
        work_items: dict[tuple[str, str | None], list[tuple[int, int, ChartEntry]]] = {}
        for chart_index, chart in enumerate(charts):
            media_hint = self._chart_media_type(chart.category)
            for entry_index, entry in enumerate(chart.entries):
                work_key = (normalize_title(entry.title), media_hint)
                work_items.setdefault(work_key, []).append(
                    (chart_index, entry_index, entry)
                )

        resolved_matches: dict[tuple[int, int], TMDBMatch | None] = {}
        max_workers = min(self.tmdb_max_workers, len(work_items))

        if max_workers <= 1:
            for (_, media_hint), positions in work_items.items():
                representative_entry = positions[0][2]
                match = self.tmdb_resolver.resolve(
                    representative_entry.title,
                    media_hint=media_hint,
                )
                for chart_index, entry_index, _ in positions:
                    resolved_matches[(chart_index, entry_index)] = match
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_key = {
                    executor.submit(
                        self.tmdb_resolver.resolve,
                        positions[0][2].title,
                        media_hint=media_hint,
                    ): work_key
                    for work_key, positions in work_items.items()
                    for _, media_hint in [work_key]
                }

                for future in as_completed(future_to_key):
                    work_key = future_to_key[future]
                    match = future.result()
                    for chart_index, entry_index, _ in work_items[work_key]:
                        resolved_matches[(chart_index, entry_index)] = match

        enriched_charts: list[Chart] = []
        for chart_index, chart in enumerate(charts):
            enriched_entries = [
                ChartEntry(
                    rank=entry.rank,
                    title=entry.title,
                    tmdb=resolved_matches[(chart_index, entry_index)],
                )
                for entry_index, entry in enumerate(chart.entries)
            ]
            enriched_charts.append(
                Chart(
                    heading=chart.heading,
                    catalog_id=chart.catalog_id,
                    category=chart.category,
                    platform=chart.platform,
                    date=chart.date,
                    is_full_top10=chart.is_full_top10,
                    entries=enriched_entries,
                    variant=chart.variant,
                )
            )

        return enriched_charts

    @staticmethod
    def _chart_media_type(category: str) -> str | None:
        if category in {"Movies", "Kids Movies"}:
            return "movie"
        if category in {"TV Shows", "Kids TV Shows"}:
            return "tv"
        return None

    @staticmethod
    def _extract_title_from_row(row: Tag) -> str:
        cells = row.find_all("td", recursive=False)
        for cell in cells:
            if cell.find("a") is not None or cell.find("img", alt=True) is not None:
                title = FlixPatrolScraper._extract_title_from_cell(cell)
                if title:
                    return title
        return ""

    @staticmethod
    def _extract_title_from_cell(cell: Tag) -> str:
        image = cell.find("img", alt=True)
        if image is not None:
            return _normalize_whitespace(image["alt"])

        link = cell.find("a")
        if link is not None:
            return _normalize_whitespace(link.get_text(" ", strip=True))

        return _normalize_whitespace(cell.get_text(" ", strip=True))


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
    for item in pending:
        buckets[(item.service_slug, item.category_slug)].append(item)

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
    return Chart(
        heading=chart.heading,
        catalog_id=catalog_id,
        category=chart.category,
        platform=chart.platform,
        date=chart.date,
        is_full_top10=chart.is_full_top10,
        entries=chart.entries,
        variant=variant,
    )


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
    output.write_text(
        result.to_json(output_region=output_region) + "\n",
        encoding="utf-8",
    )
    return output
