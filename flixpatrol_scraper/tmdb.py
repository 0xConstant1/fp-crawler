from __future__ import annotations

import logging
import os
import re
import threading
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from curl_cffi.requests import Response, Session
from curl_cffi.requests.exceptions import HTTPError, RequestException

from .http_client import format_response_diagnostics, get_with_retries
from .rate_limit import RateLimiter

TMDB_API_BASE_URL = "https://api.themoviedb.org/3"
TMDB_DEFAULT_LANGUAGE = "en-US"
TMDB_DEFAULT_TIMEOUT_SECONDS = 30
TMDB_DEFAULT_MAX_REQUESTS_PER_SECOND = 50

logger = logging.getLogger(__name__)


class TMDBResolverError(RuntimeError):
    """Raised when TMDB matching cannot be performed."""


class _AmbiguousMatchError(TMDBResolverError):
    """More than one work matches the available title information."""

    def __init__(self, matches: tuple[TMDBMatch, ...]) -> None:
        self.matches = matches
        super().__init__(
            ", ".join(f"{match.media_type}:{match.id} ({match.release_date})" for match in matches)
        )


@dataclass(slots=True)
class _InFlightRequest:
    event: threading.Event
    result: Any = None
    exception: BaseException | None = None


@dataclass(frozen=True, slots=True)
class TMDBMatch:
    id: int
    media_type: str
    matched_title: str
    original_title: str | None
    release_date: str | None
    search_endpoint: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "media_type": self.media_type,
            "release_date": self.release_date,
        }


_MatchResult = TMDBMatch | tuple[TMDBMatch, ...] | None


class TMDBResolver:
    """Resolve scraped titles to TMDB IDs using TMDB search endpoints."""

    def __init__(
        self,
        *,
        access_token: str | None = None,
        api_key: str | None = None,
        session: Session | None = None,
        timeout_seconds: int = TMDB_DEFAULT_TIMEOUT_SECONDS,
        language: str = TMDB_DEFAULT_LANGUAGE,
        include_adult: bool = False,
        max_requests_per_second: float = TMDB_DEFAULT_MAX_REQUESTS_PER_SECOND,
    ) -> None:
        self.access_token = access_token
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.language = language
        self.include_adult = include_adult
        self.max_requests_per_second = max(1, max_requests_per_second)
        self._shared_session = session
        self._thread_local = threading.local()
        self._search_cache: dict[tuple[str, str, str | None], list[dict[str, Any]]] = {}
        self._match_cache: dict[tuple[str, str | None, str | None, str], _MatchResult] = {}
        self._reported_unmatched: set[
            tuple[str, str | None, str | None, str | None, _MatchResult, tuple[str, str] | None]
        ] = set()
        self._provider_cache: dict[tuple[str, int], frozenset[str]] = {}
        self._cache_lock = threading.Lock()
        self._search_inflight: dict[tuple[str, str, str | None], _InFlightRequest] = {}
        self._match_inflight: dict[tuple[str, str | None, str | None, str], _InFlightRequest] = {}
        self._provider_inflight: dict[tuple[str, int], _InFlightRequest] = {}
        self._rate_limiter = RateLimiter(
            max_requests_per_second=self.max_requests_per_second
        )

        if not self.access_token and not self.api_key:
            raise TMDBResolverError(
                "TMDB credentials are required. Provide TMDB_ACCESS_TOKEN or TMDB_API_KEY."
            )

    @classmethod
    def from_env(
        cls,
        *,
        session: Session | None = None,
        timeout_seconds: int = TMDB_DEFAULT_TIMEOUT_SECONDS,
        language: str = TMDB_DEFAULT_LANGUAGE,
        include_adult: bool = False,
        max_requests_per_second: float = TMDB_DEFAULT_MAX_REQUESTS_PER_SECOND,
    ) -> TMDBResolver | None:
        access_token = os.getenv("TMDB_ACCESS_TOKEN")
        api_key = os.getenv("TMDB_API_KEY")
        if not access_token and not api_key:
            return None
        return cls(
            access_token=access_token,
            api_key=api_key,
            session=session,
            timeout_seconds=timeout_seconds,
            language=language,
            include_adult=include_adult,
            max_requests_per_second=max_requests_per_second,
        )

    @staticmethod
    def _build_session(*, access_token: str | None) -> Session:
        headers = {
            "Accept": "application/json",
            "User-Agent": "flixpatrol-scraper/0.1.0",
        }
        if access_token:
            headers["Authorization"] = f"Bearer {access_token}"

        return Session(
            default_encoding="utf-8",
            headers=headers,
        )

    def _get_session(self) -> Session:
        if self._shared_session is not None:
            return self._shared_session

        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = self._build_session(access_token=self.access_token)
            self._thread_local.session = session
        return session

    def _acquire_inflight(
        self,
        inflight_requests: dict[tuple[Any, ...], _InFlightRequest],
        cache_key: tuple[Any, ...],
    ) -> tuple[_InFlightRequest, bool]:
        with self._cache_lock:
            request = inflight_requests.get(cache_key)
            if request is not None:
                return request, False

            request = _InFlightRequest(event=threading.Event())
            inflight_requests[cache_key] = request
            return request, True

    def _complete_inflight(
        self,
        inflight_requests: dict[tuple[Any, ...], _InFlightRequest],
        cache_key: tuple[Any, ...],
        *,
        result: Any = None,
        exception: BaseException | None = None,
    ) -> None:
        with self._cache_lock:
            request = inflight_requests.pop(cache_key, None)
            if request is None:
                return
            request.result = result
            request.exception = exception
            request.event.set()

    def resolve(
        self,
        title: str,
        *,
        media_hint: str | None = None,
        release_year: str | None = None,
        original_provider: str | None = None,
        title_details: Callable[[], tuple[str, str] | None] | None = None,
        fallback: TMDBMatch | None = None,
    ) -> TMDBMatch | None:
        result = self.find_match(
            title, media_hint=media_hint, release_year=release_year, original_provider=original_provider
        )
        return self.complete_match(
            title, result, media_hint=media_hint, release_year=release_year,
            original_provider=original_provider, title_details=title_details, fallback=fallback,
        )

    def complete_match(
        self,
        title: str,
        result: _MatchResult,
        *,
        media_hint: str | None = None,
        release_year: str | None = None,
        original_provider: str | None = None,
        title_details: Callable[[], tuple[str, str] | None] | None = None,
        fallback: TMDBMatch | None = None,
    ) -> TMDBMatch | None:
        if isinstance(result, TMDBMatch):
            return result
        if fallback is not None:
            return fallback
        details = title_details() if result is not None and title_details is not None else None
        if details is not None:
            detail_media, detail_year = details
            if detail_media in {"movie", "tv"} and media_hint in {None, detail_media}:
                endpoint = f"/search/{detail_media}"
                try:
                    match = self._pick_best_match(
                        normalize_title(title),
                        self._search(endpoint, title, release_year=detail_year),
                        endpoint=endpoint,
                        media_hint=detail_media,
                        release_year=detail_year,
                    )
                except (TMDBResolverError, ValueError, TypeError, KeyError, AttributeError) as exc:
                    logger.debug("Could not resolve TMDB match for %r with title details: %s", title, exc)
                else:
                    if match is not None:
                        return match

        warning_key = (title, media_hint, release_year, original_provider, result, details)
        with self._cache_lock:
            if warning_key in self._reported_unmatched:
                return None
            self._reported_unmatched.add(warning_key)
        if result is None:
            logger.warning(
                "No verified TMDB match for %r (%s, %s)",
                title, media_hint, release_year,
            )
            return None
        logger.warning(
            "Ambiguous TMDB match for %r (%s): %s",
            title, media_hint, _AmbiguousMatchError(result),
        )
        return None

    def find_match(
        self,
        title: str,
        *,
        media_hint: str | None = None,
        release_year: str | None = None,
        original_provider: str | None = None,
    ) -> _MatchResult:
        provider = normalize_title(original_provider or "")
        cache_key = (title, media_hint, release_year, provider)
        with self._cache_lock:
            if cache_key in self._match_cache:
                return self._match_cache[cache_key]

        inflight_request, is_owner = self._acquire_inflight(
            self._match_inflight,
            cache_key,
        )
        if not is_owner:
            inflight_request.event.wait()
            if inflight_request.exception is not None:
                raise inflight_request.exception
            return inflight_request.result

        with self._cache_lock:
            cached_match = self._match_cache.get(cache_key)
            has_cached_match = cache_key in self._match_cache
        if has_cached_match:
            self._complete_inflight(
                self._match_inflight,
                cache_key,
                result=cached_match,
            )
            return cached_match

        try:
            endpoint_order = self._build_search_order(media_hint)
            normalized_search_title = normalize_title(title)

            candidates: list[TMDBMatch] = []
            for endpoint in endpoint_order:
                if release_year is not None and endpoint != "/search/multi":
                    results = self._search(endpoint, title, release_year=release_year)
                else:
                    results = self._search(endpoint, title)
                try:
                    candidate = self._pick_best_match(
                        normalized_search_title,
                        results,
                        endpoint=endpoint,
                        media_hint=media_hint,
                        release_year=release_year,
                    )
                except _AmbiguousMatchError as exc:
                    candidates.extend(exc.matches)
                else:
                    if candidate is not None:
                        candidates.append(candidate)
                if candidates and (media_hint or endpoint == "/search/multi"):
                    break

            cacheable = True
            match: _MatchResult = next(iter(candidates), None)
            if len(candidates) > 1:
                match = tuple(candidates)
                if provider:
                    try:
                        match = self._pick_provider_match(match, provider, title) or match
                    except (TMDBResolverError, KeyError, TypeError, ValueError, AttributeError) as exc:
                        logger.debug("Could not verify the TMDB provider for %r: %s", title, exc)
                        cacheable = False

            if cacheable:
                with self._cache_lock:
                    self._match_cache[cache_key] = match
            self._complete_inflight(
                self._match_inflight,
                cache_key,
                result=match,
            )
            return match
        except BaseException as exc:
            self._complete_inflight(
                self._match_inflight,
                cache_key,
                exception=exc,
            )
            raise

    def _search(
        self, endpoint: str, query: str, *, release_year: str | None = None
    ) -> list[dict[str, Any]]:
        if endpoint == "/search/multi":
            release_year = None
        cache_key = (endpoint, query, release_year)
        with self._cache_lock:
            if cache_key in self._search_cache:
                return self._search_cache[cache_key]

        inflight_request, is_owner = self._acquire_inflight(
            self._search_inflight,
            cache_key,
        )
        if not is_owner:
            inflight_request.event.wait()
            if inflight_request.exception is not None:
                raise inflight_request.exception
            return inflight_request.result

        with self._cache_lock:
            cached_results = self._search_cache.get(cache_key)
            has_cached_results = cache_key in self._search_cache
        if has_cached_results:
            self._complete_inflight(
                self._search_inflight,
                cache_key,
                result=cached_results,
            )
            return cached_results

        params: dict[str, Any] = {
            "query": query,
            "language": self.language,
            "include_adult": str(self.include_adult).lower(),
            "page": 1,
        }
        if release_year is not None:
            year_param = "first_air_date_year" if endpoint == "/search/tv" else "primary_release_year"
            params[year_param] = release_year
        try:
            payload = self._request_json(endpoint, query, params)
            results = payload.get("results", [])
            filtered_results = [
                result
                for result in results
                if endpoint != "/search/multi" or result.get("media_type") in {"movie", "tv"}
            ]
            with self._cache_lock:
                self._search_cache[cache_key] = filtered_results
            self._complete_inflight(
                self._search_inflight,
                cache_key,
                result=filtered_results,
            )
            return filtered_results
        except BaseException as exc:
            self._complete_inflight(
                self._search_inflight,
                cache_key,
                exception=exc,
            )
            raise

    def _request_json(self, endpoint: str, title: str, params: dict[str, Any]) -> dict[str, Any]:
        if self.api_key:
            params["api_key"] = self.api_key
        try:
            self._rate_limiter.acquire()
            response = get_with_retries(
                self._get_session(),
                f"{TMDB_API_BASE_URL}{endpoint}",
                params=params,
                timeout_seconds=self.timeout_seconds,
            )
        except RequestException as exc:
            raise TMDBResolverError(
                f"TMDB request failed for endpoint {endpoint!r} and query {title!r}: "
                f"{type(exc).__name__}"
            ) from exc
        self._raise_for_bad_response(response, endpoint, title)
        return response.json()

    def _pick_provider_match(
        self, matches: tuple[TMDBMatch, ...], provider: str, title: str
    ) -> TMDBMatch | None:
        candidate = None
        for match in matches:
            if provider in self._subscription_providers(match, title):
                if candidate is not None:
                    return None
                candidate = match
        return candidate

    def _subscription_providers(self, match: TMDBMatch, title: str) -> frozenset[str]:
        cache_key = (match.media_type, match.id)
        with self._cache_lock:
            if cache_key in self._provider_cache:
                return self._provider_cache[cache_key]

        inflight_request, is_owner = self._acquire_inflight(
            self._provider_inflight,
            cache_key,
        )
        if not is_owner:
            inflight_request.event.wait()
            if inflight_request.exception is not None:
                raise inflight_request.exception
            return inflight_request.result

        with self._cache_lock:
            providers = self._provider_cache.get(cache_key)
        try:
            if providers is None:
                endpoint = f"/{match.media_type}/{match.id}/watch/providers"
                markets = self._request_json(endpoint, title, {})["results"]
                providers = frozenset(
                    normalize_title(offer["provider_name"])
                    for market in markets.values()
                    for offer in market.get("flatrate", [])
                )
                with self._cache_lock:
                    self._provider_cache[cache_key] = providers
        except BaseException as exc:
            self._complete_inflight(self._provider_inflight, cache_key, exception=exc)
            raise
        self._complete_inflight(self._provider_inflight, cache_key, result=providers)
        return providers

    @staticmethod
    def _raise_for_bad_response(response: Response, endpoint: str, query: str) -> None:
        try:
            response.raise_for_status()
        except HTTPError as exc:
            raise TMDBResolverError(
                f"TMDB request failed for endpoint {endpoint!r} and query {query!r}: "
                f"{format_response_diagnostics(response)}"
            ) from exc

    @staticmethod
    def _build_search_order(media_hint: str | None) -> list[str]:
        if media_hint == "movie":
            return ["/search/movie", "/search/multi"]
        if media_hint == "tv":
            return ["/search/tv", "/search/multi"]
        return ["/search/multi", "/search/movie", "/search/tv"]

    def _pick_best_match(
        self,
        normalized_search_title: str,
        results: list[dict[str, Any]],
        *,
        endpoint: str,
        media_hint: str | None,
        release_year: str | None = None,
    ) -> TMDBMatch | None:
        matches: dict[tuple[str, int], TMDBMatch] = {}
        for result in results:
            media_type = self._extract_media_type(result, endpoint)
            if media_hint and media_type != media_hint:
                continue
            release_date = self._extract_result_release_date(result, media_type)
            if release_year and (release_date or "")[:4] != release_year:
                continue

            candidate_titles = [
                self._extract_result_title(result, media_type),
                self._extract_result_original_title(result, media_type),
            ]
            if any(
                title and normalize_title(title) == normalized_search_title
                for title in candidate_titles
            ):
                match = self._build_match(result, endpoint=endpoint)
                matches[(match.media_type, match.id)] = match

        if len(matches) > 1:
            raise _AmbiguousMatchError(tuple(matches.values()))
        return next(iter(matches.values()), None)

    def _build_match(
        self,
        result: dict[str, Any],
        *,
        endpoint: str,
    ) -> TMDBMatch:
        media_type = self._extract_media_type(result, endpoint)
        matched_title = self._extract_result_title(result, media_type)
        original_title = self._extract_result_original_title(result, media_type)
        release_date = self._extract_result_release_date(result, media_type)

        return TMDBMatch(
            id=int(result["id"]),
            media_type=media_type,
            matched_title=matched_title,
            original_title=original_title,
            release_date=release_date,
            search_endpoint=endpoint,
        )

    @staticmethod
    def _extract_media_type(result: dict[str, Any], endpoint: str) -> str:
        if endpoint == "/search/movie":
            return "movie"
        if endpoint == "/search/tv":
            return "tv"
        return str(result.get("media_type") or "")

    @staticmethod
    def _extract_result_title(result: dict[str, Any], media_type: str) -> str:
        if media_type == "movie":
            return str(result.get("title") or "")
        return str(result.get("name") or "")

    @staticmethod
    def _extract_result_original_title(result: dict[str, Any], media_type: str) -> str | None:
        if media_type == "movie":
            value = result.get("original_title")
        else:
            value = result.get("original_name")
        return str(value) if value else None

    @staticmethod
    def _extract_result_release_date(result: dict[str, Any], media_type: str) -> str | None:
        if media_type == "movie":
            value = result.get("release_date")
        else:
            value = result.get("first_air_date")
        return str(value) if value else None


def normalize_title(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value.casefold())
    stripped = "".join(
        character
        for character in normalized
        if unicodedata.category(character) != "Mn"
    )
    stripped = stripped.replace("&", " and ")
    stripped = re.sub(r"[^\w\s]", " ", stripped)
    return re.sub(r"\s+", " ", stripped).strip()
