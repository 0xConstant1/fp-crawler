from __future__ import annotations

from dataclasses import dataclass
import os
import re
import threading
import unicodedata
from typing import Any

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from .rate_limit import RateLimiter

TMDB_API_BASE_URL = "https://api.themoviedb.org/3"
TMDB_DEFAULT_LANGUAGE = "en-US"
TMDB_DEFAULT_TIMEOUT_SECONDS = 30
TMDB_DEFAULT_MAX_REQUESTS_PER_SECOND = 50


class TMDBResolverError(RuntimeError):
    """Raised when TMDB matching cannot be performed."""


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
        self._search_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._match_cache: dict[tuple[str, str | None], TMDBMatch | None] = {}
        self._cache_lock = threading.Lock()
        self._search_inflight: dict[tuple[str, str], _InFlightRequest] = {}
        self._match_inflight: dict[tuple[str, str | None], _InFlightRequest] = {}
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
        session = requests.Session()
        retry = Retry(
            total=4,
            connect=4,
            read=4,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "flixpatrol-scraper/0.1.0",
            }
        )
        if access_token:
            session.headers["Authorization"] = f"Bearer {access_token}"
        return session

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

    def resolve(self, title: str, *, media_hint: str | None = None) -> TMDBMatch | None:
        cache_key = (normalize_title(title), media_hint)
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
            normalized_search_title = cache_key[0]

            match: TMDBMatch | None = None
            for endpoint in endpoint_order:
                results = self._search(endpoint, title)
                match = self._pick_best_match(
                    normalized_search_title,
                    results,
                    endpoint=endpoint,
                    media_hint=media_hint,
                )
                if match is not None:
                    break

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

    def _search(self, endpoint: str, query: str) -> list[dict[str, Any]]:
        cache_key = (endpoint, query)
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
        if self.api_key:
            params["api_key"] = self.api_key

        try:
            self._rate_limiter.acquire()
            response = self._get_session().get(
                f"{TMDB_API_BASE_URL}{endpoint}",
                params=params,
                timeout=self.timeout_seconds,
            )
            self._raise_for_bad_response(response, endpoint, query)

            payload = response.json()
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

    @staticmethod
    def _raise_for_bad_response(response: Response, endpoint: str, query: str) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise TMDBResolverError(
                f"TMDB request failed for endpoint {endpoint!r} and query {query!r}: "
                f"HTTP {response.status_code}"
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
    ) -> TMDBMatch | None:
        for result in results:
            media_type = self._extract_media_type(result, endpoint)
            if media_hint and media_type != media_hint and endpoint != "/search/multi":
                continue

            candidate_titles = [
                self._extract_result_title(result, media_type),
                self._extract_result_original_title(result, media_type),
            ]
            for candidate in candidate_titles:
                if candidate and normalize_title(candidate) == normalized_search_title:
                    return self._build_match(result, endpoint=endpoint)

        if len(results) == 1:
            result = results[0]
            media_type = self._extract_media_type(result, endpoint)
            if media_hint and media_type != media_hint and endpoint != "/search/multi":
                return None
            return self._build_match(result, endpoint=endpoint)

        return None

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
