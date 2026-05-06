from __future__ import annotations

import random
import time
from collections.abc import Iterable
from email.utils import parsedate_to_datetime
from typing import Any

from curl_cffi.requests import Response, Session
from curl_cffi.requests.exceptions import RequestException

DEFAULT_RETRY_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


def get_with_retries(
    session: Session,
    url: str,
    *,
    timeout_seconds: int,
    total_retries: int = 4,
    backoff_factor: float = 1.0,
    status_forcelist: Iterable[int] = DEFAULT_RETRY_STATUS_CODES,
    **kwargs: Any,
) -> Response:
    retry_statuses = frozenset(status_forcelist)
    last_response: Response | None = None

    for attempt in range(total_retries + 1):
        try:
            response = session.get(url, timeout=timeout_seconds, **kwargs)
        except RequestException:
            if attempt >= total_retries:
                raise
            _sleep_before_retry(None, attempt, backoff_factor)
            continue

        if response.status_code not in retry_statuses:
            return response

        last_response = response
        if attempt >= total_retries:
            return response

        _sleep_before_retry(response, attempt, backoff_factor)

    if last_response is None:
        raise RuntimeError("HTTP retry loop exited without a response.")
    return last_response


def format_response_diagnostics(response: Response) -> str:
    details = [f"HTTP {response.status_code}"]
    retry_after = response.headers.get("retry-after")
    cf_ray = response.headers.get("cf-ray")

    if retry_after:
        details.append(f"retry-after={retry_after}")
    if cf_ray:
        details.append(f"cf-ray={cf_ray}")

    return ", ".join(details)


def _sleep_before_retry(
    response: Response | None,
    attempt: int,
    backoff_factor: float,
) -> None:
    retry_after_seconds = (
        _parse_retry_after(response.headers.get("retry-after")) if response else None
    )
    if retry_after_seconds is None:
        retry_after_seconds = backoff_factor * (2**attempt) + random.uniform(0.0, 0.5)

    if retry_after_seconds > 0:
        time.sleep(retry_after_seconds)


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None

    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None

    return max(0.0, retry_at.timestamp() - time.time())
