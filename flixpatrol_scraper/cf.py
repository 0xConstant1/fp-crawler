from __future__ import annotations

from dataclasses import dataclass

DEFAULT_SOLVE_URL = "https://flixpatrol.com/top10/"
DEFAULT_SOLVE_TIMEOUT_MS = 120_000
DEFAULT_SOLVE_DEADLINE_SECONDS = 180.0
CF_CLEARANCE_COOKIE = "cf_clearance"


class ChallengeSolverError(RuntimeError):
    """Raised when the challenge could not be solved."""


class ChallengeSolverUnavailableError(ChallengeSolverError):
    """Raised when the optional solver dependency is not installed."""


class ChallengeSolverTimeoutError(ChallengeSolverError):
    """Raised when the browser solve exceeds its wall-clock deadline."""


@dataclass(frozen=True, slots=True)
class ChallengeSolution:
    """A clearance cookie and the User-Agent it is bound to."""

    cf_clearance: str
    user_agent: str


def solve_challenge(
    url: str = DEFAULT_SOLVE_URL,
    *,
    headless: bool = True,
    timeout_ms: int = DEFAULT_SOLVE_TIMEOUT_MS,
    proxy: str | None = None,
    deadline_seconds: float = DEFAULT_SOLVE_DEADLINE_SECONDS,
) -> ChallengeSolution:
    """Solve in a disposable process so a deadline also stops the browser."""
    from .browser import BrowserClient, BrowserError, BrowserTimeoutError

    client = BrowserClient(headless=headless, timeout_ms=timeout_ms, proxy=proxy)
    try:
        status, clearance, user_agent = client.fetch(
            url, deadline_seconds=deadline_seconds, credentials_only=True
        )
        if status != 200 or not clearance or not user_agent:
            raise ChallengeSolverError(
                f"Challenge solver did not return valid clearance for {url!r} (HTTP {status})."
            )
        return ChallengeSolution(cf_clearance=clearance, user_agent=user_agent)
    except BrowserTimeoutError as exc:
        raise ChallengeSolverTimeoutError(str(exc)) from exc
    except BrowserError as exc:
        raise ChallengeSolverError(str(exc)) from exc
    finally:
        client.close()


def extract_cf_clearance(page: object) -> str | None:
    """Pull the clearance cookie out of a solver response."""
    cookies = getattr(page, "cookies", None) or ()
    if isinstance(cookies, dict):
        value = cookies.get(CF_CLEARANCE_COOKIE)
        return str(value) if value else None

    for cookie in cookies:
        if isinstance(cookie, dict) and cookie.get("name") == CF_CLEARANCE_COOKIE:
            value = cookie.get("value")
            return str(value) if value else None
    return None


def extract_user_agent(page: object) -> str | None:
    """Read the User-Agent the solving browser sent."""
    headers = getattr(page, "request_headers", None) or {}
    try:
        headers = dict(headers)
    except (TypeError, ValueError):
        return None

    for name, value in headers.items():
        if str(name).lower() == "user-agent" and value:
            return str(value)
    return None
