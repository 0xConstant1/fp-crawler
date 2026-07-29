from __future__ import annotations

from dataclasses import dataclass
import logging
import threading

DEFAULT_SOLVE_URL = "https://flixpatrol.com/top10/"
DEFAULT_SOLVE_TIMEOUT_MS = 120_000
DEFAULT_SOLVE_DEADLINE_SECONDS = 600.0
CF_CLEARANCE_COOKIE = "cf_clearance"

logger = logging.getLogger(__name__)


class ChallengeSolverError(RuntimeError):
    """Raised when the challenge could not be solved."""


class ChallengeSolverUnavailableError(ChallengeSolverError):
    """Raised when the optional solver dependency is not installed."""


class ChallengeSolverTimeoutError(ChallengeSolverError):
    """Raised when the solver exceeds its wall-clock deadline.

    The upstream solver retries by unbounded recursion, so a challenge that
    never clears - typically a datacenter egress IP - would otherwise hang the
    caller indefinitely rather than failing.
    """


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
    """Drive a browser through the challenge and return fresh credentials.

    Bounded by ``deadline_seconds`` because the upstream solver retries forever.
    The worker is a daemon thread, so a solver stuck in that loop cannot keep
    the interpreter alive once the caller gives up.
    """
    solved: list[ChallengeSolution] = []
    failed: list[BaseException] = []

    def run() -> None:
        try:
            solved.append(
                _solve(url, headless=headless, timeout_ms=timeout_ms, proxy=proxy)
            )
        except BaseException as exc:  # noqa: BLE001
            failed.append(exc)

    worker = threading.Thread(target=run, daemon=True, name="cf-challenge-solver")
    worker.start()
    worker.join(deadline_seconds)

    if worker.is_alive():
        raise ChallengeSolverTimeoutError(
            f"Challenge solver did not finish within {deadline_seconds:.0f}s for "
            f"{url!r}. Each retry costs 10-20s and a slow or heavily scored "
            "egress can need many of them, so raise --solve-timeout before "
            "concluding the egress is unusable."
        )
    if failed:
        raise failed[0]
    return solved[0]


def _solve(
    url: str,
    *,
    headless: bool,
    timeout_ms: int,
    proxy: str | None,
) -> ChallengeSolution:
    fetcher = _load_stealthy_fetcher()

    logger.info("Solving challenge at %s", url)
    try:
        page = fetcher.fetch(
            url,
            headless=headless,
            solve_cloudflare=True,
            timeout=timeout_ms,
            google_search=False,
            **({"proxy": proxy} if proxy else {}),
        )
    except Exception as exc:  # noqa: BLE001
        raise ChallengeSolverError(
            f"Challenge solver failed for {url!r}: {exc}"
        ) from exc

    status = getattr(page, "status", None)
    if status != 200:
        raise ChallengeSolverError(
            f"Challenge solver returned HTTP {status} for {url!r}."
        )

    clearance = extract_cf_clearance(page)
    if clearance is None:
        raise ChallengeSolverError(
            f"Challenge solver got HTTP 200 for {url!r} but no "
            f"{CF_CLEARANCE_COOKIE} cookie."
        )

    user_agent = extract_user_agent(page)
    if user_agent is None:
        raise ChallengeSolverError(
            "Challenge solver did not report the browser User-Agent, which "
            f"{CF_CLEARANCE_COOKIE} is bound to."
        )

    logger.info("Solved challenge; clearance is %d chars", len(clearance))
    return ChallengeSolution(cf_clearance=clearance, user_agent=user_agent)


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


def _load_stealthy_fetcher():
    try:
        from scrapling.fetchers import StealthyFetcher
    except ImportError as exc:
        raise ChallengeSolverUnavailableError(
            "Solving the challenge requires Scrapling. Install it with "
            "'pip install flixpatrol-scraper[solver]' followed by "
            "'scrapling install' to download the browser runtime."
        ) from exc
    return StealthyFetcher
