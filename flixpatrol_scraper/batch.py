from __future__ import annotations

import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from .scraper import FlixPatrolScraper, RecoveryOpenError


def build_region_url(region: str) -> str:
    if region == "global":
        return "https://flixpatrol.com/top10/"
    return f"https://flixpatrol.com/top10/streaming/{region}/"


def iter_region_results(
    scraper: FlixPatrolScraper, targets: list[str], *, max_workers: int = 4
):
    def scrape(target: str):
        started = time.monotonic()
        try:
            result = scraper.scrape_url(build_region_url(target))
        except Exception as exc:  # noqa: BLE001
            result = exc
        return target, result, round(time.monotonic() - started, 3)

    workers = 1 if scraper.transport == "browser" else max(1, max_workers)
    remaining = iter(targets)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending = {}

        def schedule() -> None:
            while len(pending) < workers and not scraper.recovery_open:
                target = next(remaining, None)
                if target is None:
                    break
                pending[executor.submit(scrape, target)] = target

        schedule()
        while pending:
            completed, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                pending.pop(future)
                yield future.result()
            schedule()
        for target in remaining:
            yield (
                target,
                RecoveryOpenError(
                    "Deferred until the next run after shared recovery stopped"
                ),
                0.0,
            )
