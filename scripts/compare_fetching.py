from __future__ import annotations

import argparse
import os
import time
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

from flixpatrol_scraper.__main__ import _env_flag, parse_region_targets
from flixpatrol_scraper.batch import iter_region_results
from flixpatrol_scraper.catalogs import validate_catalog
from flixpatrol_scraper.regions import SUPPORTED_REGION_SLUGS
from flixpatrol_scraper.scraper import FlixPatrolScraper, ScraperError
from flixpatrol_scraper.storage import atomic_write_json


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare HTTP clearance transfer with persistent browser fetching; never publish."
    )
    parser.add_argument("--region", default="global,algeria,united-states")
    parser.add_argument(
        "--order", choices=("http-first", "browser-first"), default="http-first"
    )
    parser.add_argument("--solve-timeout", type=float, default=180)
    parser.add_argument(
        "--output", type=Path, default=Path("exports/comparison/report.json")
    )
    args = parser.parse_args(argv)
    targets = parse_region_targets(
        region_arg=args.region,
        all_regions=False,
        supported_regions=list(SUPPORTED_REGION_SLUGS),
    )
    if not targets:
        parser.error("Select at least one region")
    report = {
        "started_at": datetime.now(UTC).isoformat(),
        "versions": {
            name: version(name)
            for name in (
                "scrapling",
                "curl_cffi",
                "patchright",
                "browserforge",
                "apify-fingerprint-datapoints",
            )
        },
        "order": args.order,
        "transports": {},
    }
    transports = (
        ("http", "browser") if args.order == "http-first" else ("browser", "http")
    )
    for transport in transports:
        started = time.monotonic()
        scraper = FlixPatrolScraper(
            transport=transport,
            solve_cf=True,
            solve_headless=not _env_flag("FLIXPATROL_SOLVE_HEADED"),
            proxy=os.getenv("FLIXPATROL_PROXY"),
            solve_deadline_seconds=args.solve_timeout,
            max_requests_per_second=0.5,
        )
        observations = []
        report["transports"][transport] = {"regions": observations}
        try:
            for target, result, duration in iter_region_results(
                scraper, targets, max_workers=1
            ):
                if not isinstance(result, Exception):
                    try:
                        validate_catalog(result.to_dict(output_region=target), target)
                    except ScraperError as exc:
                        result = exc
                row = {
                    "region": target,
                    "duration_seconds": duration,
                    "success": not isinstance(result, Exception),
                }
                if isinstance(result, Exception):
                    error = f"{type(result).__name__}: {result}"
                    row["error"] = (
                        error.replace(scraper.proxy, "[proxy]")
                        if scraper.proxy
                        else error
                    )
                else:
                    row.update(chart_date=result.date, charts=len(result.charts))
                observations.append(row)
                atomic_write_json(args.output, report)
                print(
                    f"{transport} {target}: {'success' if row['success'] else 'failed'} in {duration:.1f}s",
                    flush=True,
                )
        finally:
            scraper.close()
        report["transports"][transport].update(
            metrics=dict(scraper.metrics),
            duration_seconds=round(time.monotonic() - started, 3),
            successful_regions=sum(row["success"] for row in observations),
        )
        atomic_write_json(args.output, report)
    print(
        f"Comparison saved to {args.output}; repeat in reverse order to check order effects"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
