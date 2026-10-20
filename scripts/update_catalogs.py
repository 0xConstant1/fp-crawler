from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path

from build_availability import build_index

from flixpatrol_scraper.__main__ import _env_flag, _solve_deadline, parse_region_targets
from flixpatrol_scraper.catalogs import update_catalogs
from flixpatrol_scraper.regions import SLIM_REGION_SLUGS, SUPPORTED_REGION_SLUGS
from flixpatrol_scraper.scraper import FlixPatrolScraper
from flixpatrol_scraper.tmdb import TMDBResolver

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "catalogs"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Update pending catalogs while retaining previous valid regions.",
        epilog="Exit zero confirms safe publication; the report lists regions still pending.",
    )
    parser.add_argument(
        "--region", help="Only consider these comma-separated region slugs."
    )
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--state-dir", type=Path, default=REPO_ROOT / ".catalog-state")
    parser.add_argument(
        "--report", type=Path, default=REPO_ROOT / "exports/update/report.json"
    )
    parser.add_argument(
        "--target-date", type=date.fromisoformat, default=datetime.now(UTC).date()
    )
    parser.add_argument(
        "--force", action="store_true", help="Refresh even regions already current."
    )
    parser.add_argument(
        "--transport",
        choices=("http", "browser"),
        default=os.getenv("FLIXPATROL_TRANSPORT", "http"),
    )
    parser.add_argument("--solve-timeout", type=float)
    parser.add_argument("--max-recoveries", type=int, default=2)
    parser.add_argument("--requests-per-second", type=float, default=0.5)
    args = parser.parse_args(argv)
    resolver = TMDBResolver.from_env()
    if resolver is None:
        parser.error("TMDB_API_KEY or TMDB_ACCESS_TOKEN must be set")
    try:
        targets = parse_region_targets(
            region_arg=args.region,
            all_regions=False,
            all_regions_slim=not bool(args.region),
            supported_regions=list(SUPPORTED_REGION_SLUGS),
            slim_regions=list(SLIM_REGION_SLUGS),
        )
        if not targets or args.target_date > datetime.now(UTC).date():
            raise ValueError(
                "Choose at least one region and a target date no later than today (UTC)"
            )
        scraper = FlixPatrolScraper(
            tmdb_resolver=resolver,
            transport=args.transport,
            solve_cf=_env_flag("FLIXPATROL_SOLVE_CF"),
            solve_headless=not _env_flag("FLIXPATROL_SOLVE_HEADED"),
            proxy=os.getenv("FLIXPATROL_PROXY"),
            cf_clearance=os.getenv("FLIXPATROL_CF_CLEARANCE"),
            user_agent=os.getenv("FLIXPATROL_USER_AGENT"),
            solve_deadline_seconds=_solve_deadline(args.solve_timeout),
            max_recoveries=args.max_recoveries,
            max_requests_per_second=args.requests_per_second,
        )
        try:
            report = update_catalogs(
                scraper=scraper,
                targets=targets,
                output_dir=args.output,
                state_dir=args.state_dir,
                report_path=args.report,
                target_date=args.target_date,
                build_index=build_index,
                force=args.force,
            )
        finally:
            scraper.close()
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Update could not publish: {exc}", file=sys.stderr)
        return 1
    print(
        f"Published snapshot: {report['counts']}; {len(report['pending_regions'])} regions pending"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
