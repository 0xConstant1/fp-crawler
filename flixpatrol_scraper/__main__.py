from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import sys

from .scraper import (
    DEFAULT_OUTPUT_PATH,
    DEFAULT_TOP10_URL,
    FlixPatrolScraper,
    NoChartsFoundError,
    ScraperError,
    ScrapeResult,
    write_result,
)
from .tmdb import TMDB_DEFAULT_LANGUAGE, TMDBResolver, TMDBResolverError

SUPPORTED_REGIONS_PATH = Path(__file__).resolve().parents[1] / "supported_regions.txt"
FLIXPATROL_MAX_WORKERS = 4


def load_supported_regions(path: Path = SUPPORTED_REGIONS_PATH) -> list[str]:
    regions: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value:
            continue
        regions.append(value.strip("/").split("/")[-1].lower())
    return regions


def normalize_region_token(value: str) -> str:
    token = value.strip().lower()
    if not token:
        return ""
    if token == "global":
        return token
    if "/top10/streaming/" in token:
        return token.strip("/").split("/")[-1]
    return token.strip("/")


def parse_region_targets(
    *,
    region_arg: str | None,
    all_regions: bool,
    supported_regions: list[str],
) -> list[str]:
    if all_regions:
        return ["global", *supported_regions]

    if not region_arg:
        return []

    targets: list[str] = []
    seen: set[str] = set()
    for raw_value in region_arg.split(","):
        token = normalize_region_token(raw_value)
        if not token:
            continue
        if token != "global" and token not in supported_regions:
            raise ValueError(
                f"Unsupported region {raw_value!r}. See supported_regions.txt for valid values."
            )
        if token not in seen:
            seen.add(token)
            targets.append(token)
    return targets


def build_region_url(region: str) -> str:
    if region == "global":
        return DEFAULT_TOP10_URL
    return f"https://flixpatrol.com/top10/streaming/{region}/"


def resolve_multi_output_directory(output_path: Path) -> Path:
    if output_path.exists() and output_path.is_dir():
        return output_path
    if output_path.suffix:
        return output_path.parent
    return output_path


def build_output_path_for_target(output_dir: Path, target: str) -> Path:
    return output_dir / f"{target}.json"


def resolve_single_output_path(output_path: Path, target: str) -> Path:
    if output_path.exists() and output_path.is_dir():
        return build_output_path_for_target(output_path, target)
    if not output_path.suffix:
        return build_output_path_for_target(output_path, target)
    return output_path


def _raise_region_failure(target: str, exc: Exception) -> None:
    raise exc.__class__(f"Failed while scraping region {target!r}: {exc}") from exc


def scrape_region_targets(
    *,
    scraper: FlixPatrolScraper,
    region_targets: list[str],
    output_dir: Path,
) -> tuple[int, list[str]]:
    skipped_targets: list[str] = []
    written_count = 0
    total_targets = len(region_targets)
    completed_count = 0

    output_dir.mkdir(parents=True, exist_ok=True)
    print(
        f"Processing {total_targets} regions with up to "
        f"{min(FLIXPATROL_MAX_WORKERS, total_targets)} workers"
    )
    with ThreadPoolExecutor(
        max_workers=min(FLIXPATROL_MAX_WORKERS, total_targets)
    ) as executor:
        future_to_target = {
            executor.submit(scraper.scrape_url, build_region_url(target)): target
            for target in region_targets
        }
        for future in as_completed(future_to_target):
            target = future_to_target[future]
            completed_count += 1
            try:
                result: ScrapeResult = future.result()
            except NoChartsFoundError:
                skipped_targets.append(target)
                print(
                    f"[{completed_count}/{total_targets}] "
                    f"Skipped {target}: no TOP 10 charts available"
                )
                continue
            except (ScraperError, TMDBResolverError) as exc:
                _raise_region_failure(target, exc)

            output_path = build_output_path_for_target(output_dir, target)
            write_result(result, output_path, output_region=target)
            print(
                f"[{completed_count}/{total_targets}] "
                f"Wrote {len(result.charts)} charts to {output_path}"
            )
            written_count += 1

    return written_count, skipped_targets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scrape FlixPatrol TOP 10 charts into a JSON file."
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_TOP10_URL,
        help="FlixPatrol page to scrape when --region or --all-regions is not provided.",
    )
    parser.add_argument(
        "--region",
        help=(
            "Region slug or comma-separated region slugs from supported_regions.txt. "
            "Use 'global' for the world page."
        ),
    )
    parser.add_argument(
        "--all-regions",
        action="store_true",
        help="Scrape global plus every region listed in supported_regions.txt.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Path to the JSON output file.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds for live requests.",
    )
    parser.add_argument(
        "--resolve-tmdb",
        action="store_true",
        help="Resolve scraped titles to TMDB IDs. Requires TMDB credentials.",
    )
    parser.add_argument(
        "--tmdb-access-token",
        help="TMDB v4 bearer token. Falls back to TMDB_ACCESS_TOKEN when omitted.",
    )
    parser.add_argument(
        "--tmdb-api-key",
        help="TMDB v3 API key. Falls back to TMDB_API_KEY when omitted.",
    )
    parser.add_argument(
        "--tmdb-language",
        default=TMDB_DEFAULT_LANGUAGE,
        help="TMDB search language to use during title matching.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    supported_regions = load_supported_regions()

    try:
        region_targets = parse_region_targets(
            region_arg=args.region,
            all_regions=args.all_regions,
            supported_regions=supported_regions,
        )
    except ValueError as exc:
        parser.exit(status=1, message=f"Error: {exc}\n")

    if args.all_regions and args.region:
        parser.exit(
            status=1,
            message="Error: --all-regions cannot be combined with --region.\n",
        )

    if region_targets and args.url != DEFAULT_TOP10_URL:
        parser.exit(
            status=1,
            message="Error: --url cannot be combined with --region or --all-regions.\n",
        )

    tmdb_resolver = None
    if args.tmdb_access_token or args.tmdb_api_key:
        try:
            tmdb_resolver = TMDBResolver(
                access_token=args.tmdb_access_token,
                api_key=args.tmdb_api_key,
                timeout_seconds=args.timeout,
                language=args.tmdb_language,
            )
        except TMDBResolverError as exc:
            parser.exit(status=1, message=f"Error: {exc}\n")
    else:
        tmdb_resolver = TMDBResolver.from_env(
            timeout_seconds=args.timeout,
            language=args.tmdb_language,
        )

    if args.resolve_tmdb and tmdb_resolver is None:
        parser.exit(
            status=1,
            message=(
                "Error: TMDB resolution was requested but no TMDB credentials were found. "
                "Provide --tmdb-access-token, --tmdb-api-key, TMDB_ACCESS_TOKEN, "
                "or TMDB_API_KEY.\n"
            ),
        )

    scraper = FlixPatrolScraper(
        timeout_seconds=args.timeout,
        tmdb_resolver=tmdb_resolver,
    )

    try:
        if region_targets:
            if len(region_targets) == 1:
                target = region_targets[0]
                result = scraper.scrape_url(build_region_url(target))
                output_path = resolve_single_output_path(args.output, target)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path = write_result(result, output_path, output_region=target)
                print(f"Wrote {len(result.charts)} charts to {output_path}")
                return 0

            output_dir = resolve_multi_output_directory(args.output)
            written_count, skipped_targets = scrape_region_targets(
                scraper=scraper,
                region_targets=region_targets,
                output_dir=output_dir,
            )
            if written_count == 0:
                raise NoChartsFoundError(
                    "None of the requested regions exposed TOP 10 charts."
                )
            if skipped_targets:
                print(f"Skipped {len(skipped_targets)} regions with no charts")
            return 0

        result = scraper.scrape_url(args.url)
        output_path = write_result(result, args.output)
    except (OSError, ScraperError, TMDBResolverError) as exc:
        parser.exit(status=1, message=f"Error: {exc}\n")

    print(f"Wrote {len(result.charts)} charts to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
