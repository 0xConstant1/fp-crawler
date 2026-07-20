"""Derive ``catalogs/availability.json`` from the emitted region files.

The availability index is a compact map the AIOMetadata config UI reads at
startup instead of probing every service+country combination. It is built purely
from the region files, so it cannot disagree with them: every entry here is
guaranteed to have a matching chart in ``{region}.json``.

Shape::

    {
      "schema_version": 1,
      "generated_at": "2026-07-20T16:00:00Z",
      "variantLabels": { "hi": "Hindi", ... },
      "regions": {
        "india": { "hotstar": { "overall": ["hi", "mr"] }, ... },
        ...
      }
    }

Per ``(region -> service -> category)`` the value is the sorted list of suffixed
variant ids. The promoted/unqualified default holds the bare id and is never
listed. Only ``movies`` / ``series`` / ``overall`` are included; a service with
none of the three is omitted entirely.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

from flixpatrol_scraper.scraper import CANONICAL_CATEGORIES, split_catalog_id

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG_DIR = REPO_ROOT / "catalogs"
AVAILABILITY_FILENAME = "availability.json"
SCHEMA_VERSION = 1


def region_files(catalog_dir: Path) -> list[Path]:
    """Every region JSON in the directory, excluding the index itself."""
    return sorted(
        path
        for path in catalog_dir.glob("*.json")
        if path.name != AVAILABILITY_FILENAME
    )


def build_index(paths: list[Path]) -> dict:
    regions: dict[str, dict[str, dict[str, list[str]]]] = {}
    variant_labels: dict[str, str] = {}

    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        services: dict[str, dict[str, list[str]]] = {}

        for chart in payload.get("charts", []):
            service, category, variant_id = split_catalog_id(chart)
            if category not in CANONICAL_CATEGORIES:
                continue
            variants = services.setdefault(service, {}).setdefault(category, [])
            if variant_id is not None:
                variants.append(variant_id)
                variant_labels[variant_id] = chart["variant"]["label"]

        regions[path.stem] = {
            service: {
                category: sorted(variant_ids)
                for category, variant_ids in sorted(categories.items())
            }
            for service, categories in sorted(services.items())
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "variantLabels": dict(sorted(variant_labels.items())),
        "regions": dict(sorted(regions.items())),
    }


def write_index(catalog_dir: Path) -> Path:
    index = build_index(region_files(catalog_dir))
    output = catalog_dir / AVAILABILITY_FILENAME
    output.write_text(
        json.dumps(index, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "catalog_dir",
        nargs="?",
        type=Path,
        default=DEFAULT_CATALOG_DIR,
        help="Directory of region files to index. Defaults to catalogs/.",
    )
    args = parser.parse_args(argv)

    paths = region_files(args.catalog_dir)
    if not paths:
        print(f"No region files found in {args.catalog_dir}", file=sys.stderr)
        return 1

    output = write_index(args.catalog_dir)
    index = json.loads(output.read_text(encoding="utf-8"))
    print(
        f"Wrote {output} - {len(index['regions'])} regions, "
        f"{len(index['variantLabels'])} variant labels."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
