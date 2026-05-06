from __future__ import annotations

import os
from pathlib import Path
import shutil
import sys

from flixpatrol_scraper.__main__ import main as cli_main

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "catalogs"


def _ensure_tmdb_credentials() -> None:
    if os.getenv("TMDB_API_KEY") or os.getenv("TMDB_ACCESS_TOKEN"):
        return
    raise SystemExit(
        "TMDB_API_KEY or TMDB_ACCESS_TOKEN must be set before running the daily update."
    )


def _reset_output_dir(output_dir: Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def main() -> int:
    _ensure_tmdb_credentials()
    _reset_output_dir(OUTPUT_DIR)
    return cli_main(
        [
            "--all-regions-slim",
            "--output",
            str(OUTPUT_DIR),
            "--resolve-tmdb",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
