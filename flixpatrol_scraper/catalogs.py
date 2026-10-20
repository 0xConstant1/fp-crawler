from __future__ import annotations

import json
import shutil
import tempfile
import time
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlsplit

from .batch import build_region_url, iter_region_results
from .scraper import NoChartsFoundError, RecoveryOpenError, ScraperError
from .storage import atomic_write_json, atomic_write_text


def chart_date(payload: dict) -> date:
    return datetime.strptime(payload["date"], "%B %d, %Y").date()


def validate_catalog(payload: dict, target: str) -> None:
    try:
        if payload["region"] != target:
            raise ValueError("Region does not match the requested target")
        source = urlsplit(payload["source"])
        expected = urlsplit(build_region_url(target))
        if source.hostname != expected.hostname or source.path.rstrip(
            "/"
        ) != expected.path.rstrip("/"):
            raise ValueError("Source URL does not match the requested target")
        chart_date(payload)
        datetime.fromisoformat(payload["scraped_at_utc"])
        charts = payload["charts"]
        if not isinstance(charts, list) or not charts:
            raise ValueError("Catalog has no charts")
        identifiers = set()
        for chart in charts:
            identifier = chart["catalog_id"]
            if (
                not isinstance(identifier, str)
                or "." not in identifier
                or identifier in identifiers
            ):
                raise ValueError("Invalid or duplicate catalog ID")
            identifiers.add(identifier)
            datetime.strptime(chart["date"], "%B %d, %Y")
            entries = chart["entries"]
            if (
                not isinstance(entries, list)
                or not 1 <= len(entries) <= 10
                or chart["title_count"] != len(entries)
            ):
                raise ValueError("Invalid chart entry count")
            if [entry["rank"] for entry in entries] != list(range(1, len(entries) + 1)):
                raise ValueError("Chart ranks must be consecutive")
            if any(
                not isinstance(entry["title"], str) or not entry["title"].strip()
                for entry in entries
            ):
                raise ValueError("Chart contains an empty title")
    except (KeyError, TypeError, ValueError) as exc:
        raise ScraperError(f"Invalid catalog for {target}: {exc}") from exc


def read_catalog(path: Path, target: str) -> dict | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    validate_catalog(payload, target)
    return payload


def publish_catalogs(
    output_dir: Path, candidates: dict[str, dict], build_index
) -> None:
    """Validate the complete snapshot before changing any published file."""
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="catalog-publish-", dir=output_dir.parent
    ) as temporary:
        staging = Path(temporary) / "catalogs"
        if output_dir.exists():
            shutil.copytree(output_dir, staging)
        else:
            staging.mkdir()
        for target, payload in candidates.items():
            atomic_write_json(staging / f"{target}.json", payload)
        paths = sorted(
            p for p in staging.glob("*.json") if p.name != "availability.json"
        )
        for path in paths:
            read_catalog(path, path.stem)
        index = build_index(paths)
        if not candidates:
            try:
                previous_index = json.loads(
                    (staging / "availability.json").read_text(encoding="utf-8")
                )
            except (OSError, ValueError):
                previous_index = None
            if isinstance(previous_index, dict) and {
                key: value for key, value in previous_index.items() if key != "generated_at"
            } == {key: value for key, value in index.items() if key != "generated_at"}:
                return
        atomic_write_json(staging / "availability.json", index)

        output_dir.mkdir(parents=True, exist_ok=True)
        names = [f"{target}.json" for target in sorted(candidates)] + [
            "availability.json"
        ]
        previous = {
            name: (output_dir / name).read_text(encoding="utf-8")
            if (output_dir / name).exists()
            else None
            for name in names
        }
        try:
            for name in names:
                atomic_write_text(
                    output_dir / name, (staging / name).read_text(encoding="utf-8")
                )
        except BaseException:
            for name, content in previous.items():
                if content is None:
                    (output_dir / name).unlink(missing_ok=True)
                else:
                    atomic_write_text(output_dir / name, content)
            raise


def update_catalogs(
    *,
    scraper,
    targets: list[str],
    output_dir: Path,
    state_dir: Path,
    report_path: Path,
    target_date: date,
    build_index,
    force: bool = False,
    max_workers: int = 4,
) -> dict:
    started = time.monotonic()
    candidates: dict[str, dict] = {}
    regions: dict[str, dict] = {}
    pending = []
    report = {
        "schema_version": 1,
        "target_date": target_date.isoformat(),
        "started_at": datetime.now(UTC).isoformat(),
        "transport": scraper.transport,
        "published": False,
        "regions": regions,
    }

    def save_report() -> None:
        report["counts"] = dict(Counter(item["status"] for item in regions.values()))
        report["pending_regions"] = [
            target
            for target in targets
            if regions.get(target, {}).get("status")
            not in {"fresh", "updated", "resumed"}
        ]
        report["metrics"] = dict(scraper.metrics)
        report["duration_seconds"] = round(time.monotonic() - started, 3)
        atomic_write_json(report_path, report)

    def usable_date(payload: dict) -> date:
        observed = chart_date(payload)
        if observed > datetime.now(UTC).date():
            raise ScraperError("Source chart date is in the future")
        return observed

    for target in targets:
        previous = read_catalog(output_dir / f"{target}.json", target)
        previous_date = usable_date(previous) if previous else None
        regions[target] = {
            "status": "pending",
            "chart_date": previous_date.isoformat() if previous_date else None,
            "retained_previous": previous is not None,
        }
        if not force and previous_date is not None and previous_date >= target_date:
            regions[target]["status"] = "fresh"
            continue
        checkpoint = state_dir / "candidates" / f"{target}.json"
        try:
            cached = read_catalog(checkpoint, target)
            if (
                not force
                and cached
                and usable_date(cached) >= target_date
                and (previous_date is None or chart_date(cached) >= previous_date)
            ):
                candidates[target] = cached
                regions[target].update(
                    status="resumed", chart_date=chart_date(cached).isoformat()
                )
                continue
        except (OSError, ValueError, ScraperError):
            pass
        pending.append(target)
    save_report()
    print(
        f"Fetching {len(pending)} pending regions; {len(targets) - len(pending)} already available",
        flush=True,
    )

    for target, result, duration in iter_region_results(
        scraper, pending, max_workers=max_workers
    ):
        item = regions[target]
        item["duration_seconds"] = duration
        if isinstance(result, Exception):
            item["status"] = (
                "empty"
                if isinstance(result, NoChartsFoundError)
                else "deferred"
                if isinstance(result, RecoveryOpenError)
                else "failed"
            )
            error = f"{type(result).__name__}: {result}"
            item["error"] = (
                error.replace(scraper.proxy, "[proxy]") if scraper.proxy else error
            )
        else:
            try:
                payload = result.to_dict(output_region=target)
                validate_catalog(payload, target)
                observed = usable_date(payload)
                previous_date = (
                    date.fromisoformat(item["chart_date"])
                    if item["chart_date"]
                    else None
                )
                if previous_date and observed < previous_date:
                    raise ScraperError(
                        "Source returned an older chart than the published catalog"
                    )
                atomic_write_json(state_dir / "candidates" / f"{target}.json", payload)
                candidates[target] = payload
                item.update(
                    status="updated" if observed >= target_date else "stale",
                    chart_date=observed.isoformat(),
                )
            except (OSError, ValueError, ScraperError) as exc:
                item.update(status="failed", error=str(exc))
        print(f"{target}: {item['status']} ({duration:.1f}s)", flush=True)
        save_report()

    publish_catalogs(output_dir, candidates, build_index)
    for target in candidates:
        regions[target]["retained_previous"] = False
    report["published"] = True
    report["published_regions"] = sorted(candidates)
    report["finished_at"] = datetime.now(UTC).isoformat()
    save_report()
    return report
