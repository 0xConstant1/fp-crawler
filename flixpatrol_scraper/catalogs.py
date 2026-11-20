from __future__ import annotations

import json
import shutil
import tempfile
import time
from collections import Counter
from collections.abc import Iterable, Set
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlsplit

from .batch import build_region_url, iter_region_results
from .scraper import (
    TOP10_LIMIT,
    ChartIdentity,
    NoChartsFoundError,
    RecoveryOpenError,
    ScraperError,
    chart_identity,
    parse_chart_date,
    validate_chart_ranks,
    validate_tmdb_ids,
)
from .storage import atomic_write_bytes, atomic_write_json, atomic_write_text, catalog_publication_lock

CHECKPOINT_VERSION = 3


def chart_date(payload: dict) -> date:
    return parse_chart_date(payload["date"])


def _usable_date(payload: dict) -> date:
    observed = chart_date(payload)
    if observed > datetime.now(UTC).date():
        raise ScraperError("Source chart date is in the future")
    return observed


def _validate_chart(
    chart: dict,
    observed_date: date,
    expected_count: int,
    *,
    source_title_repeats: list[list[int]] | None = None,
    check_tmdb_uniqueness: bool = True,
) -> None:
    identifier = chart["catalog_id"]
    if chart_date(chart) != observed_date:
        raise ValueError(f"Chart {identifier!r} does not match the catalog date")
    entries = chart["entries"]
    if not isinstance(entries, list) or chart["title_count"] != len(entries):
        raise ValueError(f"Chart {identifier!r} has an invalid entry count")
    validate_chart_ranks(
        [entry["rank"] for entry in entries], identifier, count=expected_count
    )
    if any(
        not isinstance(entry["title"], str) or not entry["title"].strip()
        for entry in entries
    ):
        raise ValueError(f"Chart {identifier!r} contains an empty title")
    repeats: dict[int, int] = {}
    if source_title_repeats is not None:
        if not isinstance(source_title_repeats, list):
            raise ValueError(f"Chart {identifier!r} has invalid repeated source ranks")
        for rank, first in source_title_repeats:
            if (
                type(rank) is not int
                or type(first) is not int
                or not 1 <= first < rank <= expected_count
                or rank in repeats
                or entries[rank - 1]["tmdb"] != entries[first - 1]["tmdb"]
            ):
                raise ValueError(f"Chart {identifier!r} has invalid repeated source ranks")
            repeats[rank] = first
        if any(first in repeats for first in repeats.values()):
            raise ValueError(f"Chart {identifier!r} has invalid repeated source ranks")
    matches = []
    for entry in entries:
        match = entry["tmdb"]
        if match is None:
            continue
        media_type, tmdb_id = match["media_type"], match["id"]
        if media_type not in {"movie", "tv"} or type(tmdb_id) is not int or tmdb_id < 1:
            raise ValueError(f"Chart {identifier!r} contains an invalid TMDB identity")
        rank = entry["rank"]
        matches.append((media_type, tmdb_id, entry["title"], repeats.get(rank, rank)))
    if check_tmdb_uniqueness:
        validate_tmdb_ids(matches, identifier)


def validate_catalog(
    payload: dict,
    target: str,
    *,
    check_chart_integrity: bool = True,
    source_rank_counts: dict[str, int] | None = None,
    source_title_repeats: dict[str, list[list[int]]] | None = None,
) -> None:
    try:
        if source_title_repeats is not None and not isinstance(source_title_repeats, dict):
            raise ValueError("Invalid repeated source ranks")
        if payload["region"] != target:
            raise ValueError("Region does not match the requested target")
        source = urlsplit(payload["source"])
        expected = urlsplit(build_region_url(target))
        if source.hostname != expected.hostname or source.path.rstrip(
            "/"
        ) != expected.path.rstrip("/"):
            raise ValueError("Source URL does not match the requested target")
        observed_date = chart_date(payload)
        datetime.fromisoformat(payload["scraped_at_utc"])
        charts = payload["charts"]
        if not isinstance(charts, list) or not charts:
            raise ValueError("Catalog has no charts")
        identifiers = set()
        identities: set[ChartIdentity] = set()
        for chart in charts:
            identifier = chart["catalog_id"]
            if (
                not isinstance(identifier, str)
                or "." not in identifier
                or identifier in identifiers
            ):
                raise ValueError("Invalid or duplicate catalog ID")
            identifiers.add(identifier)
            variant = chart.get("variant")
            if variant is not None and (
                not isinstance(variant, dict)
                or any(
                    not isinstance(variant.get(key), str) or not variant[key].strip()
                    for key in ("id", "label")
                )
            ):
                raise ValueError(f"Chart {identifier!r} has invalid variant metadata")
            identity = chart_identity(chart)
            if not check_chart_integrity:
                continue
            if identity in identities:
                raise ValueError(f"Duplicate chart heading for {identifier!r}")
            identities.add(identity)
            expected_count = (
                TOP10_LIMIT if source_rank_counts is None else source_rank_counts[identifier]
            )
            _validate_chart(
                chart,
                observed_date,
                expected_count,
                source_title_repeats=(source_title_repeats or {}).get(identifier),
            )
        if source_rank_counts is not None and identifiers != source_rank_counts.keys():
            raise ValueError("Catalog charts do not match the source charts")
        if source_title_repeats and source_title_repeats.keys() - identifiers:
            raise ValueError("Repeated source ranks refer to an unknown chart")
    except (KeyError, TypeError, ValueError, ScraperError) as exc:
        raise ScraperError(f"Invalid catalog for {target}: {exc}") from exc


def read_catalog(
    path: Path,
    target: str,
    *,
    check_chart_integrity: bool = True,
    source_rank_counts: dict[str, int] | None = None,
    source_title_repeats: dict[str, list[list[int]]] | None = None,
) -> dict | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    validate_catalog(
        payload,
        target,
        check_chart_integrity=check_chart_integrity,
        source_rank_counts=source_rank_counts,
        source_title_repeats=source_title_repeats,
    )
    return payload


def catalog_paths_for_index(
    paths: Iterable[Path], *, validated_regions: Set[str] = frozenset()
) -> list[Path]:
    indexable = []
    for path in paths:
        if path.name == "availability.json":
            continue
        if path.stem not in validated_regions:
            try:
                if read_catalog(path, path.stem, check_chart_integrity=False) is None:
                    continue
            except (OSError, ValueError, ScraperError):
                # Keep the existing file for repair and omit it from the new index.
                continue
        indexable.append(path)
    return indexable


def _read_checkpoint(path: Path, target: str) -> dict | None:
    try:
        saved = json.loads(path.read_text(encoding="utf-8"))
        counts = saved["source_rank_counts"]
        limit = saved.get("max_titles_per_chart", TOP10_LIMIT)
        if (
            saved["version"] != CHECKPOINT_VERSION
            or not isinstance(counts, dict)
            or type(limit) is not int
            or not 1 <= limit <= TOP10_LIMIT
            or any(count > limit for count in counts.values())
        ):
            return None
        catalog = saved["catalog"]
        validate_catalog(
            catalog,
            target,
            source_rank_counts=counts,
            source_title_repeats=saved.get("source_title_repeats", {}),
        )
        _usable_date(catalog)
        return saved
    except (OSError, KeyError, TypeError, ValueError, ScraperError):
        return None


def _validate_replacement(
    previous: dict,
    replacement: dict,
    *,
    source_complete: bool = False,
    max_titles_per_chart: int = TOP10_LIMIT,
    previous_title_limit: int = TOP10_LIMIT,
    title_details_deferred: bool = False,
) -> None:
    published_date = chart_date(previous)
    observed = chart_date(replacement)
    if observed < published_date:
        raise ScraperError("Source returned an older chart than the published catalog")
    if observed > published_date and source_complete:
        return

    replacement_ids = {chart["catalog_id"] for chart in replacement["charts"]}
    replacement_charts = {chart_identity(chart): chart for chart in replacement["charts"]}
    previous_ids = {
        chart["catalog_id"]: chart_identity(chart) for chart in previous["charts"]
    }
    missing = [
        f"{identifier} ({identity[2]})" if identity[2] else identifier
        for identifier, identity in previous_ids.items()
        if identifier not in replacement_ids or identity not in replacement_charts
    ]
    if missing:
        raise ScraperError(
            f"Source is missing previously published charts {', '.join(sorted(missing))}"
        )

    if observed == published_date:
        for chart in previous["charts"]:
            identifier = chart["catalog_id"]
            updated = replacement_charts[previous_ids[identifier]]
            fewer_ranks = (
                chart.get("title_count") == previous_title_limit
                and updated["title_count"] < min(previous_title_limit, max_titles_per_chart)
            )
            if not fewer_ranks and not title_details_deferred:
                continue
            try:
                _validate_chart(
                    chart, published_date, chart["title_count"], check_tmdb_uniqueness=False
                )
            except (KeyError, TypeError, ValueError, ScraperError):
                continue
            if fewer_ranks:
                raise ScraperError(
                    f"Source returned fewer ranks than the published chart {identifier!r} "
                    "for the same date"
                )
            previous_matches = Counter(
                entry["title"] for entry in chart["entries"] if entry["tmdb"] is not None
            )
            updated_titles = Counter(entry["title"] for entry in updated["entries"])
            updated_matches = Counter(
                entry["title"] for entry in updated["entries"] if entry["tmdb"] is not None
            )
            if any(
                updated_matches[title] < min(count, updated_titles[title])
                for title, count in previous_matches.items()
            ):
                raise ScraperError(f"Deferred title details would remove TMDB matches from {identifier!r}")


def publish_catalogs(
    output_dir: Path,
    candidates: dict[str, dict],
    build_index,
    *,
    source_rank_counts: dict[str, dict[str, int]] | None = None,
    source_title_repeats: dict[str, dict[str, list[list[int]]]] | None = None,
    checkpoints: dict[str, tuple[Path, dict]] | None = None,
) -> dict[str, tuple[date, str]]:
    """Validate and publish a snapshot, returning regions rejected against it."""
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    rejected: dict[str, tuple[date, str]] = {}
    with (
        catalog_publication_lock(output_dir),
        tempfile.TemporaryDirectory(
            prefix="catalog-publish-", dir=output_dir.parent
        ) as temporary,
    ):
        staging = Path(temporary) / "catalogs"
        if output_dir.exists():
            shutil.copytree(output_dir, staging)
        else:
            staging.mkdir()
        staged_checkpoints: dict[Path, Path] = {}
        for target, payload in candidates.items():
            counts = (source_rank_counts or {}).get(target)
            repeats = (source_title_repeats or {}).get(target)
            validate_catalog(
                payload, target, source_rank_counts=counts, source_title_repeats=repeats
            )
            path = staging / f"{target}.json"
            try:
                previous = read_catalog(path, target, check_chart_integrity=False)
                published_date = _usable_date(previous) if previous is not None else None
            except (OSError, ValueError, ScraperError):
                previous = None
                published_date = None
            checkpoint = (checkpoints or {}).get(target)
            saved = checkpoint[1] if checkpoint is not None else {}
            if checkpoint is not None and saved.get("catalog") != payload:
                raise ScraperError(f"Checkpoint does not match the catalog for {target!r}")
            published = _read_checkpoint(checkpoint[0], target) if checkpoint is not None else None
            previous_limit = TOP10_LIMIT
            if published is not None:
                accepted_catalog = published["catalog"]
                checkpoint_date = chart_date(accepted_catalog)
                checkpoint_limit = published.get("max_titles_per_chart", TOP10_LIMIT)
                if checkpoint_date == published_date and previous != accepted_catalog:
                    for previous_counts in (None, published["source_rank_counts"]):
                        try:
                            validate_catalog(previous, target, source_rank_counts=previous_counts)
                        except ScraperError:
                            continue
                        if previous_counts is not None:
                            previous_limit = checkpoint_limit
                        break
                    else:
                        previous = None
                if previous is None or checkpoint_date > published_date:
                    previous, published_date = accepted_catalog, checkpoint_date
                if previous == accepted_catalog:
                    previous_limit = checkpoint_limit
            if previous is not None and published_date is not None:
                try:
                    _validate_replacement(
                        previous,
                        payload,
                        source_complete=saved.get("source_complete") is True,
                        max_titles_per_chart=saved.get("max_titles_per_chart", TOP10_LIMIT),
                        previous_title_limit=previous_limit,
                        title_details_deferred=saved.get("title_details_deferred") is True,
                    )
                except ScraperError as exc:
                    rejected[target] = published_date, str(exc)
                    continue
            atomic_write_json(path, payload)
            if checkpoint is not None:
                checkpoint_path, saved = checkpoint
                staged_checkpoint = Path(temporary) / "published" / path.name
                atomic_write_json(staged_checkpoint, saved)
                staged_checkpoints[checkpoint_path] = staged_checkpoint
        accepted = candidates.keys() - rejected.keys()
        paths = catalog_paths_for_index(sorted(staging.glob("*.json")), validated_regions=accepted)
        index = build_index(paths)
        if not accepted:
            try:
                previous_index = json.loads(
                    (staging / "availability.json").read_text(encoding="utf-8")
                )
            except (OSError, ValueError):
                previous_index = None
            if isinstance(previous_index, dict) and {
                key: value
                for key, value in previous_index.items()
                if key != "generated_at"
            } == {key: value for key, value in index.items() if key != "generated_at"}:
                return rejected
        atomic_write_json(staging / "availability.json", index)

        output_dir.mkdir(parents=True, exist_ok=True)
        names = [f"{target}.json" for target in sorted(accepted)] + ["availability.json"]
        writes = {output_dir / name: staging / name for name in names}
        writes.update(staged_checkpoints)
        previous_files = {
            path: path.read_bytes() if path.exists() else None for path in writes
        }
        try:
            for path, staged in writes.items():
                atomic_write_text(path, staged.read_text(encoding="utf-8"))
        except BaseException:
            for path, content in previous_files.items():
                if content is None:
                    path.unlink(missing_ok=True)
                else:
                    atomic_write_bytes(path, content)
            raise
    return rejected


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
    candidate_rank_counts: dict[str, dict[str, int]] = {}
    candidate_title_repeats: dict[str, dict[str, list[list[int]]]] = {}
    accepted_checkpoints: dict[str, tuple[Path, dict]] = {}
    previous_catalogs: dict[str, tuple[dict, int]] = {}
    fetch_title_details = bool(getattr(scraper, "fetch_title_details", False))
    max_titles_per_chart = getattr(scraper, "max_titles_per_chart", TOP10_LIMIT)
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
        report["active_transport"] = getattr(
            scraper, "active_transport", scraper.transport
        )
        report["stop_reason"] = getattr(scraper, "stop_reason", None)
        if report["stop_reason"] and scraper.proxy:
            report["stop_reason"] = report["stop_reason"].replace(
                scraper.proxy, "[proxy]"
            )
        report["duration_seconds"] = round(time.monotonic() - started, 3)
        atomic_write_json(report_path, report)

    def matches_configuration(saved: dict) -> bool:
        return (
            saved.get("max_titles_per_chart", TOP10_LIMIT) == max_titles_per_chart
            and (not fetch_title_details or saved.get("fetch_title_details") is True)
        )

    for target in targets:
        previous_path = output_dir / f"{target}.json"
        published_checkpoint = state_dir / "published" / f"{target}.json"
        candidate_checkpoint = state_dir / "candidates" / f"{target}.json"
        checkpoints = []

        previous_date = None
        regions[target] = {
            "status": "pending",
            "chart_date": None,
            "retained_previous": previous_path.exists(),
        }
        previous_matches_configuration = False
        try:
            with catalog_publication_lock(output_dir):
                published = _read_checkpoint(published_checkpoint, target)
                if published is not None:
                    previous_catalogs[target] = (
                        published["catalog"], published.get("max_titles_per_chart", TOP10_LIMIT)
                    )
                checkpoints = [
                    saved for saved in (published, _read_checkpoint(candidate_checkpoint, target))
                    if saved is not None
                ]
                previous = read_catalog(previous_path, target, check_chart_integrity=False)
            if previous is not None:
                previous_date = _usable_date(previous)
                regions[target].update(chart_date=previous_date.isoformat(), retained_previous=True)
                matching = [saved for saved in checkpoints if previous == saved["catalog"]]
                proof = matching[0] if matching else {}
                previous_catalogs[target] = previous, proof.get("max_titles_per_chart", TOP10_LIMIT)
                previous_matches_configuration = any(
                    matches_configuration(saved) for saved in matching or [proof]
                )
                validate_catalog(
                    previous,
                    target,
                    source_rank_counts=proof.get("source_rank_counts"),
                    source_title_repeats=proof.get("source_title_repeats"),
                )
        except (OSError, ValueError, ScraperError) as exc:
            regions[target]["previous_error"] = str(exc)
        else:
            if (
                not force
                and previous_date is not None
                and previous_date >= target_date
                and previous_matches_configuration
            ):
                regions[target]["status"] = "fresh"
                continue
        if not force:
            for saved in sorted(
                checkpoints, key=lambda item: chart_date(item["catalog"]), reverse=True
            ):
                cached_catalog = saved["catalog"]
                cached_date = chart_date(cached_catalog)
                if cached_date < target_date or not matches_configuration(saved):
                    continue
                try:
                    if target in previous_catalogs:
                        previous, previous_limit = previous_catalogs[target]
                        _validate_replacement(
                            previous,
                            cached_catalog,
                            source_complete=saved.get("source_complete") is True,
                            max_titles_per_chart=max_titles_per_chart,
                            previous_title_limit=previous_limit,
                            title_details_deferred=saved.get("title_details_deferred") is True,
                        )
                except ScraperError:
                    continue
                candidates[target] = cached_catalog
                candidate_rank_counts[target] = saved["source_rank_counts"]
                candidate_title_repeats[target] = saved.get("source_title_repeats", {})
                accepted_checkpoints[target] = published_checkpoint, saved
                regions[target].update(status="resumed", chart_date=cached_date.isoformat())
                break
            if target in candidates:
                continue
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
                counts = result.expected_rank_counts
                repeats = result.source_title_repeats
                validate_catalog(
                    payload, target, source_rank_counts=counts, source_title_repeats=repeats
                )
                observed = _usable_date(payload)
                if target in previous_catalogs:
                    previous, previous_limit = previous_catalogs[target]
                    _validate_replacement(
                        previous,
                        payload,
                        source_complete=result.source_complete,
                        max_titles_per_chart=result.max_titles_per_chart,
                        previous_title_limit=previous_limit,
                        title_details_deferred=result.title_details_deferred,
                    )
                saved = {
                    "version": CHECKPOINT_VERSION,
                    "catalog": payload,
                    "source_rank_counts": counts,
                    "source_title_repeats": repeats,
                    "source_complete": result.source_complete,
                    "max_titles_per_chart": result.max_titles_per_chart,
                    "title_details_deferred": result.title_details_deferred,
                    "fetch_title_details": fetch_title_details and not result.title_details_deferred,
                }
                atomic_write_json(state_dir / "candidates" / f"{target}.json", saved)
                candidates[target] = payload
                candidate_rank_counts[target] = counts
                candidate_title_repeats[target] = repeats
                accepted_checkpoints[target] = (state_dir / "published" / f"{target}.json", saved)
                item.update(
                    status="updated" if observed >= target_date else "stale",
                    chart_date=observed.isoformat(),
                )
            except (OSError, ValueError, ScraperError) as exc:
                item.update(status="failed", error=str(exc))
        print(f"{target}: {item['status']} ({duration:.1f}s)", flush=True)
        if item.get("error") and (item["status"] != "deferred" or duration > 0):
            print(f"  {item['error']}", flush=True)
        save_report()

    rejected = publish_catalogs(
        output_dir,
        candidates,
        build_index,
        source_rank_counts=candidate_rank_counts,
        source_title_repeats=candidate_title_repeats,
        checkpoints=accepted_checkpoints,
    )
    for target, (published_date, error) in rejected.items():
        regions[target].update(
            status="failed",
            chart_date=published_date.isoformat(),
            error=error,
            retained_previous=True,
        )
    published_targets = sorted(candidates.keys() - rejected.keys())
    for target in published_targets:
        regions[target]["retained_previous"] = False
    report["published"] = True
    report["published_regions"] = published_targets
    report["finished_at"] = datetime.now(UTC).isoformat()
    save_report()
    return report
