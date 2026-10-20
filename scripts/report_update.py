from __future__ import annotations

import json
import os
from pathlib import Path


def main() -> int:
    path = Path("exports/update/report.json")
    if not path.exists():
        print("No update report was produced")
        return 1
    report = json.loads(path.read_text(encoding="utf-8"))
    pending = report["pending_regions"]
    published = report["published"]
    summary = [
        f"Catalog freshness for {report['target_date']} ({report['transport']})",
        "",
        (
            f"Snapshot validated: {published}. Regions updated: {len(report.get('published_regions', []))}. "
            f"Regions still pending: {len(pending)}."
        ),
        "",
        f"Active transport: {report.get('active_transport', report['transport'])}.",
        "",
    ]
    if report.get("stop_reason"):
        summary.extend([f"Crawler stopped: {report['stop_reason']}", ""])
    errors = [
        (region, item["error"])
        for region, item in report["regions"].items()
        if item.get("error")
        and (item["status"] != "deferred" or item.get("duration_seconds", 0) > 0)
    ]
    if errors:
        summary.extend(["Fetch failures:", ""])
        summary.extend(f"- {region}: {error}" for region, error in errors[:8])
        summary.append("")
    summary.extend(["| Region | Status | Chart date |", "| --- | --- | --- |"])
    for region, item in sorted(report["regions"].items()):
        summary.append(
            f"| {region} | {item['status']} | {item['chart_date'] or 'missing'} |"
        )
    text = "\n".join(summary) + "\n"
    print(text)
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open(
            "a", encoding="utf-8"
        ) as stream:
            stream.write(text)
    if os.getenv("GITHUB_OUTPUT"):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as stream:
            stream.write(
                f"published={str(published).lower()}\npending={len(pending)}\n"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
