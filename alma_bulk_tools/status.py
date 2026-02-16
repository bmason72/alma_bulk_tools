from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


def _bin_month(date_str: str | None) -> str:
    if not date_str:
        return "unknown"
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", ""))
        return dt.strftime("%Y-%m")
    except ValueError:
        if len(date_str) >= 7:
            return date_str[:7]
        return "unknown"


def build_status_report(conn: sqlite3.Connection, top_n_errors: int = 10) -> dict[str, Any]:
    counts = {}
    counts["discovered"] = conn.execute("SELECT COUNT(*) FROM mous WHERE discovered=1").fetchone()[0]
    counts["downloaded"] = conn.execute("SELECT COUNT(*) FROM mous WHERE downloaded=1").fetchone()[0]
    counts["unpacked"] = conn.execute("SELECT COUNT(*) FROM mous WHERE unpacked=1").fetchone()[0]
    counts["summarized"] = conn.execute("SELECT COUNT(*) FROM mous WHERE summarized=1").fetchone()[0]
    counts["indexed"] = conn.execute("SELECT COUNT(*) FROM mous WHERE indexed=1").fetchone()[0]

    fail_rows = conn.execute(
        "SELECT COALESCE(last_error_stage, 'none') AS stage, COALESCE(last_error_message, '') AS msg FROM mous"
    ).fetchall()
    by_stage = Counter(row[0] for row in fail_rows if row[0] != "none")
    msg_counts = Counter(row[1] for row in fail_rows if row[1])

    bands = Counter()
    for row in conn.execute("SELECT band_json FROM mous").fetchall():
        band_json = row[0] or "[]"
        try:
            items = json.loads(band_json)
        except json.JSONDecodeError:
            items = []
        if not items:
            bands["unknown"] += 1
            continue
        for b in items:
            bands[str(b)] += 1

    date_bins = Counter(_bin_month(r[0]) for r in conn.execute("SELECT release_date FROM mous").fetchall())

    todo = {
        "missing_qa": conn.execute(
            """
            SELECT COUNT(*) FROM mous
            WHERE summarized=1 AND COALESCE(asa_qa_present, 0)=0
            """
        ).fetchone()[0],
        "missing_summary": conn.execute("SELECT COUNT(*) FROM mous WHERE summarized=0").fetchone()[0],
        "failed_downloads": conn.execute(
            "SELECT COUNT(*) FROM artifact WHERE status='error'"
        ).fetchone()[0],
        "failed_auxiliary_downloads": conn.execute(
            "SELECT COUNT(*) FROM artifact WHERE status='error' AND kind='auxiliary'"
        ).fetchone()[0],
    }

    return {
        "counts": counts,
        "failure_by_stage": dict(by_stage),
        "top_errors": msg_counts.most_common(top_n_errors),
        "band_coverage": dict(sorted(bands.items())),
        "release_date_bins": dict(sorted(date_bins.items())),
        "todo": todo,
    }


def format_status_report(report: dict[str, Any]) -> str:
    lines = []
    lines.append("ALMA Bulk Status")
    lines.append("================")
    lines.append(
        "Counts: discovered={discovered} downloaded={downloaded} unpacked={unpacked} summarized={summarized} indexed={indexed}".format(
            **report["counts"]
        )
    )

    lines.append("\nFailures by stage:")
    if report["failure_by_stage"]:
        for stage, count in sorted(report["failure_by_stage"].items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"- {stage}: {count}")
    else:
        lines.append("- none")

    lines.append("\nTop error messages:")
    if report["top_errors"]:
        for msg, count in report["top_errors"]:
            lines.append(f"- ({count}) {msg}")
    else:
        lines.append("- none")

    lines.append("\nCoverage by band:")
    for band, count in report["band_coverage"].items():
        lines.append(f"- {band}: {count}")

    lines.append("\nCoverage by release month:")
    for month, count in report["release_date_bins"].items():
        lines.append(f"- {month}: {count}")

    lines.append("\nTo do next:")
    for key, value in report["todo"].items():
        lines.append(f"- {key}: {value}")

    return "\n".join(lines) + "\n"
