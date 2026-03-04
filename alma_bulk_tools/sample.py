from __future__ import annotations

import html
import json
import random
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .downloader import write_candidates_jsonl
from .models import MousRecord
from .utils import ensure_dir, now_utc_iso

BASELINE_BIN_CENTERS_M = [55.0, 160.0, 300.0, 500.0, 780.0, 1400.0, 2500.0, 3600.0, 8500.0, 13900.0, 16200.0]
SPW_WIDTH_BIN_CENTERS_MHZ = [15.625, 31.25, 62.5, 120.0, 234.375, 468.75, 937.5, 1875.0, 2000.0]
SAMPLING_DIMS = [
    "science_category",
    "band",
    "array",
    "max_baseline_bin",
    "min_spw_width_bin",
    "max_spw_width_bin",
]
GRID_SPECS = [
    ("science_category", "band"),
    ("science_category", "max_baseline_bin"),
    ("band", "array"),
    ("max_baseline_bin", "max_spw_width_bin"),
    ("min_spw_width_bin", "max_spw_width_bin"),
]


def _first_band(record: MousRecord) -> str:
    if not record.band_list:
        return "UNKNOWN"
    return str(record.band_list[0])


def _nearest_center_label(value: float | None, centers: list[float], unit: str) -> str:
    if value is None:
        return "UNKNOWN"
    center = min(centers, key=lambda item: abs(item - value))
    if unit == "m":
        return f"{int(center)} m"
    if center >= 1000.0:
        return f"{center / 1000.0:.3f}".rstrip("0").rstrip(".") + " GHz"
    return f"{center:.3f}".rstrip("0").rstrip(".") + " MHz"


def _science_category(record: MousRecord) -> str:
    meta = record.archive_meta or {}
    return str(meta.get("science_category") or "UNKNOWN")


def _array_bin(record: MousRecord) -> str:
    meta = record.archive_meta or {}
    value = str(meta.get("array") or "UNKNOWN")
    if value in {"12m", "7m", "TP"}:
        return value
    if value == "12m+TP":
        return "TP"
    if value == "12m+7m":
        return "7m"
    if value == "7m+TP":
        return "TP"
    if value == "12m+7m+TP":
        return "TP"
    return value


def _annotate_record(record: MousRecord) -> dict[str, Any]:
    meta = record.archive_meta or {}
    return {
        "record": record,
        "project_code": record.project_code,
        "science_category": _science_category(record),
        "band": _first_band(record),
        "array": _array_bin(record),
        "max_baseline_bin": _nearest_center_label(meta.get("max_baseline_m"), BASELINE_BIN_CENTERS_M, "m"),
        "min_spw_width_bin": _nearest_center_label(meta.get("min_spw_total_width_mhz"), SPW_WIDTH_BIN_CENTERS_MHZ, "mhz"),
        "max_spw_width_bin": _nearest_center_label(meta.get("max_spw_total_width_mhz"), SPW_WIDTH_BIN_CENTERS_MHZ, "mhz"),
    }


def _singleton_keys(row: dict[str, Any]) -> set[tuple[str, str]]:
    return {(dim, str(row[dim])) for dim in SAMPLING_DIMS}


def _pair_keys(row: dict[str, Any]) -> set[tuple[str, str, str, str]]:
    out: set[tuple[str, str, str, str]] = set()
    for idx, left in enumerate(SAMPLING_DIMS):
        for right in SAMPLING_DIMS[idx + 1 :]:
            out.add((left, str(row[left]), right, str(row[right])))
    return out


def _rarity_score(row: dict[str, Any], counts: dict[str, Counter[str]]) -> float:
    score = 0.0
    for dim in SAMPLING_DIMS:
        count = counts[dim][str(row[dim])]
        if count > 0:
            score += 1.0 / count
    return score


def _sample_rows(
    rows: list[dict[str, Any]],
    *,
    target_size: int | None,
    seed: int,
    max_per_project: int | None,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    rng = random.Random(seed)
    counts_by_dim = {
        dim: Counter(str(row[dim]) for row in rows)
        for dim in SAMPLING_DIMS
    }
    remaining_singletons = set().union(*(_singleton_keys(row) for row in rows))
    remaining_pairs = set().union(*(_pair_keys(row) for row in rows))
    selected: list[dict[str, Any]] = []
    project_counts: Counter[str] = Counter()
    available = list(rows)

    while available:
        if target_size is not None and len(selected) >= target_size:
            break

        best_idx = -1
        best_score = -1.0
        best_tiebreak = -1.0
        for idx, row in enumerate(available):
            project = row["project_code"]
            if max_per_project is not None and project_counts[project] >= max_per_project:
                continue
            new_singletons = len(_singleton_keys(row) & remaining_singletons)
            new_pairs = len(_pair_keys(row) & remaining_pairs)
            rarity = _rarity_score(row, counts_by_dim)
            score = new_pairs * 1000.0 + new_singletons * 100.0 + rarity
            tiebreak = rng.random()
            if score > best_score or (score == best_score and tiebreak > best_tiebreak):
                best_idx = idx
                best_score = score
                best_tiebreak = tiebreak

        if best_idx < 0:
            if max_per_project is None:
                break
            # Relax the cap only after exhausting capped coverage.
            max_per_project = None
            continue

        chosen = available.pop(best_idx)
        selected.append(chosen)
        project_counts[chosen["project_code"]] += 1
        remaining_singletons -= _singleton_keys(chosen)
        remaining_pairs -= _pair_keys(chosen)

        if target_size is None and not remaining_singletons and not remaining_pairs:
            break

    return selected


def _supplemental_output_path(out_path: Path) -> Path:
    return out_path.with_name(f"{out_path.stem}.supplemental{out_path.suffix}")


def _supplemental_rows(
    rows: list[dict[str, Any]],
    *,
    selected_rows: list[dict[str, Any]],
    seed: int,
) -> list[dict[str, Any]]:
    selected_projects = {str(row["project_code"]) for row in selected_rows}
    by_project: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        project = str(row["project_code"])
        if project in selected_projects:
            continue
        if str(row["array"]) not in {"7m", "12m"}:
            continue
        by_project.setdefault(project, []).append(row)

    rng = random.Random(seed + 1009)
    supplemental: list[dict[str, Any]] = []
    for project in sorted(by_project):
        choices = list(by_project[project])
        rng.shuffle(choices)
        supplemental.append(choices[0])
    return supplemental


def _grid_counts(rows: list[dict[str, Any]], left: str, right: str) -> tuple[list[str], list[str], dict[tuple[str, str], int]]:
    left_labels = sorted({str(row[left]) for row in rows})
    right_labels = sorted({str(row[right]) for row in rows})
    counts: dict[tuple[str, str], int] = Counter((str(row[left]), str(row[right])) for row in rows)
    return left_labels, right_labels, counts


def _cell_style(selected: int, total: int, max_total: int) -> str:
    if total <= 0 or max_total <= 0:
        return "background:#f5f5f5;color:#333;"
    intensity = int(235 - (total / max_total) * 160)
    border = "#d0d0d0"
    if selected > 0:
        return f"background:rgb({intensity},{240 - min(selected * 10, 90)},{intensity});color:#111;border:1px solid {border};"
    return f"background:rgb(245,245,245);color:#444;border:1px solid {border};"


def _html_grid(title: str, population: list[dict[str, Any]], selected: list[dict[str, Any]], left: str, right: str) -> str:
    left_labels, right_labels, pop_counts = _grid_counts(population, left, right)
    _, _, sel_counts = _grid_counts(selected, left, right)
    max_total = max(pop_counts.values()) if pop_counts else 0
    parts = [f"<h2>{html.escape(title)}</h2>", "<table>"]
    parts.append("<tr><th></th>" + "".join(f"<th>{html.escape(label)}</th>" for label in right_labels) + "</tr>")
    for left_label in left_labels:
        parts.append(f"<tr><th>{html.escape(left_label)}</th>")
        for right_label in right_labels:
            total = int(pop_counts.get((left_label, right_label), 0))
            selected_count = int(sel_counts.get((left_label, right_label), 0))
            frac = f"{selected_count / total:.0%}" if total else "0%"
            style = _cell_style(selected_count, total, max_total)
            parts.append(
                f"<td style='{style}'><div>{selected_count}/{total}</div><div class='sub'>{frac}</div></td>"
            )
        parts.append("</tr>")
    parts.append("</table>")
    return "".join(parts)


def _report_html(population: list[dict[str, Any]], selected: list[dict[str, Any]]) -> str:
    sections = []
    for left, right in GRID_SPECS:
        title = f"{left} vs {right}"
        sections.append(_html_grid(title, population, selected, left, right))
    return (
        "<!doctype html><html><head><meta charset='utf-8'><title>ALMA Bulk Sampling Report</title>"
        "<style>body{font-family:Georgia,serif;margin:24px;background:#fbfaf7;color:#181818}"
        "table{border-collapse:collapse;margin:18px 0 36px 0;font-size:13px}"
        "th,td{padding:6px 8px;border:1px solid #d0d0d0;text-align:center;vertical-align:middle}"
        "th{background:#eee8dc}.sub{font-size:11px;color:#333}</style></head><body>"
        f"<h1>ALMA Bulk Sampling Report</h1><p>Generated {html.escape(now_utc_iso())}. Cells show selected/population and selection fraction.</p>"
        + "".join(sections)
        + "</body></html>"
    )


def create_stratified_sample(
    *,
    records: list[MousRecord],
    out_path: Path,
    report_dir: Path,
    target_size: int | None,
    seed: int,
    max_per_project: int | None,
) -> dict[str, Any]:
    annotated = [_annotate_record(record) for record in records]
    selected_rows = _sample_rows(
        annotated,
        target_size=target_size,
        seed=seed,
        max_per_project=max_per_project,
    )
    selected_records = [row["record"] for row in selected_rows]
    supplemental_rows = _supplemental_rows(annotated, selected_rows=selected_rows, seed=seed)
    supplemental_records = [row["record"] for row in supplemental_rows]
    supplemental_path = _supplemental_output_path(out_path)

    ensure_dir(report_dir)
    write_candidates_jsonl(out_path, selected_records, "sampled_from_discover_candidates")
    write_candidates_jsonl(supplemental_path, supplemental_records, "supplemental_sampled_from_discover_candidates")
    (report_dir / "coverage_report.html").write_text(_report_html(annotated, selected_rows), encoding="utf-8")

    summary = {
        "created_at": now_utc_iso(),
        "input_records": len(records),
        "selected_records": len(selected_records),
        "supplemental_records": len(supplemental_records),
        "seed": seed,
        "target_size": target_size,
        "max_per_project": max_per_project,
        "dimensions": SAMPLING_DIMS,
        "selected_mous_uids": [record.member_ous_uid for record in selected_records],
        "supplemental_mous_uids": [record.member_ous_uid for record in supplemental_records],
        "supplemental_output_path": str(supplemental_path),
        "selected_records_preview": [asdict(record) for record in selected_records[:10]],
        "supplemental_records_preview": [asdict(record) for record in supplemental_records[:10]],
    }
    (report_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary
