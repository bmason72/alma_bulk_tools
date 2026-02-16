from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .layout import SUMMARY_FILENAME
from .utils import atomic_write_json, atomic_write_text, load_json, now_utc_iso

LOGGER = logging.getLogger(__name__)
EB_UID_SEGMENT_RE = re.compile(
    r"uid___A002_X[0-9A-Za-z]+_X[0-9A-Za-z]+(?![0-9A-Za-z_])", re.IGNORECASE
)
EB_UID_URI_RE = re.compile(r"uid://A002/X[0-9A-Za-z]+/X[0-9A-Za-z]+(?![0-9A-Za-z_])")


def _iter_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return [p for p in root.rglob("*") if p.is_file()]


def _tag_local(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _uid_segment_to_uri(segment: str) -> str | None:
    token = segment.strip()
    if token.lower().startswith("uid___"):
        token = token[6:]
    parts = token.split("_")
    if len(parts) != 3:
        return None
    if parts[0].upper() != "A002":
        return None
    if not parts[1].startswith("X") or not parts[2].startswith("X"):
        return None
    return f"uid://{parts[0]}/{parts[1]}/{parts[2]}"


def _extract_eb_uids_from_text(values: list[str]) -> set[str]:
    out: set[str] = set()
    for value in values:
        if not value:
            continue
        if "uid://A002/" in value:
            for token in EB_UID_URI_RE.findall(value):
                out.add(token)
        for match in EB_UID_SEGMENT_RE.findall(value):
            uid = _uid_segment_to_uri(match)
            if uid:
                out.add(uid)
    return out


def find_run_artifacts(run_dir: Path) -> dict[str, list[str]]:
    paths = _iter_files(run_dir)
    out: dict[str, list[str]] = defaultdict(list)
    for path in paths:
        name = path.name.lower()
        rel_parts = [part.lower() for part in path.relative_to(run_dir).parts]
        if name == "pipeline_aquareport.xml":
            out["pipeline_aquareport_xml"].append(str(path))
        if name == "applycalqa_outliers.txt":
            out["applycalqa_outliers"].append(str(path))
        if "flagtemplate" in name and name.endswith(".txt"):
            out["flag_templates"].append(str(path))
        if name.endswith(".pprequest.xml"):
            out["pprequest_xml"].append(str(path))
        if "qa" in rel_parts:
            out["qa_files"].append(str(path))
        if name.endswith(".qa0_report.pdf") or name.endswith(".qa2_report.pdf"):
            out["qa_report_files"].append(str(path))
        if "html" in rel_parts and name in {"index.html", "t1-1.html"}:
            out["weblog_landing_candidates"].append(str(path))
    return dict(out)


def find_key_artifacts(delivered_dir: Path) -> dict[str, list[str]]:
    # Backward-compatible alias used by helper scripts.
    return find_run_artifacts(delivered_dir)


def _choose_weblog_landing(candidates: list[str]) -> str | None:
    if not candidates:
        return None
    sorted_paths = sorted(candidates)
    index_matches = [p for p in sorted_paths if p.lower().endswith("/index.html")]
    if index_matches:
        return index_matches[0]
    t1_matches = [p for p in sorted_paths if p.lower().endswith("/t1-1.html")]
    if t1_matches:
        return t1_matches[0]
    return sorted_paths[0]


def parse_aqua_report(xml_path: Path) -> dict[str, Any]:
    root = ET.fromstring(xml_path.read_bytes())
    topics = []
    stages = []

    for elem in root.iter():
        name = _tag_local(elem.tag).lower()
        if "qapertopic" in name:
            topic = {
                "topic": elem.attrib.get("Topic") or elem.attrib.get("topic") or elem.attrib.get("name"),
                "score": elem.attrib.get("Score") or elem.attrib.get("score"),
                "reason": elem.attrib.get("Reason") or elem.attrib.get("reason"),
            }
            text = (elem.text or "").strip()
            if text and not topic["reason"]:
                topic["reason"] = text
            topics.append(topic)
        if "qaperstage" in name or "representativescore" in name or "subscore" in name:
            stages.append(
                {
                    "tag": _tag_local(elem.tag),
                    "name": elem.attrib.get("Name") or elem.attrib.get("name"),
                    "score": elem.attrib.get("Score") or elem.attrib.get("score"),
                    "reason": elem.attrib.get("Reason") or elem.attrib.get("reason"),
                }
            )

    qa_reasons = [
        t["reason"]
        for t in topics
        if t.get("reason") and isinstance(t.get("reason"), str) and t["reason"].strip()
    ]
    return {
        "qa_per_topic": topics,
        "qa_per_stage": stages,
        "qa_reasons": sorted(set(r.strip() for r in qa_reasons)),
    }


_REASON_RE = re.compile(r"(?:reason|comment)\s*=\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)


def parse_flag_template(path: Path) -> dict[str, Any]:
    commands = 0
    manual_commands = 0
    reasons: Counter[str] = Counter()
    recent_comments: list[str] = []

    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            c = line.lstrip("#").strip()
            if c:
                recent_comments.append(c)
                if len(recent_comments) > 3:
                    recent_comments = recent_comments[-3:]
            continue

        commands += 1
        line_lower = line.lower()
        is_manual = "mode='manual'" in line_lower or 'mode="manual"' in line_lower
        if is_manual:
            manual_commands += 1

        match = _REASON_RE.search(line)
        if match:
            reasons[match.group(1).strip()] += 1
        else:
            for c in recent_comments:
                if any(k in c.lower() for k in ["reason", "flag", "rfi", "bad", "manual"]):
                    reasons[c] += 1
            recent_comments = []

    return {
        "path": str(path),
        "dr_flag_commands_count": commands,
        "dr_manual_flag_commands_count": manual_commands,
        "dr_action_evidence": manual_commands > 0,
        "dr_intervention_suspected": manual_commands > 0,
        "flag_reasons": [{"reason": r, "count": c} for r, c in reasons.most_common(20)],
    }


def parse_pprequest(path: Path) -> dict[str, Any]:
    root = ET.fromstring(path.read_bytes())
    eb_uids: set[str] = set()
    for elem in root.iter():
        if _tag_local(elem.tag).lower() != "intents":
            continue
        keyword = None
        value = None
        for child in list(elem):
            child_name = _tag_local(child.tag).lower()
            if child_name == "keyword":
                keyword = (child.text or "").strip()
            elif child_name == "value":
                value = (child.text or "").strip()
        if keyword and keyword.upper().startswith("SESSION_") and value:
            if value.startswith("uid://A002/"):
                eb_uids.add(value)
    return {"path": str(path), "eb_uids": sorted(eb_uids)}


def _normalize_qa2_status(raw_status: Any, raw_bool: Any) -> str:
    value = raw_status
    if isinstance(value, str):
        text = value.strip().upper()
        if text in {"PASS", "SEMIPASS", "FAIL", "UNKNOWN"}:
            return text
        if text in {"TRUE", "T", "1"}:
            return "PASS"
        if text in {"FALSE", "F", "0"}:
            return "FAIL"
    if isinstance(raw_bool, bool):
        return "PASS" if raw_bool else "FAIL"
    if isinstance(raw_bool, str):
        low = raw_bool.strip().lower()
        if low in {"true", "t", "1"}:
            return "PASS"
        if low in {"false", "f", "0"}:
            return "FAIL"
    return "UNKNOWN"


def _normalize_qa0_status(raw_status: Any) -> str:
    if raw_status is None:
        return "UNKNOWN"
    if isinstance(raw_status, bool):
        return "PASS" if raw_status else "FAIL"
    text = str(raw_status).strip().upper()
    if text in {"PASS", "SEMIPASS", "FAIL", "UNKNOWN"}:
        return text
    if text in {"TRUE", "T", "1"}:
        return "PASS"
    if text in {"FALSE", "F", "0"}:
        return "FAIL"
    return "UNKNOWN"


def _explicit_qa0_by_eb(manifest: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    raw = manifest.get("qa0_by_eb") or manifest.get("eb_qa0_status")
    if isinstance(raw, dict):
        for eb_uid, status in raw.items():
            out[str(eb_uid)] = _normalize_qa0_status(status)
        return out
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            eb_uid = item.get("eb_uid") or item.get("uid") or item.get("asdm_uid")
            if not eb_uid:
                continue
            status = item.get("qa0_status") or item.get("status")
            out[str(eb_uid)] = _normalize_qa0_status(status)
    return out


def _summarize_run(run_dir: Path) -> tuple[dict[str, Any], set[str], set[str]]:
    artifacts = find_run_artifacts(run_dir)
    ppr_entries = []
    ppr_ebs: set[str] = set()
    for item in artifacts.get("pprequest_xml", []):
        try:
            parsed = parse_pprequest(Path(item))
            ppr_entries.append(parsed)
            for eb_uid in parsed.get("eb_uids", []):
                ppr_ebs.add(eb_uid)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed parsing pprequest %s: %s", item, exc)
            ppr_entries.append({"path": item, "error": str(exc), "eb_uids": []})

    flag_entries = []
    dr_flags_total = 0
    dr_manual_total = 0
    dr_action_evidence = False
    reason_counts: Counter[str] = Counter()
    for item in artifacts.get("flag_templates", []):
        try:
            parsed = parse_flag_template(Path(item))
            flag_entries.append(parsed)
            dr_flags_total += int(parsed.get("dr_flag_commands_count") or 0)
            dr_manual_total += int(parsed.get("dr_manual_flag_commands_count") or 0)
            dr_action_evidence = dr_action_evidence or bool(parsed.get("dr_action_evidence"))
            for reason in parsed.get("flag_reasons", []):
                reason_counts[str(reason.get("reason"))] += int(reason.get("count") or 0)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed parsing flag template %s: %s", item, exc)

    aqua_reports = []
    qa2_reasons: set[str] = set()
    for item in artifacts.get("pipeline_aquareport_xml", []):
        try:
            parsed = parse_aqua_report(Path(item))
            aqua_reports.append({"path": item, "qa_reasons": parsed.get("qa_reasons", []), "parsed": parsed})
            for reason in parsed.get("qa_reasons", []):
                qa2_reasons.add(reason)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed parsing AQUA report %s: %s", item, exc)
            aqua_reports.append({"path": item, "error": str(exc), "qa_reasons": []})

    run_file_paths = [str(p) for p in _iter_files(run_dir)]
    eb_uids_detected = (
        set(ppr_ebs)
        | _extract_eb_uids_from_text(run_file_paths)
        | _extract_eb_uids_from_text(artifacts.get("qa_report_files", []))
        | _extract_eb_uids_from_text(artifacts.get("flag_templates", []))
    )

    run_summary = {
        "path": str(run_dir),
        "has_contents": bool(run_file_paths),
        "file_count": len(run_file_paths),
        "pprequest_files": sorted(artifacts.get("pprequest_xml", [])),
        "pprequest": ppr_entries,
        "pipeline_aquareport_files": sorted(artifacts.get("pipeline_aquareport_xml", [])),
        "applycalqa_outliers_files": sorted(artifacts.get("applycalqa_outliers", [])),
        "weblog_landing_page": _choose_weblog_landing(artifacts.get("weblog_landing_candidates", [])),
        "weblog_landing_candidates": sorted(artifacts.get("weblog_landing_candidates", [])),
        "flag_templates": flag_entries,
        "dr_action_evidence": dr_action_evidence,
        "dr_flag_commands_count": dr_flags_total,
        "dr_manual_flag_commands_count": dr_manual_total,
        "dr_flag_reason_summary": [
            {"reason": reason, "count": count} for reason, count in reason_counts.most_common(20)
        ],
        "qa_report_files": sorted(artifacts.get("qa_report_files", [])),
        "qa_files": sorted(artifacts.get("qa_files", [])),
        "aqua_reports": aqua_reports,
        "eb_uids_detected": sorted(eb_uids_detected),
    }

    return run_summary, set(eb_uids_detected), qa2_reasons


def summarize_mous(
    *,
    mous_dir: Path,
    manifest_path: Path,
    query_timestamp: str | None,
    tool_version: str,
    write_markdown: bool,
) -> dict[str, Any]:
    manifest = load_json(manifest_path, default={}) or {}
    summary_path = mous_dir / SUMMARY_FILENAME

    run_payloads: dict[str, dict[str, Any]] = {}
    delivered_ebs: set[str] = set()
    run1_ebs: set[str] = set()
    qa2_reasons_from_aqua: set[str] = set()
    for run_name in ("delivered", "run1"):
        run_dir = mous_dir / run_name
        run_summary, run_eb_uids, run_qa2_reasons = _summarize_run(run_dir)
        run_payloads[run_name] = run_summary
        if run_name == "delivered":
            delivered_ebs |= run_eb_uids
        if run_name == "run1":
            run1_ebs |= run_eb_uids
        qa2_reasons_from_aqua |= set(run_qa2_reasons)

    explicit_qa0 = _explicit_qa0_by_eb(manifest)
    manifest_ebs = set(manifest.get("eb_uids") or [])
    all_ebs = sorted(manifest_ebs | delivered_ebs | run1_ebs | set(explicit_qa0.keys()))

    eb_in_asa = []
    for eb_uid in all_ebs:
        if eb_uid in explicit_qa0:
            eb_in_asa.append(
                {
                    "eb_uid": eb_uid,
                    "qa0_status": explicit_qa0[eb_uid],
                    "qa0_status_suggested": False,
                    "qa0_status_source": "archive_explicit",
                }
            )
            continue
        if eb_uid in delivered_ebs:
            eb_in_asa.append(
                {
                    "eb_uid": eb_uid,
                    "qa0_status": "PASS",
                    "qa0_status_suggested": True,
                    "qa0_status_source": "inferred_from_delivered_presence",
                }
            )
            continue
        if eb_uid in run1_ebs and eb_uid not in delivered_ebs:
            eb_in_asa.append(
                {
                    "eb_uid": eb_uid,
                    "qa0_status": "SEMIPASS",
                    "qa0_status_suggested": True,
                    "qa0_status_source": "inferred_from_run1_only_presence",
                }
            )
            continue
        eb_in_asa.append(
            {
                "eb_uid": eb_uid,
                "qa0_status": "UNKNOWN",
                "qa0_status_suggested": False,
                "qa0_status_source": "unknown",
            }
        )

    qa2_reasons = set(manifest.get("qa2_reasons") or []) | qa2_reasons_from_aqua
    qa2_status = _normalize_qa2_status(manifest.get("qa2_status"), manifest.get("qa2_passed"))

    dr_flags_total = int(run_payloads["delivered"]["dr_flag_commands_count"]) + int(
        run_payloads["run1"]["dr_flag_commands_count"]
    )
    dr_manual_total = int(run_payloads["delivered"]["dr_manual_flag_commands_count"]) + int(
        run_payloads["run1"]["dr_manual_flag_commands_count"]
    )
    dr_intervention = bool(run_payloads["delivered"]["dr_action_evidence"]) or bool(
        run_payloads["run1"]["dr_action_evidence"]
    )
    dr_reasons: Counter[str] = Counter()
    for run_name in ("delivered", "run1"):
        for reason in run_payloads[run_name].get("dr_flag_reason_summary", []):
            dr_reasons[str(reason.get("reason"))] += int(reason.get("count") or 0)

    summary = {
        "schema_version": 2,
        "mous": {
            "project_code": manifest.get("project_code"),
            "public_release_date": manifest.get("release_date"),
            "mous_uid": manifest.get("mous_uid"),
            "group_ous_uid": manifest.get("group_ous_uid"),
            "science_goal_uid": manifest.get("science_goal_uid"),
            "eb_uid_list": sorted(all_ebs),
            "band": manifest.get("band_list", []),
            "obs_date": manifest.get("obs_date"),
        },
        "qa": {
            "qa2_status": qa2_status,
            "qa2_reasons": sorted(qa2_reasons),
            "eb_in_asa": eb_in_asa,
        },
        "has_delivered_products": bool(run_payloads["delivered"]["has_contents"]),
        "has_run1_products": bool(run_payloads["run1"]["has_contents"]),
        "runs": run_payloads,
        "dr": {
            "dr_intervention_suspected": dr_intervention,
            "dr_flag_commands_count": dr_flags_total,
            "dr_manual_flag_commands_count": dr_manual_total,
            "dr_flag_reason_summary": [
                {"reason": reason, "count": count} for reason, count in dr_reasons.most_common(20)
            ],
        },
        "artifacts_present": [
            {
                "kind": a.get("kind"),
                "filename": a.get("filename"),
                "local_path": a.get("local_path"),
                "url": a.get("url"),
                "size_bytes": a.get("size_bytes"),
                "checksum": a.get("checksum"),
                "status": a.get("status"),
            }
            for a in manifest.get("artifacts", [])
        ],
        "provenance": {
            "query_timestamp": query_timestamp,
            "tool_version": tool_version,
            "manifest_path": str(manifest_path),
            "summary_generated_at": now_utc_iso(),
        },
        "updated_at": now_utc_iso(),
    }

    atomic_write_json(summary_path, summary)

    if write_markdown:
        lines = [
            f"# {summary.get('mous', {}).get('mous_uid', 'unknown MOUS')}",
            "",
            f"- Project code: {summary.get('mous', {}).get('project_code')}",
            f"- Public release date: {summary.get('mous', {}).get('public_release_date')}",
            f"- Bands: {', '.join(summary.get('mous', {}).get('band') or [])}",
            f"- QA2 status: {summary.get('qa', {}).get('qa2_status')}",
            f"- Has delivered products: {summary.get('has_delivered_products')}",
            f"- Has run1 products: {summary.get('has_run1_products')}",
            f"- DR intervention suspected: {summary.get('dr', {}).get('dr_intervention_suspected')}",
            f"- Artifacts present: {len(summary.get('artifacts_present') or [])}",
        ]
        atomic_write_text(mous_dir / "summary.md", "\n".join(lines) + "\n")

    return summary
