from __future__ import annotations

import csv
import io
import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

import requests

from .config import load_list_from_value
from .models import MousRecord
from .utils import parse_band_token

LOGGER = logging.getLogger(__name__)
FREQUENCY_RANGE_RE = re.compile(r"\[[^\]]+\]")
WIDTH_RE = re.compile(r"([-+]?\d+(?:\.\d+)?)\s*(Hz|kHz|MHz|GHz)\b", re.IGNORECASE)
RANGE_RE = re.compile(
    r"([-+]?\d+(?:\.\d+)?)\s*\.\.\s*([-+]?\d+(?:\.\d+)?)\s*(Hz|kHz|MHz|GHz)\b", re.IGNORECASE
)
INT_RE = re.compile(r"^\d+$")


def _date_to_mjd(value: str) -> float:
    dt = date.fromisoformat(value)
    epoch = date(1858, 11, 17)
    return float((dt - epoch).days)


def _band_number(token: str) -> str:
    text = parse_band_token(token)
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits


def _band_match_clause(token: str) -> str:
    num = _band_number(token)
    if not num:
        band = parse_band_token(token)
        return f"UPPER(band_list) LIKE '%{band}%'"

    candidates = [
        f"band_list = '{num}'",
        f"band_list LIKE '{num},%'",
        f"band_list LIKE '{num}, %'",
        f"band_list LIKE '%,{num}'",
        f"band_list LIKE '%, {num}'",
        f"band_list LIKE '%,{num},%'",
        f"band_list LIKE '%, {num},%'",
        f"band_list LIKE '%,{num}, %'",
        f"band_list LIKE '%, {num}, %'",
        f"UPPER(band_list) LIKE '%BAND {num}%'",
    ]
    return "(" + " OR ".join(candidates) + ")"


def _build_where_clauses(
    *,
    start: str,
    end: str,
    date_field: str,
    exclude_tp: bool,
    exclude_7m: bool,
    bands_include: list[str],
    bands_exclude: list[str],
    project_codes_include: list[str],
    project_codes_exclude: list[str],
    min_freq_ghz: float | None,
    max_freq_ghz: float | None,
) -> list[str]:
    clauses = ["data_rights = 'Public'", "member_ous_uid IS NOT NULL"]
    if date_field == "release":
        clauses.append(f"obs_release_date >= '{start}'")
        clauses.append(f"obs_release_date < '{end}'")
    else:
        start_mjd = _date_to_mjd(start)
        end_mjd = _date_to_mjd(end)
        clauses.append(f"t_min IS NOT NULL")
        clauses.append(f"t_min >= {start_mjd}")
        clauses.append(f"t_min < {end_mjd}")

    if exclude_tp:
        clauses.append(
            "(antenna_arrays IS NULL OR (LOWER(antenna_arrays) NOT LIKE '%pm%' "
            "AND LOWER(antenna_arrays) NOT LIKE '%tp%'))"
        )
    if exclude_7m:
        clauses.append(
            "(antenna_arrays IS NULL OR (LOWER(antenna_arrays) NOT LIKE '%cm%' "
            "AND LOWER(antenna_arrays) NOT LIKE '%7m%'))"
        )

    for band in bands_include:
        clauses.append(_band_match_clause(band))
    for band in bands_exclude:
        clauses.append(f"NOT {_band_match_clause(band)}")

    if project_codes_include:
        joined = ",".join(f"'{p}'" for p in project_codes_include)
        clauses.append(f"proposal_id IN ({joined})")
    if project_codes_exclude:
        joined = ",".join(f"'{p}'" for p in project_codes_exclude)
        clauses.append(f"proposal_id NOT IN ({joined})")

    c_m_per_s = 299_792_458.0
    if min_freq_ghz is not None:
        min_lambda_m = c_m_per_s / (float(min_freq_ghz) * 1e9)
        clauses.append(f"em_min <= {min_lambda_m}")
    if max_freq_ghz is not None:
        max_lambda_m = c_m_per_s / (float(max_freq_ghz) * 1e9)
        clauses.append(f"em_max >= {max_lambda_m}")

    return clauses


def build_adql_query(
    *,
    start: str,
    end: str,
    date_field: str,
    filters: dict[str, Any],
) -> str:
    clauses = _build_where_clauses(
        start=start,
        end=end,
        date_field=date_field,
        exclude_tp=bool(filters.get("exclude_tp", False)),
        exclude_7m=bool(filters.get("exclude_7m", False)),
        bands_include=load_list_from_value(filters.get("bands_include")),
        bands_exclude=load_list_from_value(filters.get("bands_exclude")),
        project_codes_include=load_list_from_value(filters.get("project_codes_include")),
        project_codes_exclude=load_list_from_value(filters.get("project_codes_exclude")),
        min_freq_ghz=filters.get("min_freq_ghz"),
        max_freq_ghz=filters.get("max_freq_ghz"),
    )

    # Uses current documented ALMA ObsCore columns: member_ous_uid, group_ous_uid,
    # asdm_uid, qa2_passed, obs_release_date, band_list, proposal_id.
    return (
        "SELECT "
        "proposal_id, member_ous_uid, group_ous_uid, asdm_uid, band_list, "
        "obs_release_date, t_min, t_max, qa2_passed, target_name, scientific_category, science_keyword, "
        "scan_intent, antenna_arrays, schedblock_name, frequency_support, "
        "science_observation, is_mosaic, frequency, spatial_resolution "
        "FROM ivoa.obscore WHERE "
        + " AND ".join(clauses)
    )


def run_tap_sync(
    tap_sync_url: str,
    adql: str,
    *,
    timeout_sec: int,
    user_agent: str,
) -> list[dict[str, str]]:
    headers = {"User-Agent": user_agent}
    payload = {
        "REQUEST": "doQuery",
        "LANG": "ADQL",
        "FORMAT": "csv",
        "QUERY": adql,
    }
    response = requests.post(tap_sync_url, data=payload, timeout=timeout_sec, headers=headers)
    response.raise_for_status()
    text = response.text.strip()
    if not text:
        return []
    reader = csv.DictReader(io.StringIO(text))
    rows = [dict(row) for row in reader]
    LOGGER.info("TAP returned %s rows", len(rows))
    return rows


def _mjd_to_iso_date(value: str | None) -> str | None:
    if value in (None, "", "NULL"):
        return None
    try:
        mjd = float(value)
    except ValueError:
        return None
    epoch = datetime(1858, 11, 17)
    dt = epoch + timedelta(days=mjd)
    return dt.date().isoformat()


def _as_bool(value: Any) -> bool | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"t", "true", "1", "y", "yes"}:
        return True
    if text in {"f", "false", "0", "n", "no"}:
        return False
    return None


def _normalize_qa2_status(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    text = str(value).strip().upper()
    if text in {"PASS", "P"}:
        return "PASS"
    if text in {"SP", "SEMIPASS", "SEMI-PASS", "S"}:
        return "SP"
    if text in {"FAIL", "F"}:
        return "FAIL"
    if text in {"TRUE", "T", "1"}:
        return "PASS"
    if text in {"FALSE", "0"}:
        return "FAIL"
    return "UNKNOWN"


def _channel_width_to_mhz(value: float, unit: str) -> float:
    unit_l = unit.lower()
    if unit_l == "hz":
        return value / 1_000_000.0
    if unit_l == "khz":
        return value / 1_000.0
    if unit_l == "ghz":
        return value * 1_000.0
    return value


def _parse_frequency_support_details(value: str | None) -> list[tuple[float, int | None]]:
    if not value:
        return []
    details: list[tuple[float, int | None]] = []
    for block in FREQUENCY_RANGE_RE.findall(value):
        parts = [part.strip() for part in block.strip("[]").split(",")]
        if len(parts) < 1:
            continue
        width_mhz: float | None = None
        range_match = RANGE_RE.search(parts[0])
        if range_match:
            low = float(range_match.group(1))
            high = float(range_match.group(2))
            unit = range_match.group(3).lower()
            width_mhz = abs(high - low)
            width_mhz = _channel_width_to_mhz(width_mhz, unit)
        if width_mhz is None:
            continue

        explicit_nchan: int | None = None
        for part in parts[2:]:
            token = part.strip()
            if INT_RE.fullmatch(token):
                explicit_nchan = int(token)
                break
        inferred_nchan = explicit_nchan
        if inferred_nchan is None and len(parts) >= 2:
            channel_match = WIDTH_RE.search(parts[1])
            if channel_match:
                channel_width = float(channel_match.group(1))
                channel_width_mhz = _channel_width_to_mhz(channel_width, channel_match.group(2))
                if channel_width_mhz > 0:
                    inferred_nchan = max(1, int(round(width_mhz / channel_width_mhz)))
        details.append((width_mhz, inferred_nchan))
    return details


def _array_label_from_rows(rows: list[dict[str, str]]) -> str:
    labels: list[str] = []
    has_12m = False
    has_7m = False
    has_tp = False

    for row in rows:
        arrays = (row.get("antenna_arrays") or "").upper()
        if any(token in arrays for token in ("DV", "DA")):
            has_12m = True
        if "CM" in arrays or "7M" in arrays:
            has_7m = True
        if "PM" in arrays or "TP" in arrays:
            has_tp = True

    if has_12m and not labels:
        labels.append("12m")
    if has_7m:
        labels.append("7m")
    if has_tp:
        labels.append("TP")
    return "+".join(labels) if labels else "UNKNOWN"


def _as_float(value: Any) -> float | None:
    if value in (None, "", "NULL"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _infer_max_baseline_m(rows: list[dict[str, str]]) -> float | None:
    max_baseline: float | None = None
    for row in rows:
        spatial_resolution_arcsec = _as_float(row.get("spatial_resolution"))
        representative_frequency_ghz = _as_float(row.get("frequency"))
        if not spatial_resolution_arcsec or not representative_frequency_ghz:
            continue
        if spatial_resolution_arcsec <= 0 or representative_frequency_ghz <= 0:
            continue
        baseline_m = 61836.625 / (spatial_resolution_arcsec * representative_frequency_ghz)
        if max_baseline is None or baseline_m > max_baseline:
            max_baseline = baseline_m
    return max_baseline


def group_rows_to_mous(rows: list[dict[str, str]], filters: dict[str, Any]) -> list[MousRecord]:
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "project_code": None,
            "group_ous_uid": None,
            "science_goal_uid": None,
            "eb_uids": set(),
            "bands": set(),
            "release_date": None,
            "obs_date": None,
            "qa2_passed": None,
            "qa2_status": "UNKNOWN",
            "source_rows": 0,
            "archive_meta": {
                "sb_name": None,
                "science_category": None,
                "execution_count": 0,
                "spw_count": None,
                "min_spw_total_width_mhz": None,
                "max_spw_total_width_mhz": None,
                "min_nchan": None,
                "max_nchan": None,
                "min_spw_width_nchan": None,
                "max_spw_width_nchan": None,
                "array": "UNKNOWN",
                "max_baseline_m": None,
                "science_target_count": 0,
                "is_mosaic": None,
                "qa2_status": "UNKNOWN",
            },
            "science_targets": set(),
            "execution_uids": set(),
            "spw_details": [],
            "spw_count_seen": 0,
            "rows": [],
        }
    )
    for row in rows:
        mous = (row.get("member_ous_uid") or "").strip()
        if not mous:
            continue
        item = grouped[mous]
        item["rows"].append(row)
        item["source_rows"] += 1
        if not item["project_code"]:
            item["project_code"] = row.get("proposal_id")
        if not item["group_ous_uid"]:
            item["group_ous_uid"] = row.get("group_ous_uid")
        if not item["archive_meta"].get("sb_name"):
            item["archive_meta"]["sb_name"] = (row.get("schedblock_name") or "").strip() or None
        if not item["archive_meta"].get("science_category"):
            item["archive_meta"]["science_category"] = (row.get("scientific_category") or "").strip() or None
        eb_uid = (row.get("asdm_uid") or "").strip()
        if eb_uid:
            item["eb_uids"].add(eb_uid)
            item["execution_uids"].add(eb_uid)
        band_raw = row.get("band_list") or ""
        for token in band_raw.replace(";", ",").split(","):
            token = token.strip()
            if token:
                item["bands"].add(parse_band_token(token))
        release_date = (row.get("obs_release_date") or "").strip()
        if release_date:
            if item["release_date"] is None or release_date > item["release_date"]:
                item["release_date"] = release_date
        obs_date = _mjd_to_iso_date((row.get("t_min") or "").strip())
        if obs_date:
            if item["obs_date"] is None or obs_date < item["obs_date"]:
                item["obs_date"] = obs_date
        qa2 = (row.get("qa2_passed") or "").strip().lower()
        if qa2 in {"t", "true", "1"}:
            item["qa2_passed"] = True
        elif qa2 in {"f", "false", "0"} and item["qa2_passed"] is None:
            item["qa2_passed"] = False

        qa2_status = _normalize_qa2_status(row.get("qa2_passed"))
        if qa2_status == "FAIL":
            item["qa2_status"] = "FAIL"
        elif qa2_status == "SP" and item["qa2_status"] != "FAIL":
            item["qa2_status"] = "SP"
        elif qa2_status == "PASS" and item["qa2_status"] == "UNKNOWN":
            item["qa2_status"] = "PASS"

        details = _parse_frequency_support_details(row.get("frequency_support"))
        item["spw_details"].extend(details)
        item["spw_count_seen"] += len(details)

        science_observation = _as_bool(row.get("science_observation"))
        target_name = (row.get("target_name") or "").strip()
        if target_name and science_observation is not False:
            item["science_targets"].add(target_name)

        is_mosaic = _as_bool(row.get("is_mosaic"))
        if is_mosaic is True:
            item["archive_meta"]["is_mosaic"] = True
        elif item["archive_meta"]["is_mosaic"] is None and is_mosaic is False:
            item["archive_meta"]["is_mosaic"] = False

    mous_include = set(load_list_from_value(filters.get("mous_include")))
    mous_exclude = set(load_list_from_value(filters.get("mous_exclude")))

    out: list[MousRecord] = []
    for mous_uid, item in grouped.items():
        if mous_include and mous_uid not in mous_include:
            continue
        if mous_uid in mous_exclude:
            continue
        archive_meta = dict(item["archive_meta"])
        sorted_details = sorted(item["spw_details"], key=lambda pair: (pair[0], pair[1] if pair[1] is not None else -1))
        widths_mhz = [pair[0] for pair in sorted_details]
        nchans = sorted(pair[1] for pair in sorted_details if pair[1] is not None)
        archive_meta["execution_count"] = len(item["execution_uids"])
        archive_meta["spw_count"] = item["spw_count_seen"] or None
        archive_meta["min_spw_total_width_mhz"] = widths_mhz[0] if widths_mhz else None
        archive_meta["max_spw_total_width_mhz"] = widths_mhz[-1] if widths_mhz else None
        archive_meta["min_nchan"] = nchans[0] if nchans else None
        archive_meta["max_nchan"] = nchans[-1] if nchans else None
        archive_meta["min_spw_width_nchan"] = sorted_details[0][1] if sorted_details else None
        archive_meta["max_spw_width_nchan"] = sorted_details[-1][1] if sorted_details else None
        archive_meta["array"] = _array_label_from_rows(item["rows"])
        archive_meta["max_baseline_m"] = _infer_max_baseline_m(item["rows"])
        archive_meta["science_target_count"] = len(item["science_targets"])
        archive_meta["qa2_status"] = item["qa2_status"]
        out.append(
            MousRecord(
                project_code=item["project_code"] or "UNKNOWN",
                member_ous_uid=mous_uid,
                group_ous_uid=item["group_ous_uid"],
                science_goal_uid=item["science_goal_uid"],
                eb_uids=sorted(item["eb_uids"]),
                band_list=sorted(item["bands"]),
                release_date=item["release_date"],
                obs_date=item["obs_date"],
                qa2_passed=item["qa2_passed"],
                source_rows=item["source_rows"],
                archive_meta=archive_meta,
            )
        )
    out.sort(key=lambda r: (r.release_date or "", r.project_code, r.member_ous_uid))
    return out


def discover_mous(
    *,
    tap_sync_url: str,
    timeout_sec: int,
    user_agent: str,
    start: str,
    end: str,
    date_field: str,
    filters: dict[str, Any],
) -> tuple[list[MousRecord], str]:
    adql = build_adql_query(start=start, end=end, date_field=date_field, filters=filters)
    rows = run_tap_sync(
        tap_sync_url=tap_sync_url,
        adql=adql,
        timeout_sec=timeout_sec,
        user_agent=user_agent,
    )
    records = group_rows_to_mous(rows, filters)
    return records, adql
