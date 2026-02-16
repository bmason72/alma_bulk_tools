from __future__ import annotations

import csv
import io
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

import requests

from .config import load_list_from_value
from .models import MousRecord
from .utils import parse_band_token

LOGGER = logging.getLogger(__name__)


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
        "obs_release_date, t_min, t_max, qa2_passed, target_name, science_keyword, "
        "scan_intent, antenna_arrays "
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
            "source_rows": 0,
            "archive_meta": {},
        }
    )
    for row in rows:
        mous = (row.get("member_ous_uid") or "").strip()
        if not mous:
            continue
        item = grouped[mous]
        item["source_rows"] += 1
        if not item["project_code"]:
            item["project_code"] = row.get("proposal_id")
        if not item["group_ous_uid"]:
            item["group_ous_uid"] = row.get("group_ous_uid")
        eb_uid = (row.get("asdm_uid") or "").strip()
        if eb_uid:
            item["eb_uids"].add(eb_uid)
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

    mous_include = set(load_list_from_value(filters.get("mous_include")))
    mous_exclude = set(load_list_from_value(filters.get("mous_exclude")))

    out: list[MousRecord] = []
    for mous_uid, item in grouped.items():
        if mous_include and mous_uid not in mous_include:
            continue
        if mous_uid in mous_exclude:
            continue
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
                archive_meta=item["archive_meta"],
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
