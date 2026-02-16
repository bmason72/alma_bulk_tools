from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
import xml.etree.ElementTree as ET

import requests
from tqdm import tqdm

from .models import ArtifactInfo, MousRecord
from .utils import atomic_write_json, ensure_dir, load_json, now_utc_iso, sha256_file, uid_to_path_segment

LOGGER = logging.getLogger(__name__)

DEFAULT_ARTIFACT_KINDS = {
    "calibration",
    "scripts",
    "weblog",
    "qa_reports",
    "auxiliary",
    "readme",
    "calibration_products",
}
ALL_NONIMAGE_KINDS = {
    "calibration",
    "scripts",
    "weblog",
    "qa_reports",
    "auxiliary",
    "readme",
    "raw",
    "other",
}

def _normalize_kind(kind: str) -> str:
    return (kind or "").strip().lower()


def _filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.path:
        name = Path(parsed.path).name
        if name:
            return name
    query = parse_qs(parsed.query)
    if "ID" in query and query["ID"]:
        return query["ID"][0].split("/")[-1]
    return "download.dat"


def _classify_artifact(kind_hint: str, semantics: str | None, filename: str) -> str:
    name = filename.lower()
    semantics_l = (semantics or "").lower()
    hint = kind_hint.lower()

    if "readme" in name or "readme" in semantics_l or "documentation" in semantics_l:
        return "readme"
    if "weblog" in name or "weblog" in semantics_l:
        return "weblog"
    if (
        "qa0_report" in name
        or "qa2_report" in name
        or "qa/" in name
        or "/qa/" in semantics_l
        or "#qa" in semantics_l
        or "qa2" in semantics_l
        or "aquareport" in name
    ):
        return "qa_reports"
    if (
        "auxiliary" in name
        or "#auxiliary" in semantics_l
        or "auxiliary" in semantics_l
        or "auxproducts" in name
    ):
        return "auxiliary"
    if "scriptforpi" in name or "script" in semantics_l:
        return "scripts"
    if "calibration" in name or name.endswith(".cal") or "calibration" in semantics_l:
        return "calibration"
    if "calimage" in name or "calimage" in semantics_l:
        return "calibration_products"
    if "cube" in name:
        return "cubes"
    if "cont" in name:
        return "continuum_images"
    if "admit" in name:
        return "admit"
    if "image" in name:
        return "continuum_images"
    if "asdm" in name or "raw" in name:
        return "raw"
    if hint:
        return _normalize_kind(hint)
    return "other"


def resolve_artifact_selection(spec: str) -> set[str]:
    spec = (spec or "default").strip()
    if not spec:
        spec = "default"
    selected: set[str] = set()
    for token in [s.strip() for s in spec.split(",") if s.strip()]:
        token_norm = _normalize_kind(token.lstrip("+-"))
        if token == "default":
            selected.update(DEFAULT_ARTIFACT_KINDS)
            continue
        if token == "all-nonimage":
            selected.update(ALL_NONIMAGE_KINDS)
            continue
        if token.startswith("+"):
            selected.add(token_norm)
            continue
        if token.startswith("-"):
            selected.discard(token_norm)
            continue
        selected.add(token_norm)
    if not selected:
        selected.update(DEFAULT_ARTIFACT_KINDS)
    return selected


def _artifact_selected(kind: str, selected_kinds: set[str]) -> bool:
    return _normalize_kind(kind) in selected_kinds


def _artifact_satisfied_without_archive_file(entry: dict[str, Any] | None) -> bool:
    if not entry:
        return False
    if entry.get("archive_removed_after_unpack") and entry.get("unpacked_to"):
        return True
    return False


def datalink_id_from_member_ous_uid(member_ous_uid: str) -> str:
    value = (member_ous_uid or "").strip()
    if value.startswith("uid___"):
        return value
    if value.startswith("uid://"):
        return uid_to_path_segment(value)
    return value


def fetch_datalink_artifacts(
    *,
    datalink_sync_url: str,
    member_ous_uid: str,
    timeout_sec: int,
    user_agent: str,
) -> list[ArtifactInfo]:
    headers = {"User-Agent": user_agent}
    datalink_id = datalink_id_from_member_ous_uid(member_ous_uid)
    response = requests.get(
        datalink_sync_url,
        params={"ID": datalink_id},
        timeout=timeout_sec,
        headers=headers,
    )
    response.raise_for_status()
    root = ET.fromstring(response.content)

    ns = {
        "v": "http://www.ivoa.net/xml/VOTable/v1.3",
        "v1": "http://www.ivoa.net/xml/VOTable/v1.1",
        "v2": "http://www.ivoa.net/xml/VOTable/v1.2",
    }

    table = None
    for xpath in [".//v:TABLE", ".//v1:TABLE", ".//v2:TABLE", ".//TABLE"]:
        table = root.find(xpath, ns)
        if table is not None:
            break
    if table is None:
        return []

    fields = []
    for fld in table.findall("./v:FIELD", ns) + table.findall("./v1:FIELD", ns) + table.findall(
        "./v2:FIELD", ns
    ) + table.findall("./FIELD"):
        key = (fld.get("name") or fld.get("ID") or "").strip()
        fields.append(key)

    artifacts: list[ArtifactInfo] = []
    trs = table.findall(".//v:TR", ns) + table.findall(".//v1:TR", ns) + table.findall(
        ".//v2:TR", ns
    ) + table.findall(".//TR")

    for tr in trs:
        values = [td.text.strip() if td.text else "" for td in list(tr)]
        row = {fields[i]: values[i] for i in range(min(len(fields), len(values)))}
        url = row.get("access_url") or row.get("accessURL")
        if not url:
            continue
        filename = _filename_from_url(url)
        semantics = row.get("semantics") or row.get("content_qualifier")
        description = row.get("description")
        kind = _classify_artifact("", semantics, filename)
        size = row.get("content_length")
        size_bytes = int(size) if size and size.isdigit() else None
        checksum = row.get("checksum")
        artifacts.append(
            ArtifactInfo(
                kind=kind,
                url=url,
                filename=filename,
                semantics=semantics,
                content_type=row.get("content_type"),
                size_bytes=size_bytes,
                checksum=checksum,
                description=description,
            )
        )
    return artifacts


def _read_manifest(manifest_path: Path, record: MousRecord) -> dict[str, Any]:
    payload = load_json(manifest_path, default=None)
    if payload:
        payload["mous_uid"] = payload.get("mous_uid") or record.member_ous_uid
        payload["project_code"] = payload.get("project_code") or record.project_code
        payload["group_ous_uid"] = payload.get("group_ous_uid") or record.group_ous_uid
        payload["science_goal_uid"] = payload.get("science_goal_uid") or record.science_goal_uid
        payload["release_date"] = payload.get("release_date") or record.release_date
        payload["obs_date"] = payload.get("obs_date") or record.obs_date
        payload["band_list"] = payload.get("band_list") or record.band_list
        payload["eb_uids"] = payload.get("eb_uids") or record.eb_uids
        payload["qa2_passed"] = (
            payload.get("qa2_passed")
            if payload.get("qa2_passed") is not None
            else record.qa2_passed
        )
        payload["qa0_status"] = payload.get("qa0_status") or record.qa0_status
        payload["qa0_reasons"] = payload.get("qa0_reasons") or record.qa0_reasons
        payload["qa2_reasons"] = payload.get("qa2_reasons") or record.qa2_reasons
        return payload
    return {
        "mous_uid": record.member_ous_uid,
        "project_code": record.project_code,
        "group_ous_uid": record.group_ous_uid,
        "science_goal_uid": record.science_goal_uid,
        "release_date": record.release_date,
        "obs_date": record.obs_date,
        "band_list": record.band_list,
        "eb_uids": record.eb_uids,
        "qa2_passed": record.qa2_passed,
        "qa0_status": record.qa0_status,
        "qa0_reasons": record.qa0_reasons,
        "qa2_reasons": record.qa2_reasons,
        "created_at": now_utc_iso(),
        "updated_at": now_utc_iso(),
        "artifacts": [],
        "history": [],
    }


def _save_manifest(manifest_path: Path, payload: dict[str, Any]) -> None:
    payload["updated_at"] = now_utc_iso()
    atomic_write_json(manifest_path, payload)


def _artifact_exists(loc: Path, expected_size: int | None) -> bool:
    if not loc.exists():
        return False
    if expected_size is None:
        return True
    return loc.stat().st_size == expected_size


def _download_one(
    session: requests.Session,
    artifact: ArtifactInfo,
    local_path: Path,
    *,
    timeout_sec: int,
    retry_count: int,
    rate_limit_sec: float,
) -> tuple[str, str | None, int | None]:
    ensure_dir(local_path.parent)
    temp_path = local_path.with_suffix(local_path.suffix + ".part")

    downloaded = temp_path.stat().st_size if temp_path.exists() else 0
    expected = artifact.size_bytes

    for attempt in range(1, retry_count + 1):
        downloaded = temp_path.stat().st_size if temp_path.exists() else 0
        headers = {}
        mode = "wb"
        if downloaded > 0:
            headers["Range"] = f"bytes={downloaded}-"
            mode = "ab"

        try:
            with session.get(
                artifact.url,
                stream=True,
                timeout=timeout_sec,
                headers=headers,
            ) as resp:
                if downloaded > 0 and resp.status_code == 200:
                    downloaded = 0
                    mode = "wb"
                resp.raise_for_status()
                with temp_path.open(mode) as handle:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            handle.write(chunk)
                final_size = temp_path.stat().st_size
                if expected is not None and final_size != expected:
                    raise IOError(
                        f"Size mismatch for {local_path.name}: expected {expected}, got {final_size}"
                    )
                os.replace(temp_path, local_path)
                if rate_limit_sec > 0:
                    time.sleep(rate_limit_sec)
                return "ok", None, local_path.stat().st_size
        except Exception as exc:  # noqa: BLE001
            if attempt == retry_count:
                return "error", str(exc), None
            time.sleep(min(2**attempt, 10))
    return "error", "retry exhausted", None


def _entry_lookup(manifest: dict[str, Any], filename: str) -> dict[str, Any] | None:
    for item in manifest.get("artifacts", []):
        if item.get("filename") == filename:
            return item
    return None


def download_for_record(
    *,
    record: MousRecord,
    delivered_dir: Path,
    manifest_path: Path,
    datalink_sync_url: str,
    timeout_sec: int,
    user_agent: str,
    artifacts_spec: str,
    max_workers: int,
    retry_count: int,
    rate_limit_sec: float,
    compute_sha256: bool,
) -> dict[str, Any]:
    manifest = _read_manifest(manifest_path, record)
    selected_kinds = resolve_artifact_selection(artifacts_spec)
    available = fetch_datalink_artifacts(
        datalink_sync_url=datalink_sync_url,
        member_ous_uid=record.member_ous_uid,
        timeout_sec=timeout_sec,
        user_agent=user_agent,
    )

    to_fetch: list[ArtifactInfo] = []
    for art in available:
        if not _artifact_selected(art.kind, selected_kinds):
            continue
        local_path = delivered_dir / art.filename
        existing = _entry_lookup(manifest, art.filename)
        if _artifact_exists(local_path, art.size_bytes) or _artifact_satisfied_without_archive_file(existing):
            if existing:
                existing["status"] = "present"
                existing["kind"] = _normalize_kind(art.kind)
                if local_path.exists():
                    existing["size_bytes"] = local_path.stat().st_size
                existing["updated_at"] = now_utc_iso()
            else:
                manifest["artifacts"].append(
                    {
                        "kind": _normalize_kind(art.kind),
                        "filename": art.filename,
                        "url": art.url,
                        "local_path": str(local_path),
                        "size_bytes": local_path.stat().st_size if local_path.exists() else None,
                        "checksum": None,
                        "status": "present",
                        "downloaded_at": now_utc_iso(),
                        "updated_at": now_utc_iso(),
                        "semantics": art.semantics,
                        "description": art.description,
                    }
                )
            continue
        to_fetch.append(art)

    if not to_fetch:
        manifest.setdefault("history", []).append(
            {
                "timestamp": now_utc_iso(),
                "event": "download",
                "message": "No missing artifacts for selected kinds",
                "selected_kinds": sorted(selected_kinds),
            }
        )
        _save_manifest(manifest_path, manifest)
        return manifest

    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    futures = {}
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
        for art in to_fetch:
            path = delivered_dir / art.filename
            fut = pool.submit(
                _download_one,
                session,
                art,
                path,
                timeout_sec=timeout_sec,
                retry_count=retry_count,
                rate_limit_sec=rate_limit_sec,
            )
            futures[fut] = (art, path)

        for fut in tqdm(as_completed(futures), total=len(futures), desc=f"{record.member_ous_uid}"):
            art, local_path = futures[fut]
            status, error, size_bytes = fut.result()
            checksum = sha256_file(local_path) if (status == "ok" and compute_sha256) else None
            existing = _entry_lookup(manifest, art.filename)
            payload = {
                "kind": _normalize_kind(art.kind),
                "filename": art.filename,
                "url": art.url,
                "local_path": str(local_path),
                "size_bytes": size_bytes,
                "checksum": checksum or art.checksum,
                "status": "present" if status == "ok" else "error",
                "downloaded_at": now_utc_iso() if status == "ok" else None,
                "updated_at": now_utc_iso(),
                "error": error,
                "semantics": art.semantics,
                "description": art.description,
                "archive_removed_after_unpack": False,
            }
            if existing:
                existing.update(payload)
            else:
                manifest["artifacts"].append(payload)

    manifest.setdefault("history", []).append(
        {
            "timestamp": now_utc_iso(),
            "event": "download",
            "selected_kinds": sorted(selected_kinds),
            "downloaded": len(to_fetch),
        }
    )
    _save_manifest(manifest_path, manifest)
    return manifest


def read_candidates_jsonl(path: Path) -> list[MousRecord]:
    out: list[MousRecord] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            out.append(
                MousRecord(
                    project_code=row["project_code"],
                    member_ous_uid=row["member_ous_uid"],
                    group_ous_uid=row.get("group_ous_uid"),
                    science_goal_uid=row.get("science_goal_uid"),
                    eb_uids=row.get("eb_uids", []),
                    band_list=row.get("band_list", []),
                    release_date=row.get("release_date"),
                    obs_date=row.get("obs_date"),
                    qa2_passed=row.get("qa2_passed"),
                    qa0_status=row.get("qa0_status"),
                    qa0_reasons=row.get("qa0_reasons", []),
                    qa2_reasons=row.get("qa2_reasons", []),
                    source_rows=row.get("source_rows", 0),
                    archive_meta=row.get("archive_meta", {}),
                )
            )
    return out


def write_candidates_jsonl(path: Path, rows: list[MousRecord], adql: str) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            payload = asdict(row)
            payload["query_adql"] = adql
            payload["query_timestamp"] = now_utc_iso()
            handle.write(json.dumps(payload) + "\n")
