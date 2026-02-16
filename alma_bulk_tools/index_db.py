from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

from .utils import now_utc_iso

LOGGER = logging.getLogger(__name__)


def _summary_mous(summary: dict[str, Any]) -> dict[str, Any]:
    block = summary.get("mous")
    return block if isinstance(block, dict) else {}


def _summary_qa(summary: dict[str, Any]) -> dict[str, Any]:
    block = summary.get("qa")
    return block if isinstance(block, dict) else {}


def _summary_runs(summary: dict[str, Any]) -> dict[str, Any]:
    block = summary.get("runs")
    return block if isinstance(block, dict) else {}


def _qa_status_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "PASS" if value else "FAIL"
    text = str(value).strip().upper()
    if not text:
        return None
    if text in {"TRUE", "T", "1"}:
        return "PASS"
    if text in {"FALSE", "F", "0"}:
        return "FAIL"
    if text in {"PASS", "SEMIPASS", "FAIL", "UNKNOWN"}:
        return text
    return text


def _sum_run_metric(summary: dict[str, Any], key: str) -> int:
    total = 0
    for run in _summary_runs(summary).values():
        if not isinstance(run, dict):
            continue
        total += int(run.get(key) or 0)
    return total


def _summary_has_qa_evidence(summary: dict[str, Any]) -> bool:
    runs = _summary_runs(summary)
    for run in runs.values():
        if not isinstance(run, dict):
            continue
        if run.get("pipeline_aquareport_files"):
            return True
    return False


def _summary_eb_uids(summary: dict[str, Any], manifest: dict[str, Any]) -> list[str]:
    mous = _summary_mous(summary)
    qa = _summary_qa(summary)
    eb_uids = summary.get("eb_uid_list") or mous.get("eb_uid_list") or manifest.get("eb_uids") or []
    out = {str(v) for v in eb_uids if str(v)}
    for item in qa.get("eb_in_asa", []) if isinstance(qa.get("eb_in_asa"), list) else []:
        if not isinstance(item, dict):
            continue
        eb_uid = item.get("eb_uid")
        if eb_uid:
            out.add(str(eb_uid))
    return sorted(out)


def db_path_for(dest: Path, shard_name: str | None = None) -> Path:
    if shard_name:
        return dest / f"alma_index.{shard_name}.sqlite"
    return dest / "alma_index.sqlite"


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS mous (
            mous_uid TEXT PRIMARY KEY,
            project_code TEXT,
            release_date TEXT,
            obs_date TEXT,
            band_json TEXT,
            qa2_status TEXT,
            qa0_status TEXT,
            qa2_reasons_json TEXT,
            qa0_reasons_json TEXT,
            dr_intervention_suspected INTEGER,
            dr_flag_commands_count INTEGER,
            dr_manual_flag_commands_count INTEGER,
            asa_qa_present INTEGER DEFAULT 0,
            local_dir TEXT,
            manifest_path TEXT,
            summary_path TEXT,
            discovered INTEGER DEFAULT 0,
            downloaded INTEGER DEFAULT 0,
            unpacked INTEGER DEFAULT 0,
            summarized INTEGER DEFAULT 0,
            indexed INTEGER DEFAULT 1,
            last_error_stage TEXT,
            last_error_message TEXT,
            shard_id TEXT,
            last_seen TEXT,
            last_updated TEXT
        );

        CREATE TABLE IF NOT EXISTS eb (
            mous_uid TEXT,
            eb_uid TEXT,
            PRIMARY KEY(mous_uid, eb_uid),
            FOREIGN KEY(mous_uid) REFERENCES mous(mous_uid) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS artifact (
            mous_uid TEXT,
            filename TEXT,
            kind TEXT,
            status TEXT,
            local_path TEXT,
            source_url TEXT,
            size_bytes INTEGER,
            checksum TEXT,
            updated_at TEXT,
            PRIMARY KEY(mous_uid, filename),
            FOREIGN KEY(mous_uid) REFERENCES mous(mous_uid) ON DELETE CASCADE
        );
        """
    )
    conn.commit()


def upsert_mous_from_summary(
    conn: sqlite3.Connection,
    *,
    summary: dict[str, Any],
    manifest: dict[str, Any],
    local_dir: str,
    shard_id: str | None,
    last_error_stage: str | None = None,
    last_error_message: str | None = None,
) -> None:
    mous = _summary_mous(summary)
    qa = _summary_qa(summary)
    mous_uid = summary.get("mous_uid") or mous.get("mous_uid") or manifest.get("mous_uid")
    if not mous_uid:
        raise ValueError("Missing mous_uid for upsert")

    release_date = (
        summary.get("public_release_date") or mous.get("public_release_date") or manifest.get("release_date")
    )
    obs_date = summary.get("obs_date") or mous.get("obs_date") or manifest.get("obs_date")
    band_json = json.dumps(summary.get("band") or mous.get("band") or manifest.get("band_list") or [])
    qa2_status = _qa_status_text(summary.get("qa2_status") or qa.get("qa2_status"))
    if qa2_status is None:
        qa2_status = _qa_status_text(manifest.get("qa2_status"))
    if qa2_status is None:
        qa2_status = _qa_status_text(manifest.get("qa2_passed"))
    qa0_status = _qa_status_text(summary.get("qa0_status"))
    qa2_reasons = json.dumps(summary.get("qa2_flag_reasons") or qa.get("qa2_reasons") or [])
    qa0_reasons = json.dumps(summary.get("qa0_flag_reasons") or [])

    dr_block = summary.get("dr") if isinstance(summary.get("dr"), dict) else {}
    dr_intervention = 1 if (summary.get("dr_intervention_suspected") or dr_block.get("dr_intervention_suspected")) else 0
    dr_flags = int(summary.get("dr_flag_commands_count") or dr_block.get("dr_flag_commands_count") or _sum_run_metric(summary, "dr_flag_commands_count"))
    dr_manual = int(
        summary.get("dr_manual_flag_commands_count")
        or dr_block.get("dr_manual_flag_commands_count")
        or _sum_run_metric(summary, "dr_manual_flag_commands_count")
    )
    asa_qa_present = 1 if (summary.get("asa_qa_present") or _summary_has_qa_evidence(summary)) else 0

    artifacts = manifest.get("artifacts", [])
    downloaded = 1 if any(a.get("status") == "present" for a in artifacts) else 0
    unpacked = 1 if manifest.get("unpacked") else 0
    summarized = 1 if summary else 0
    discovered = 1
    now = now_utc_iso()

    if not last_error_stage or not last_error_message:
        for art in artifacts:
            if art.get("status") == "error":
                last_error_stage = "download"
                last_error_message = art.get("error") or "artifact download failed"
                break

    conn.execute(
        """
        INSERT INTO mous (
            mous_uid, project_code, release_date, obs_date, band_json,
            qa2_status, qa0_status, qa2_reasons_json, qa0_reasons_json,
            dr_intervention_suspected, dr_flag_commands_count, dr_manual_flag_commands_count, asa_qa_present,
            local_dir, manifest_path, summary_path,
            discovered, downloaded, unpacked, summarized, indexed,
            last_error_stage, last_error_message, shard_id, last_seen, last_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mous_uid) DO UPDATE SET
            project_code=excluded.project_code,
            release_date=excluded.release_date,
            obs_date=excluded.obs_date,
            band_json=excluded.band_json,
            qa2_status=excluded.qa2_status,
            qa0_status=excluded.qa0_status,
            qa2_reasons_json=excluded.qa2_reasons_json,
            qa0_reasons_json=excluded.qa0_reasons_json,
            dr_intervention_suspected=excluded.dr_intervention_suspected,
            dr_flag_commands_count=excluded.dr_flag_commands_count,
            dr_manual_flag_commands_count=excluded.dr_manual_flag_commands_count,
            asa_qa_present=excluded.asa_qa_present,
            local_dir=excluded.local_dir,
            manifest_path=excluded.manifest_path,
            summary_path=excluded.summary_path,
            discovered=excluded.discovered,
            downloaded=excluded.downloaded,
            unpacked=excluded.unpacked,
            summarized=excluded.summarized,
            indexed=excluded.indexed,
            last_error_stage=excluded.last_error_stage,
            last_error_message=excluded.last_error_message,
            shard_id=excluded.shard_id,
            last_seen=excluded.last_seen,
            last_updated=excluded.last_updated
        """,
        (
            mous_uid,
            summary.get("project_code") or mous.get("project_code") or manifest.get("project_code"),
            release_date,
            obs_date,
            band_json,
            qa2_status,
            qa0_status,
            qa2_reasons,
            qa0_reasons,
            dr_intervention,
            dr_flags,
            dr_manual,
            asa_qa_present,
            local_dir,
            manifest.get("manifest_path"),
            summary.get("summary_path"),
            discovered,
            downloaded,
            unpacked,
            summarized,
            1,
            last_error_stage,
            last_error_message,
            shard_id,
            now,
            now,
        ),
    )

    eb_uids = _summary_eb_uids(summary, manifest)
    for eb in eb_uids:
        conn.execute(
            "INSERT OR REPLACE INTO eb (mous_uid, eb_uid) VALUES (?, ?)",
            (mous_uid, eb),
        )

    for art in artifacts:
        conn.execute(
            """
            INSERT INTO artifact (mous_uid, filename, kind, status, local_path, source_url, size_bytes, checksum, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mous_uid, filename) DO UPDATE SET
                kind=excluded.kind,
                status=excluded.status,
                local_path=excluded.local_path,
                source_url=excluded.source_url,
                size_bytes=excluded.size_bytes,
                checksum=excluded.checksum,
                updated_at=excluded.updated_at
            """,
            (
                mous_uid,
                art.get("filename"),
                art.get("kind"),
                art.get("status"),
                art.get("local_path"),
                art.get("url"),
                art.get("size_bytes"),
                art.get("checksum"),
                now,
            ),
        )
    conn.commit()


def ingest_summary_file(
    conn: sqlite3.Connection,
    *,
    summary_path: Path,
    manifest_path: Path,
    shard_id: str | None,
) -> None:
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    summary.setdefault("summary_path", str(summary_path))
    manifest.setdefault("manifest_path", str(manifest_path))
    if not (summary.get("mous_uid") or manifest.get("mous_uid")):
        raise ValueError(f"Missing mous_uid in {summary_path} and {manifest_path}")
    upsert_mous_from_summary(
        conn,
        summary=summary,
        manifest=manifest,
        local_dir=str(summary_path.parent),
        shard_id=shard_id,
    )


def merge_db(source: sqlite3.Connection, target: sqlite3.Connection) -> None:
    init_db(target)
    cur = source.execute("SELECT * FROM mous")
    mous_rows = cur.fetchall()
    mous_cols = [d[0] for d in cur.description]

    for row in mous_rows:
        item = dict(zip(mous_cols, row))
        target.execute(
            """
            INSERT INTO mous (
                mous_uid, project_code, release_date, obs_date, band_json,
                qa2_status, qa0_status, qa2_reasons_json, qa0_reasons_json,
                dr_intervention_suspected, dr_flag_commands_count, dr_manual_flag_commands_count, asa_qa_present,
                local_dir, manifest_path, summary_path,
                discovered, downloaded, unpacked, summarized, indexed,
                last_error_stage, last_error_message, shard_id, last_seen, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mous_uid) DO UPDATE SET
                project_code=excluded.project_code,
                release_date=excluded.release_date,
                obs_date=excluded.obs_date,
                band_json=excluded.band_json,
                qa2_status=excluded.qa2_status,
                qa0_status=excluded.qa0_status,
                qa2_reasons_json=excluded.qa2_reasons_json,
                qa0_reasons_json=excluded.qa0_reasons_json,
                dr_intervention_suspected=excluded.dr_intervention_suspected,
                dr_flag_commands_count=excluded.dr_flag_commands_count,
                dr_manual_flag_commands_count=excluded.dr_manual_flag_commands_count,
                asa_qa_present=excluded.asa_qa_present,
                local_dir=excluded.local_dir,
                manifest_path=excluded.manifest_path,
                summary_path=excluded.summary_path,
                discovered=excluded.discovered,
                downloaded=excluded.downloaded,
                unpacked=excluded.unpacked,
                summarized=excluded.summarized,
                indexed=excluded.indexed,
                last_error_stage=excluded.last_error_stage,
                last_error_message=excluded.last_error_message,
                shard_id=excluded.shard_id,
                last_seen=excluded.last_seen,
                last_updated=excluded.last_updated
            """,
            tuple(item.get(k) for k in mous_cols),
        )

    for table in ("eb", "artifact"):
        cur = source.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        placeholders = ",".join("?" for _ in cols)
        target.executemany(
            f"INSERT OR REPLACE INTO {table} ({','.join(cols)}) VALUES ({placeholders})",
            rows,
        )
    target.commit()


def integrity_check(conn: sqlite3.Connection) -> str:
    row = conn.execute("PRAGMA integrity_check").fetchone()
    return str(row[0]) if row else "unknown"
