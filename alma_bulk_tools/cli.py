from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from . import __version__
from .archive_query import discover_mous
from .config import apply_cli_overrides, load_config
from .downloader import download_for_record, read_candidates_jsonl, write_candidates_jsonl
from .index_db import connect_db, db_path_for, init_db, ingest_summary_file, upsert_mous_from_summary
from .index_merge import merge_index_from_shards
from .layout import (
    MANIFEST_FILENAME,
    SUMMARY_FILENAME,
    ensure_layout_for_existing_mous,
    ensure_mous_layout,
    find_mous_dirs,
)
from .models import MousRecord
from .status import build_status_report, format_status_report
from .summarize import summarize_mous
from .unpack import unpack_mous_delivered
from .utils import ensure_dir, load_json, now_utc_iso, setup_logging

LOGGER = logging.getLogger(__name__)


def _parse_bands_override(value: str | None) -> tuple[list[str], list[str]]:
    if not value:
        return [], []
    value = value.strip()
    if value.startswith("exclude:"):
        return [], [v.strip() for v in value.split(":", 1)[1].split(",") if v.strip()]
    if value.startswith("include:"):
        return [v.strip() for v in value.split(":", 1)[1].split(",") if v.strip()], []
    return [v.strip() for v in value.split(",") if v.strip()], []


def _load_records_from_existing(dest: Path) -> list[MousRecord]:
    records: list[MousRecord] = []
    for mous_dir in find_mous_dirs(dest):
        manifest = load_json(mous_dir / MANIFEST_FILENAME, default={}) or {}
        if not manifest.get("mous_uid"):
            continue
        records.append(
            MousRecord(
                project_code=manifest.get("project_code", "UNKNOWN"),
                member_ous_uid=manifest.get("mous_uid"),
                group_ous_uid=manifest.get("group_ous_uid"),
                science_goal_uid=manifest.get("science_goal_uid"),
                eb_uids=manifest.get("eb_uids", []),
                band_list=manifest.get("band_list", []),
                release_date=manifest.get("release_date"),
                obs_date=manifest.get("obs_date"),
                qa2_passed=manifest.get("qa2_passed"),
                qa0_status=manifest.get("qa0_status"),
                qa0_reasons=manifest.get("qa0_reasons", []),
                qa2_reasons=manifest.get("qa2_reasons", []),
            )
        )
    records.sort(key=lambda r: r.member_ous_uid)
    return records


def _artifact_spec_from_cfg(cfg: dict[str, Any]) -> str:
    dl = cfg.get("download", {})
    explicit = dl.get("artifacts")
    if explicit:
        return str(explicit)

    selected: list[str] = []
    for key, enabled in (dl.get("deliverables") or {}).items():
        if enabled:
            selected.append(str(key))
    for key, enabled in (dl.get("products") or {}).items():
        if enabled:
            selected.append(str(key))
    return ",".join(selected) if selected else "default"


def _resolve_dest(args: argparse.Namespace, cfg: dict[str, Any]) -> Path:
    cli_dest = getattr(args, "dest", None)
    if cli_dest:
        return Path(cli_dest)
    cfg_dest = (cfg.get("paths") or {}).get("dest")
    if cfg_dest:
        return Path(str(cfg_dest))
    raise ValueError("Destination is required: pass --dest or set paths.dest in config")


def _write_plan_file(shard_files: list[Path], out_dir: Path, total_records: int, shard_size: int) -> None:
    payload = {
        "created_at": now_utc_iso(),
        "total_records": total_records,
        "shard_size": shard_size,
        "shards": [str(p) for p in shard_files],
    }
    (out_dir / "plan.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _maybe_stop_for_runtime(started: datetime, max_runtime_min: int | None) -> bool:
    if not max_runtime_min:
        return False
    return datetime.utcnow() >= started + timedelta(minutes=max_runtime_min)


def _create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="alma-bulk")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="cmd", required=True)

    discover = sub.add_parser("discover", help="Discover public MOUS candidates via ALMA TAP")
    discover.add_argument("--config", type=Path)
    discover.add_argument("--start", required=True)
    discover.add_argument("--end", required=True)
    discover.add_argument("--date-field", choices=["release", "observation"], default=None)
    discover.add_argument("--exclude-tp", action="store_true")
    discover.add_argument("--exclude-7m", action="store_true")
    discover.add_argument("--bands", help="include:3,4 or exclude:9,10")
    discover.add_argument("--project-code-include")
    discover.add_argument("--project-code-exclude")
    discover.add_argument("--out", type=Path, required=True)

    download = sub.add_parser("download", help="Download selected archive deliverables")
    download.add_argument("--config", type=Path)
    download.add_argument("--input", type=Path)
    download.add_argument("--dest", type=Path)
    download.add_argument("--artifacts", default=None)
    download.add_argument("--max-workers", type=int, default=None)
    download.add_argument("--max-runtime-min", type=int, default=None)

    unpack = sub.add_parser("unpack", help="Unpack downloaded archive bundles")
    unpack.add_argument("--config", type=Path)
    unpack.add_argument("--dest", type=Path)
    unpack.add_argument("--max-runtime-min", type=int, default=None)

    summarize = sub.add_parser("summarize", help="Generate per-MOUS summaries")
    summarize.add_argument("--config", type=Path)
    summarize.add_argument("--dest", type=Path)
    summarize.add_argument("--write-markdown", action="store_true")
    summarize.add_argument("--shard-id")
    summarize.add_argument("--index-db", type=Path)
    summarize.add_argument("--max-runtime-min", type=int, default=None)

    scan = sub.add_parser("scan", help="Scan existing trees and index manifests/summaries")
    scan.add_argument("--config", type=Path)
    scan.add_argument("--dest", type=Path)
    scan.add_argument("--fix-layout", action="store_true")
    scan.add_argument("--rebuild-db", action="store_true")
    scan.add_argument("--index-db", type=Path)

    plan = sub.add_parser("plan", help="Create shard files for batch processing")
    plan.add_argument("--config", type=Path)
    plan.add_argument("--input", type=Path, required=True)
    plan.add_argument("--out", type=Path, required=True)
    plan.add_argument("--shard-size", type=int, default=200)

    run_shard = sub.add_parser("run-shard", help="Process one shard (download optional, unpack, summarize, shard index)")
    run_shard.add_argument("--config", type=Path)
    run_shard.add_argument("--dest", type=Path)
    run_shard.add_argument("--shard", type=Path, required=True)
    run_shard.add_argument("--download-missing", action="store_true")
    run_shard.add_argument("--max-workers", type=int, default=None)
    run_shard.add_argument("--max-runtime-min", type=int, default=None)

    merge = sub.add_parser("merge-index", help="Merge shard outputs into central SQLite")
    merge.add_argument("--config", type=Path)
    merge.add_argument("--dest", type=Path)
    merge.add_argument("--shards", type=Path, required=True)
    merge.add_argument("--vacuum", action="store_true")
    merge.add_argument("--integrity-check", action="store_true")

    status = sub.add_parser("status", help="Print index progress/failure dashboard")
    status.add_argument("--config", type=Path)
    status.add_argument("--dest", type=Path)
    status.add_argument("--top-n-errors", type=int, default=10)

    return parser


def _command_discover(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    include_bands, exclude_bands = _parse_bands_override(args.bands)
    cfg = apply_cli_overrides(
        cfg,
        {
            "filters": {
                "exclude_tp": args.exclude_tp or cfg["filters"].get("exclude_tp", False),
                "exclude_7m": args.exclude_7m or cfg["filters"].get("exclude_7m", False),
                "bands_include": include_bands or cfg["filters"].get("bands_include", []),
                "bands_exclude": exclude_bands or cfg["filters"].get("bands_exclude", []),
                "project_codes_include": args.project_code_include or cfg["filters"].get("project_codes_include", []),
                "project_codes_exclude": args.project_code_exclude or cfg["filters"].get("project_codes_exclude", []),
                "date_field": args.date_field or cfg["filters"].get("date_field", "release"),
            }
        },
    )

    records, adql = discover_mous(
        tap_sync_url=cfg["archive"]["tap_sync_url"],
        timeout_sec=int(cfg["archive"]["timeout_sec"]),
        user_agent=cfg["archive"]["user_agent"],
        start=args.start,
        end=args.end,
        date_field=cfg["filters"]["date_field"],
        filters=cfg["filters"],
    )

    write_candidates_jsonl(args.out, records, adql)
    print(f"Wrote {len(records)} candidates to {args.out}")
    return 0


def _upsert_from_manifest_only(conn, manifest: dict[str, Any], mous_dir: Path, shard_id: str | None = None) -> None:
    summary = {
        "mous_uid": manifest.get("mous_uid"),
        "project_code": manifest.get("project_code"),
        "public_release_date": manifest.get("release_date"),
        "obs_date": manifest.get("obs_date"),
        "band": manifest.get("band_list", []),
        "qa2_status": manifest.get("qa2_passed"),
        "qa0_status": manifest.get("qa0_status"),
        "qa2_flag_reasons": manifest.get("qa2_reasons", []),
        "qa0_flag_reasons": manifest.get("qa0_reasons", []),
        "dr_intervention_suspected": False,
        "dr_flag_commands_count": 0,
        "dr_manual_flag_commands_count": 0,
        "summary_path": str(mous_dir / SUMMARY_FILENAME),
    }
    manifest = dict(manifest)
    manifest["manifest_path"] = str(mous_dir / MANIFEST_FILENAME)
    if not summary.get("mous_uid"):
        return
    upsert_mous_from_summary(
        conn,
        summary=summary,
        manifest=manifest,
        local_dir=str(mous_dir),
        shard_id=shard_id,
    )


def _command_download(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    dest = _resolve_dest(args, cfg)
    cfg = apply_cli_overrides(
        cfg,
        {
            "download": {
                "artifacts": args.artifacts,
                "max_workers": args.max_workers,
            },
            "runtime": {
                "max_runtime_min": args.max_runtime_min,
            },
        },
    )

    if args.input:
        records = read_candidates_jsonl(args.input)
    else:
        records = _load_records_from_existing(dest)
    if not records:
        print("No MOUS records found to download")
        return 0

    started = datetime.utcnow()
    dbp = db_path_for(dest)
    conn = connect_db(dbp)
    init_db(conn)
    artifact_spec = _artifact_spec_from_cfg(cfg)

    processed = 0
    for record in records:
        layout = ensure_mous_layout(dest, record)
        manifest = download_for_record(
            record=record,
            delivered_dir=layout["delivered"],
            manifest_path=layout["manifest"],
            datalink_sync_url=cfg["archive"]["datalink_sync_url"],
            timeout_sec=int(cfg["archive"]["timeout_sec"]),
            user_agent=cfg["archive"]["user_agent"],
            artifacts_spec=artifact_spec,
            max_workers=int(cfg["download"]["max_workers"]),
            retry_count=int(cfg["download"].get("retry_count", 3)),
            rate_limit_sec=float(cfg["download"].get("rate_limit_sec", 0.0)),
            compute_sha256=bool(cfg["download"].get("compute_sha256", False)),
        )
        _upsert_from_manifest_only(conn, manifest, layout["mous_dir"])
        processed += 1

        if _maybe_stop_for_runtime(started, cfg["runtime"].get("max_runtime_min")):
            LOGGER.info("Stopping due to max runtime after %s MOUS", processed)
            break

    conn.close()
    print(f"Download stage completed for {processed} MOUS")
    return 0


def _command_unpack(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    dest = _resolve_dest(args, cfg)
    cfg = apply_cli_overrides(cfg, {"runtime": {"max_runtime_min": args.max_runtime_min}})

    mous_dirs = find_mous_dirs(dest)
    started = datetime.utcnow()
    conn = connect_db(db_path_for(dest))
    init_db(conn)
    done = 0
    for mous_dir in mous_dirs:
        manifest_path = mous_dir / MANIFEST_FILENAME
        if not manifest_path.exists():
            continue
        manifest = unpack_mous_delivered(
            mous_dir / "delivered",
            manifest_path,
            unpack_auxiliary=bool(cfg.get("unpack", {}).get("unpack_auxiliary", True)),
            unpack_readme_archives=bool(
                cfg.get("unpack", {}).get("unpack_readme_archives", True)
            ),
            unpack_weblog_archives=bool(
                cfg.get("unpack", {}).get("unpack_weblog_archives", True)
            ),
            unpack_other_archives=bool(cfg.get("unpack", {}).get("unpack_other_archives", False)),
            remove_archives_after_unpack=bool(
                cfg.get("unpack", {}).get("remove_archives_after_unpack", True)
            ),
            recursive_unpack_enabled=bool(
                cfg.get("unpack", {}).get("recursive_unpack_enabled", True)
            ),
            recursive_unpack_patterns=list(
                cfg.get("unpack", {}).get("recursive_unpack_patterns", [])
            ),
            recursive_unpack_max_passes=int(
                cfg.get("unpack", {}).get("recursive_unpack_max_passes", 3)
            ),
        )
        _upsert_from_manifest_only(conn, manifest, mous_dir)
        done += 1
        if _maybe_stop_for_runtime(started, cfg["runtime"].get("max_runtime_min")):
            LOGGER.info("Stopping due to max runtime after %s MOUS", done)
            break
    conn.close()
    print(f"Unpack stage completed for {done} MOUS")
    return 0


def _command_summarize(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    dest = _resolve_dest(args, cfg)
    cfg = apply_cli_overrides(cfg, {"runtime": {"max_runtime_min": args.max_runtime_min}})

    started = datetime.utcnow()
    shard_id = args.shard_id
    index_db_path = args.index_db or db_path_for(dest)
    conn = connect_db(index_db_path)
    init_db(conn)

    done = 0
    for mous_dir in find_mous_dirs(dest):
        manifest_path = mous_dir / MANIFEST_FILENAME
        if not manifest_path.exists():
            continue
        summary = summarize_mous(
            mous_dir=mous_dir,
            manifest_path=manifest_path,
            query_timestamp=now_utc_iso(),
            tool_version=__version__,
            write_markdown=bool(args.write_markdown),
        )
        manifest = load_json(manifest_path, default={}) or {}
        summary["summary_path"] = str(mous_dir / SUMMARY_FILENAME)
        manifest["manifest_path"] = str(manifest_path)
        upsert_mous_from_summary(
            conn,
            summary=summary,
            manifest=manifest,
            local_dir=str(mous_dir),
            shard_id=shard_id,
        )
        done += 1
        if _maybe_stop_for_runtime(started, cfg["runtime"].get("max_runtime_min")):
            LOGGER.info("Stopping due to max runtime after %s MOUS", done)
            break

    conn.close()
    print(f"Summarize stage completed for {done} MOUS")
    return 0


def _command_scan(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    dest = _resolve_dest(args, cfg)
    dbp = args.index_db or db_path_for(dest)
    if args.rebuild_db and dbp.exists():
        dbp.unlink()

    conn = connect_db(dbp)
    init_db(conn)

    mous_dirs = find_mous_dirs(dest)
    ensure_layout_for_existing_mous(mous_dirs, args.fix_layout)

    count = 0
    for mous_dir in mous_dirs:
        summary_path = mous_dir / SUMMARY_FILENAME
        manifest_path = mous_dir / MANIFEST_FILENAME
        if summary_path.exists() or manifest_path.exists():
            try:
                ingest_summary_file(
                    conn,
                    summary_path=summary_path,
                    manifest_path=manifest_path,
                    shard_id=None,
                )
                count += 1
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Skipping %s due to ingest error: %s", mous_dir, exc)

    conn.close()
    print(f"Scanned and indexed {count} MOUS directories")
    return 0


def _command_plan(args: argparse.Namespace) -> int:
    records = read_candidates_jsonl(args.input)
    if not records:
        print("No records to shard")
        return 0
    out_dir = ensure_dir(args.out)
    records = sorted(records, key=lambda r: r.member_ous_uid)

    shard_files: list[Path] = []
    shard_size = max(1, args.shard_size)
    for idx in range(0, len(records), shard_size):
        chunk = records[idx : idx + shard_size]
        shard_idx = idx // shard_size
        shard_path = out_dir / f"part-{shard_idx:04d}.jsonl"
        with shard_path.open("w", encoding="utf-8") as handle:
            for rec in chunk:
                handle.write(json.dumps(asdict(rec)) + "\n")
        shard_files.append(shard_path)

    _write_plan_file(shard_files, out_dir, len(records), shard_size)
    print(f"Wrote {len(shard_files)} shard files to {out_dir}")
    return 0


def _command_run_shard(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    dest = _resolve_dest(args, cfg)
    cfg = apply_cli_overrides(
        cfg,
        {
            "download": {"max_workers": args.max_workers},
            "runtime": {"max_runtime_min": args.max_runtime_min},
        },
    )

    records = read_candidates_jsonl(args.shard)
    if not records:
        print(f"Shard is empty: {args.shard}")
        return 0

    shard_id = args.shard.stem
    shard_db_path = args.shard.with_suffix(".sqlite")
    conn = connect_db(shard_db_path)
    init_db(conn)

    started = datetime.utcnow()
    artifact_spec = _artifact_spec_from_cfg(cfg)
    done = 0
    for record in records:
        layout = ensure_mous_layout(dest, record)

        if args.download_missing:
            manifest = download_for_record(
                record=record,
                delivered_dir=layout["delivered"],
                manifest_path=layout["manifest"],
                datalink_sync_url=cfg["archive"]["datalink_sync_url"],
                timeout_sec=int(cfg["archive"]["timeout_sec"]),
                user_agent=cfg["archive"]["user_agent"],
                artifacts_spec=artifact_spec,
                max_workers=int(cfg["download"]["max_workers"]),
                retry_count=int(cfg["download"].get("retry_count", 3)),
                rate_limit_sec=float(cfg["download"].get("rate_limit_sec", 0.0)),
                compute_sha256=bool(cfg["download"].get("compute_sha256", False)),
            )
        else:
            manifest = load_json(layout["manifest"], default={}) or {}

        if not layout["manifest"].exists():
            manifest = {
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
                "artifacts": [],
                "history": [],
            }
            layout["manifest"].write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

        if layout["manifest"].exists():
            unpack_mous_delivered(
                layout["delivered"],
                layout["manifest"],
                unpack_auxiliary=bool(cfg.get("unpack", {}).get("unpack_auxiliary", True)),
                unpack_readme_archives=bool(
                    cfg.get("unpack", {}).get("unpack_readme_archives", True)
                ),
                unpack_weblog_archives=bool(
                    cfg.get("unpack", {}).get("unpack_weblog_archives", True)
                ),
                unpack_other_archives=bool(cfg.get("unpack", {}).get("unpack_other_archives", False)),
                remove_archives_after_unpack=bool(
                    cfg.get("unpack", {}).get("remove_archives_after_unpack", True)
                ),
                recursive_unpack_enabled=bool(
                    cfg.get("unpack", {}).get("recursive_unpack_enabled", True)
                ),
                recursive_unpack_patterns=list(
                    cfg.get("unpack", {}).get("recursive_unpack_patterns", [])
                ),
                recursive_unpack_max_passes=int(
                    cfg.get("unpack", {}).get("recursive_unpack_max_passes", 3)
                ),
            )

        summary = summarize_mous(
            mous_dir=layout["mous_dir"],
            manifest_path=layout["manifest"],
            query_timestamp=now_utc_iso(),
            tool_version=__version__,
            write_markdown=False,
        )
        summary["summary_path"] = str(layout["summary"])
        manifest = load_json(layout["manifest"], default=manifest) or {}
        manifest["manifest_path"] = str(layout["manifest"])

        upsert_mous_from_summary(
            conn,
            summary=summary,
            manifest=manifest,
            local_dir=str(layout["mous_dir"]),
            shard_id=shard_id,
        )
        done += 1
        if _maybe_stop_for_runtime(started, cfg["runtime"].get("max_runtime_min")):
            LOGGER.info("Stopping due to max runtime after %s MOUS", done)
            break

    conn.close()
    print(f"Processed {done} MOUS from shard {args.shard} into {shard_db_path}")
    return 0


def _command_merge_index(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    dest = _resolve_dest(args, cfg)
    central_db = db_path_for(dest)
    result = merge_index_from_shards(
        dest_root=dest,
        shards_root=args.shards,
        central_db_path=central_db,
        vacuum=args.vacuum,
        run_integrity_check=args.integrity_check,
    )
    print(
        "Merged shard_dbs={merged_shard_dbs} summary_files={merged_summary_files} integrity={integrity}".format(
            **result
        )
    )
    return 0


def _command_status(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    dest = _resolve_dest(args, cfg)
    dbp = db_path_for(dest)
    if not dbp.exists():
        print(f"Index DB not found: {dbp}")
        return 1
    conn = connect_db(dbp)
    report = build_status_report(conn, top_n_errors=args.top_n_errors)
    conn.close()
    print(format_status_report(report), end="")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _create_parser()
    args = parser.parse_args(argv)

    cfg = load_config(getattr(args, "config", None))
    setup_logging(cfg.get("runtime", {}).get("log_level", "INFO"))

    try:
        if args.cmd == "discover":
            return _command_discover(args)
        if args.cmd == "download":
            return _command_download(args)
        if args.cmd == "unpack":
            return _command_unpack(args)
        if args.cmd == "summarize":
            return _command_summarize(args)
        if args.cmd == "scan":
            return _command_scan(args)
        if args.cmd == "plan":
            return _command_plan(args)
        if args.cmd == "run-shard":
            return _command_run_shard(args)
        if args.cmd == "merge-index":
            return _command_merge_index(args)
        if args.cmd == "status":
            return _command_status(args)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    parser.error(f"Unhandled command: {args.cmd}")
    return 2


def _single_command_main(cmd: str) -> int:
    return main([cmd, *sys.argv[1:]])


def main_discover() -> int:
    return _single_command_main("discover")


def main_download() -> int:
    return _single_command_main("download")


def main_unpack() -> int:
    return _single_command_main("unpack")


def main_summarize() -> int:
    return _single_command_main("summarize")


def main_scan() -> int:
    return _single_command_main("scan")


def main_merge_index() -> int:
    return _single_command_main("merge-index")


def main_status() -> int:
    return _single_command_main("status")


if __name__ == "__main__":
    raise SystemExit(main())
