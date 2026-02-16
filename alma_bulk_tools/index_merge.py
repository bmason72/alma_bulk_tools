from __future__ import annotations

import json
import logging
from pathlib import Path

from .index_db import connect_db, init_db, ingest_summary_file, integrity_check, merge_db
from .layout import MANIFEST_FILENAME, SUMMARY_FILENAME

LOGGER = logging.getLogger(__name__)


def merge_index_from_shards(
    *,
    dest_root: Path,
    shards_root: Path,
    central_db_path: Path,
    vacuum: bool,
    run_integrity_check: bool,
) -> dict[str, int | str]:
    merged_shard_dbs = 0
    merged_summary_files = 0

    conn = connect_db(central_db_path)
    init_db(conn)

    for db_path in sorted(shards_root.rglob("*.sqlite")):
        if db_path.name == central_db_path.name:
            continue
        try:
            source = connect_db(db_path)
            merge_db(source, conn)
            source.close()
            merged_shard_dbs += 1
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Skipping shard db %s: %s", db_path, exc)

    for summary_path in sorted(shards_root.rglob(SUMMARY_FILENAME)):
        manifest_path = summary_path.parent / MANIFEST_FILENAME
        shard_id = summary_path.parent.name
        try:
            ingest_summary_file(
                conn,
                summary_path=summary_path,
                manifest_path=manifest_path,
                shard_id=shard_id,
            )
            merged_summary_files += 1
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Skipping summary file %s: %s", summary_path, exc)

    if vacuum:
        conn.execute("VACUUM")

    integrity = "not-run"
    if run_integrity_check:
        integrity = integrity_check(conn)

    conn.commit()
    conn.close()

    return {
        "merged_shard_dbs": merged_shard_dbs,
        "merged_summary_files": merged_summary_files,
        "integrity": integrity,
    }
