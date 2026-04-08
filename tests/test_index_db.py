import sqlite3
from pathlib import Path

import pytest

from alma_bulk_tools.index_db import connect_db, init_db, upsert_mous_from_summary


def _minimal_summary(mous_uid: str = "uid://A001/X1/X1") -> dict:
    return {"mous_uid": mous_uid, "project_code": "2024.1.00001.S"}


def _minimal_manifest(mous_uid: str = "uid://A001/X1/X1") -> dict:
    return {"mous_uid": mous_uid, "artifacts": []}


# --- connect_db write mode ---


def test_connect_db_creates_file(tmp_path: Path) -> None:
    db = tmp_path / "test.sqlite"
    conn = connect_db(db)
    conn.close()
    assert db.exists()


def test_connect_db_creates_parent_dirs(tmp_path: Path) -> None:
    db = tmp_path / "sub" / "dir" / "test.sqlite"
    conn = connect_db(db)
    conn.close()
    assert db.exists()


def test_connect_db_write_and_read(tmp_path: Path) -> None:
    db = tmp_path / "test.sqlite"
    conn = connect_db(db)
    init_db(conn)
    upsert_mous_from_summary(
        conn,
        summary=_minimal_summary(),
        manifest=_minimal_manifest(),
        local_dir=str(tmp_path),
        shard_id=None,
    )
    conn.close()

    conn2 = connect_db(db)
    row = conn2.execute("SELECT mous_uid FROM mous").fetchone()
    conn2.close()
    assert row is not None
    assert row[0] == "uid://A001/X1/X1"


def test_connect_db_journal_mode_is_not_wal(tmp_path: Path) -> None:
    db = tmp_path / "test.sqlite"
    conn = connect_db(db)
    init_db(conn)
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    conn.close()
    assert mode != "wal"


def test_connect_db_multiple_open_close_cycles(tmp_path: Path) -> None:
    db = tmp_path / "test.sqlite"
    for i in range(3):
        conn = connect_db(db)
        init_db(conn)
        upsert_mous_from_summary(
            conn,
            summary=_minimal_summary(f"uid://A001/X1/X{i}"),
            manifest=_minimal_manifest(f"uid://A001/X1/X{i}"),
            local_dir=str(tmp_path),
            shard_id=None,
        )
        conn.close()

    conn = connect_db(db)
    count = conn.execute("SELECT COUNT(*) FROM mous").fetchone()[0]
    conn.close()
    assert count == 3


# --- connect_db readonly mode ---


def test_connect_db_readonly_reads_existing(tmp_path: Path) -> None:
    db = tmp_path / "test.sqlite"
    conn = connect_db(db)
    init_db(conn)
    upsert_mous_from_summary(
        conn,
        summary=_minimal_summary(),
        manifest=_minimal_manifest(),
        local_dir=str(tmp_path),
        shard_id=None,
    )
    conn.close()

    ro = connect_db(db, readonly=True)
    count = ro.execute("SELECT COUNT(*) FROM mous").fetchone()[0]
    ro.close()
    assert count == 1


def test_connect_db_readonly_raises_on_missing_file(tmp_path: Path) -> None:
    db = tmp_path / "nonexistent.sqlite"
    with pytest.raises(Exception):
        connect_db(db, readonly=True)


def test_connect_db_readonly_cannot_write(tmp_path: Path) -> None:
    db = tmp_path / "test.sqlite"
    conn = connect_db(db)
    init_db(conn)
    conn.close()

    ro = connect_db(db, readonly=True)
    with pytest.raises(sqlite3.OperationalError):
        ro.execute("INSERT INTO mous (mous_uid) VALUES ('x')")
    ro.close()
