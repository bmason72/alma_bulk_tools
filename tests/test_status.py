import sqlite3

from alma_bulk_tools.index_db import init_db
from alma_bulk_tools.status import build_status_report


def test_missing_qa_uses_summary_qa_signal() -> None:
    conn = sqlite3.connect(":memory:")
    init_db(conn)

    conn.execute(
        """
        INSERT INTO mous (
            mous_uid, discovered, downloaded, unpacked, summarized, indexed, asa_qa_present
        ) VALUES
            ('uid://A/B/C1', 1, 1, 1, 1, 1, 0),
            ('uid://A/B/C2', 1, 1, 1, 1, 1, 1),
            ('uid://A/B/C3', 1, 1, 1, 0, 1, 0)
        """
    )
    conn.commit()

    report = build_status_report(conn)
    conn.close()

    assert report["todo"]["missing_qa"] == 1
