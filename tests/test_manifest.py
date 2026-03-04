from pathlib import Path

from alma_bulk_tools.downloader import (
    datalink_id_from_member_ous_uid,
    read_candidates_jsonl,
    resolve_artifact_selection,
    write_candidates_jsonl,
)
from alma_bulk_tools.models import MousRecord


def test_default_artifact_selection_uses_archive_aligned_names() -> None:
    selected = resolve_artifact_selection("default")
    assert {
        "calibration",
        "scripts",
        "weblog",
        "qa_reports",
        "auxiliary",
        "readme",
        "calibration_products",
    }.issubset(selected)
    assert "raw" not in selected
    assert "continuum_images" not in selected
    assert "cubes" not in selected


def test_incremental_artifact_selection_addition() -> None:
    selected = resolve_artifact_selection("default,+raw,-weblog")
    assert "raw" in selected
    assert "weblog" not in selected


def test_all_nonimage_excludes_image_products() -> None:
    selected = resolve_artifact_selection("all-nonimage")
    assert "continuum_images" not in selected
    assert "cubes" not in selected
    assert "calibration" in selected
    assert "auxiliary" in selected
    assert "readme" in selected


def test_datalink_id_normalization_from_member_ous_uid() -> None:
    assert (
        datalink_id_from_member_ous_uid("uid://A001/X2f6/X2b")
        == "uid___A001_X2f6_X2b"
    )
    assert datalink_id_from_member_ous_uid("uid___A001_X2f6_X2b") == "uid___A001_X2f6_X2b"


def test_editable_text_candidate_file_can_feed_download(tmp_path: Path) -> None:
    out_path = tmp_path / "candidates.txt"
    rows = [
        MousRecord(
            project_code="2024.1.00001.S",
            member_ous_uid="uid://A001/X1/X2",
            group_ous_uid="uid://A001/X1/X3",
            eb_uids=["uid://A002/X100/X200", "uid://A002/X101/X201"],
            band_list=["BAND 6"],
            release_date="2024-03-11",
            obs_date="2024-01-02",
            qa2_passed=True,
            archive_meta={
                "sb_name": "SB_A",
                "science_category": "Galaxy evolution",
                "execution_count": 2,
                "spw_count": 4,
                "min_spw_total_width_mhz": 937.5,
                "max_spw_total_width_mhz": 2000.0,
                "min_nchan": 960,
                "max_nchan": 3840,
                "min_spw_width_nchan": 960,
                "max_spw_width_nchan": 3840,
                "array": "12m+7m",
                "max_baseline_m": 1850.0,
                "science_target_count": 2,
                "is_mosaic": True,
                "qa2_status": "PASS",
            },
        )
    ]

    write_candidates_jsonl(out_path, rows, "SELECT ...")
    text = out_path.read_text(encoding="utf-8")
    assert text.startswith("# alma-bulk discover candidates\n")
    assert "project_code,science_category,mous_uid" in text
    assert "937.5 MHz,2 GHz" in text
    assert "Galaxy evolution" in text
    loaded = read_candidates_jsonl(out_path)

    assert len(loaded) == 1
    assert loaded[0].member_ous_uid == "uid://A001/X1/X2"
    assert loaded[0].project_code == "2024.1.00001.S"
    assert loaded[0].group_ous_uid == "uid://A001/X1/X3"
    assert loaded[0].eb_uids == ["uid://A002/X100/X200", "uid://A002/X101/X201"]
    assert loaded[0].archive_meta["sb_name"] == "SB_A"
    assert loaded[0].archive_meta["science_category"] == "Galaxy evolution"
    assert loaded[0].archive_meta["array"] == "12m+7m"
    assert loaded[0].archive_meta["min_spw_total_width_mhz"] == 937.5
    assert loaded[0].archive_meta["max_spw_total_width_mhz"] == 2000.0
    assert loaded[0].archive_meta["min_nchan"] == 960
    assert loaded[0].archive_meta["max_nchan"] == 3840
    assert loaded[0].archive_meta["min_spw_width_nchan"] == 960
    assert loaded[0].archive_meta["max_spw_width_nchan"] == 3840
