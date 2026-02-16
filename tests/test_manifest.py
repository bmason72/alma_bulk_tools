from alma_bulk_tools.downloader import datalink_id_from_member_ous_uid, resolve_artifact_selection


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
