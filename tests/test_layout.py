from pathlib import Path

from alma_bulk_tools.layout import build_mous_dir, ensure_mous_layout
from alma_bulk_tools.models import MousRecord


def test_build_mous_dir_with_known_uids(tmp_path: Path) -> None:
    record = MousRecord(
        project_code="2019.1.00001.S",
        member_ous_uid="uid://A001/X15a0/X111",
        group_ous_uid="uid://A001/X15a0/X222",
        science_goal_uid="uid://A001/X15a0/X333",
    )
    path = build_mous_dir(tmp_path, record)
    path_str = str(path)
    assert "2019.1.00001.S" in path_str
    assert "science_goal.uid___A001_X15a0_X333" in path_str
    assert "group.uid___A001_X15a0_X222" in path_str
    assert path.name == "member.uid___A001_X15a0_X111"


def test_ensure_layout_fallback(tmp_path: Path) -> None:
    record = MousRecord(project_code="P", member_ous_uid="uid://A001/X1/X2")
    layout = ensure_mous_layout(tmp_path, record)
    assert layout["mous_dir"].exists()
    assert layout["delivered"].exists()
    assert layout["run1"].exists()
    assert "science_goal.uid___unknown" in str(layout["mous_dir"])


def test_build_mous_dir_preserves_legacy_layout_if_present(tmp_path: Path) -> None:
    record = MousRecord(
        project_code="2019.1.00001.S",
        member_ous_uid="uid://A001/X15a0/X111",
        group_ous_uid="uid://A001/X15a0/X222",
        science_goal_uid="uid://A001/X15a0/X333",
    )
    legacy = (
        tmp_path
        / "science_goal_uid___A001_X15a0_X333"
        / "group_obs_unit_set_uid___A001_X15a0_X222"
        / "member.uid___A001_X15a0_X111"
    )
    legacy.mkdir(parents=True)

    path = build_mous_dir(tmp_path, record)
    assert path == legacy


def test_build_mous_dir_reuses_existing_project_member_path_when_uids_change(tmp_path: Path) -> None:
    record = MousRecord(
        project_code="2022.1.00055.S",
        member_ous_uid="uid://A001/X2d20/X3ca2",
        group_ous_uid="uid://A001/X2d20/X3ca1",
        science_goal_uid="uid://A001/X2d20/X3ca0",
    )
    existing = (
        tmp_path
        / "2022.1.00055.S"
        / "science_goal.uid___unknown"
        / "group.uid___A001_X2d20_X3ca1"
        / "member.uid___A001_X2d20_X3ca2"
    )
    existing.mkdir(parents=True)

    path = build_mous_dir(tmp_path, record)
    assert path == existing
