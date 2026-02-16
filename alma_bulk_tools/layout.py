from __future__ import annotations

from pathlib import Path
from typing import Iterable

from .models import MousRecord
from .utils import ensure_dir, uid_to_path_segment


FALLBACK_SCIENCE_GOAL = "science_goal.uid___unknown"
FALLBACK_GROUP = "group.uid___unknown"
MANIFEST_FILENAME = "almaBulkManifest.json"
SUMMARY_FILENAME = "almaBulkSummary.json"


def _project_segment(project_code: str | None) -> str:
    project = (project_code or "unknown_project").strip()
    if not project:
        project = "unknown_project"
    return project.replace("/", "_")


def _legacy_build_mous_dir(dest_root: Path, record: MousRecord) -> Path:
    science = (
        f"science_goal_{uid_to_path_segment(record.science_goal_uid)}"
        if record.science_goal_uid
        else "science_goal_unknown"
    )
    group = (
        f"group_obs_unit_set_{uid_to_path_segment(record.group_ous_uid)}"
        if record.group_ous_uid
        else "group_obs_unit_set_unknown"
    )
    member = f"member.uid___{uid_to_path_segment(record.member_ous_uid).replace('uid___', '', 1)}"
    return dest_root / science / group / member


def _find_existing_project_member_dir(dest_root: Path, project: str, member: str) -> Path | None:
    project_root = dest_root / project
    if not project_root.exists():
        return None
    matches = sorted(
        path
        for path in project_root.glob(f"*/*/{member}")
        if path.is_dir()
    )
    return matches[0] if matches else None


def build_mous_dir(dest_root: Path, record: MousRecord) -> Path:
    # Keep backward compatibility with earlier layout builds and avoid moving user data.
    legacy = _legacy_build_mous_dir(dest_root, record)
    if legacy.exists():
        return legacy

    project = _project_segment(record.project_code)
    science = (
        f"science_goal.{uid_to_path_segment(record.science_goal_uid)}"
        if record.science_goal_uid
        else FALLBACK_SCIENCE_GOAL
    )
    group = (
        f"group.{uid_to_path_segment(record.group_ous_uid)}"
        if record.group_ous_uid
        else FALLBACK_GROUP
    )
    member = f"member.{uid_to_path_segment(record.member_ous_uid)}"
    preferred = dest_root / project / science / group / member
    if preferred.exists():
        return preferred

    existing = _find_existing_project_member_dir(dest_root, project, member)
    if existing is not None:
        return existing

    return preferred


def ensure_mous_layout(dest_root: Path, record: MousRecord) -> dict[str, Path]:
    mous_dir = ensure_dir(build_mous_dir(dest_root, record))
    delivered = ensure_dir(mous_dir / "delivered")
    run1 = ensure_dir(mous_dir / "run1")
    return {
        "mous_dir": mous_dir,
        "delivered": delivered,
        "run1": run1,
        "manifest": mous_dir / MANIFEST_FILENAME,
        "summary": mous_dir / SUMMARY_FILENAME,
    }


def find_mous_dirs(root: Path) -> list[Path]:
    result: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_dir():
            continue
        name = path.name
        if name.startswith("member.uid___") or name.startswith("member_uid___"):
            result.append(path)
            continue
        if (path / MANIFEST_FILENAME).exists() or (path / SUMMARY_FILENAME).exists():
            result.append(path)
    return sorted(set(result))


def ensure_layout_for_existing_mous(mous_dirs: Iterable[Path], fix_layout: bool) -> None:
    if not fix_layout:
        return
    for path in mous_dirs:
        ensure_dir(path / "delivered")
        ensure_dir(path / "run1")
