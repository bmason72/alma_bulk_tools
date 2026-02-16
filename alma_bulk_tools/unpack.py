from __future__ import annotations

import fnmatch
import logging
import os
import shutil
import tarfile
from pathlib import Path, PurePosixPath
from typing import Any

from .utils import atomic_write_json, load_json, now_utc_iso

LOGGER = logging.getLogger(__name__)
DEFAULT_RECURSIVE_UNPACK_PATTERNS = [
    "*.auxproducts.tgz",
    "*.auxproducts.tar.gz",
    "*.auxproducts.tar",
    "*.caltables.tgz",
    "*.caltables.tar.gz",
    "*.caltables.tar",
    "*weblog*.tgz",
    "*weblog*.tar.gz",
    "*weblog*.tar",
    "*readme*.tgz",
    "*readme*.tar.gz",
    "*readme*.tar",
]


def _is_archive(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(".tgz") or name.endswith(".tar.gz") or name.endswith(".tar")


def _uid_uri_from_uid_segment(segment: str) -> str | None:
    token = segment.strip()
    if token.startswith("uid___"):
        token = token[6:]
    parts = token.split("_")
    if len(parts) < 3:
        return None
    return f"uid://{parts[0]}/{parts[1]}/{'_'.join(parts[2:])}"


def _uid_from_component(component: str, prefix: str) -> str | None:
    if not component.startswith(prefix + "."):
        return None
    suffix = component.split(".", 1)[1]
    return _uid_uri_from_uid_segment(suffix)


def _member_parts(member_name: str) -> list[str]:
    return [p for p in PurePosixPath(member_name).parts if p not in {"", "."}]


def _detect_asa_prefix(members: list[tarfile.TarInfo]) -> tuple[str, ...] | None:
    detected: tuple[str, ...] | None = None
    for member in members:
        parts = _member_parts(member.name)
        if len(parts) >= 5:
            if (
                parts[1].startswith("science_goal.uid___")
                and parts[2].startswith("group.uid___")
                and parts[3].startswith("member.uid___")
            ):
                candidate = tuple(parts[:4])
            else:
                continue
        elif len(parts) >= 4:
            if (
                parts[0].startswith("science_goal.uid___")
                and parts[1].startswith("group.uid___")
                and parts[2].startswith("member.uid___")
            ):
                candidate = tuple(parts[:3])
            else:
                continue
        else:
            continue

        if detected is None:
            detected = candidate
        elif detected != candidate:
            return None
    return detected


def _detect_parent_redundant_prefix(
    members: list[tarfile.TarInfo], parent_dir_name: str
) -> tuple[str, ...] | None:
    top_levels: set[str] = set()
    for member in members:
        parts = _member_parts(member.name)
        if len(parts) < 2:
            continue
        top_levels.add(parts[0])
    if len(top_levels) == 1:
        top = next(iter(top_levels))
        if top == parent_dir_name:
            return (top,)
    return None


def _strip_parts(parts: list[str], strip_prefix: tuple[str, ...] | None) -> list[str]:
    if not strip_prefix:
        return parts
    n = len(strip_prefix)
    if len(parts) >= n and tuple(parts[:n]) == strip_prefix:
        return parts[n:]
    return parts


def _safe_extract(
    tar: tarfile.TarFile,
    *,
    target_dir: Path,
    strip_prefix: tuple[str, ...] | None,
) -> None:
    root = target_dir.resolve()
    for member in tar.getmembers():
        parts = _strip_parts(_member_parts(member.name), strip_prefix)
        if not parts:
            continue
        rel = Path(*parts)
        dest = (target_dir / rel).resolve()
        if not str(dest).startswith(str(root)):
            raise ValueError(f"Unsafe tar member path: {member.name}")

        if member.isdir():
            dest.mkdir(parents=True, exist_ok=True)
            continue

        if member.isfile():
            dest.parent.mkdir(parents=True, exist_ok=True)
            source = tar.extractfile(member)
            if source is None:
                continue
            with source, dest.open("wb") as out:
                shutil.copyfileobj(source, out)
            try:
                os.chmod(dest, member.mode & 0o777)
            except OSError:
                pass
            continue

        # Ignore links/special files for safety.
        LOGGER.debug("Skipping unsupported tar member type: %s", member.name)


def _select_archives_for_unpack(
    manifest: dict[str, Any],
    *,
    unpack_auxiliary: bool,
    unpack_readme_archives: bool,
    unpack_weblog_archives: bool,
    unpack_other_archives: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    for artifact in manifest.get("artifacts", []):
        local_path = Path(artifact.get("local_path", ""))
        if not local_path.exists() or not _is_archive(local_path):
            continue
        kind = str(artifact.get("kind") or "other").lower()
        if kind == "auxiliary" and not unpack_auxiliary:
            continue
        if kind == "readme" and not unpack_readme_archives:
            continue
        if kind == "weblog" and not unpack_weblog_archives:
            continue
        if kind not in {"auxiliary", "readme", "weblog"} and not unpack_other_archives:
            continue
        candidates.append(artifact)

    by_kind: dict[str, list[dict[str, Any]]] = {}
    for artifact in candidates:
        kind = str(artifact.get("kind") or "other").lower()
        by_kind.setdefault(kind, []).append(artifact)

    selected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for kind, group in by_kind.items():
        newest = max(group, key=lambda a: Path(a["local_path"]).stat().st_mtime)
        selected.append(newest)
        for item in group:
            if item is newest:
                continue
            skipped.append(
                {
                    "kind": kind,
                    "filename": item.get("filename"),
                    "local_path": item.get("local_path"),
                    "reason": "older archive version for same kind",
                }
            )

    return selected, skipped


def _matches_any_pattern(path: Path, patterns: list[str]) -> bool:
    rel = path.as_posix()
    name_l = path.name.lower()
    rel_l = rel.lower()
    for pattern in patterns:
        pattern_l = pattern.lower()
        if (
            fnmatch.fnmatch(path.name, pattern)
            or fnmatch.fnmatch(rel, pattern)
            or fnmatch.fnmatch(name_l, pattern_l)
            or fnmatch.fnmatch(rel_l, pattern_l)
        ):
            return True
    return False


def _recursive_unpack(
    *,
    root_dir: Path,
    patterns: list[str],
    max_passes: int,
    remove_archives_after_unpack: bool,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "enabled": True,
        "patterns": patterns,
        "passes": [],
        "unpacked_count": 0,
        "error_count": 0,
    }
    if not patterns:
        return summary

    processed: set[str] = set()
    for pass_idx in range(max(1, max_passes)):
        candidates: list[Path] = []
        for path in sorted(root_dir.rglob("*")):
            if not path.is_file() or not _is_archive(path):
                continue
            if not _matches_any_pattern(path, patterns):
                continue
            key = str(path.resolve())
            if key in processed:
                continue
            candidates.append(path)

        if not candidates:
            break

        pass_info = {
            "pass": pass_idx + 1,
            "archives": [],
            "errors": [],
        }

        for archive in candidates:
            processed.add(str(archive.resolve()))
            try:
                with tarfile.open(archive, "r:*") as tar:
                    members = tar.getmembers()
                    strip_prefix = _detect_parent_redundant_prefix(
                        members, archive.parent.name
                    )
                    _safe_extract(tar, target_dir=archive.parent, strip_prefix=strip_prefix)
                summary["unpacked_count"] += 1
                pass_info["archives"].append(str(archive))
                if remove_archives_after_unpack and archive.exists():
                    archive.unlink()
            except Exception as exc:  # noqa: BLE001
                summary["error_count"] += 1
                pass_info["errors"].append({"archive": str(archive), "error": str(exc)})
                LOGGER.warning("Recursive unpack failed for %s: %s", archive, exc)

        summary["passes"].append(pass_info)

    return summary


def unpack_mous_delivered(
    delivered_dir: Path,
    manifest_path: Path,
    *,
    unpack_auxiliary: bool = True,
    unpack_readme_archives: bool = True,
    unpack_weblog_archives: bool = True,
    unpack_other_archives: bool = False,
    remove_archives_after_unpack: bool = True,
    recursive_unpack_enabled: bool = True,
    recursive_unpack_patterns: list[str] | None = None,
    recursive_unpack_max_passes: int = 3,
) -> dict[str, Any]:
    manifest = load_json(manifest_path, default={}) or {}
    manifest.setdefault("artifacts", [])
    unpack_index = manifest.setdefault("unpacked", {})

    selected_archives, skipped_archives = _select_archives_for_unpack(
        manifest,
        unpack_auxiliary=unpack_auxiliary,
        unpack_readme_archives=unpack_readme_archives,
        unpack_weblog_archives=unpack_weblog_archives,
        unpack_other_archives=unpack_other_archives,
    )

    for artifact in selected_archives:
        local_path = Path(artifact.get("local_path", ""))
        if not local_path.exists():
            if unpack_index.get(local_path.name):
                continue
            LOGGER.info("Skipping missing archive %s", local_path)
            continue

        stamp = f"{local_path.stat().st_size}:{int(local_path.stat().st_mtime)}"
        if unpack_index.get(local_path.name) == stamp:
            continue

        target = local_path.parent
        try:
            with tarfile.open(local_path, "r:*") as tar:
                members = tar.getmembers()
                strip_prefix = _detect_asa_prefix(members)
                _safe_extract(tar, target_dir=target, strip_prefix=strip_prefix)

            artifact["unpacked_to"] = str(target)
            artifact["unpacked_at"] = now_utc_iso()
            unpack_index[local_path.name] = stamp

            if strip_prefix:
                if len(strip_prefix) == 4:
                    sg_component, group_component, member_component = (
                        strip_prefix[1],
                        strip_prefix[2],
                        strip_prefix[3],
                    )
                elif len(strip_prefix) == 3:
                    sg_component, group_component, member_component = (
                        strip_prefix[0],
                        strip_prefix[1],
                        strip_prefix[2],
                    )
                else:
                    sg_component = group_component = member_component = ""

                sg_uid = _uid_from_component(sg_component, "science_goal") if sg_component else None
                group_uid = _uid_from_component(group_component, "group") if group_component else None
                member_uid = _uid_from_component(member_component, "member") if member_component else None

                if sg_uid and (
                    not manifest.get("science_goal_uid")
                    or str(manifest.get("science_goal_uid")).endswith("unknown")
                ):
                    manifest["science_goal_uid"] = sg_uid
                if group_uid and not manifest.get("group_ous_uid"):
                    manifest["group_ous_uid"] = group_uid
                if member_uid and not manifest.get("mous_uid"):
                    manifest["mous_uid"] = member_uid

                artifact["strip_prefix"] = "/".join(strip_prefix)

            if remove_archives_after_unpack and local_path.exists():
                local_path.unlink()
                artifact["archive_removed_after_unpack"] = True
            else:
                artifact["archive_removed_after_unpack"] = False

            LOGGER.info("Unpacked %s into %s", local_path, target)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to unpack %s: %s", local_path, exc)
            artifact.setdefault("unpack_errors", []).append(
                {"timestamp": now_utc_iso(), "error": str(exc)}
            )

    patterns = (
        list(recursive_unpack_patterns)
        if recursive_unpack_patterns is not None
        else list(DEFAULT_RECURSIVE_UNPACK_PATTERNS)
    )

    recursive_summary = {
        "enabled": False,
        "patterns": patterns,
        "passes": [],
        "unpacked_count": 0,
        "error_count": 0,
    }
    if recursive_unpack_enabled:
        recursive_summary = _recursive_unpack(
            root_dir=delivered_dir,
            patterns=patterns,
            max_passes=int(recursive_unpack_max_passes),
            remove_archives_after_unpack=remove_archives_after_unpack,
        )

    manifest.setdefault("history", []).append(
        {
            "timestamp": now_utc_iso(),
            "event": "unpack",
            "message": "Unpack pass completed",
            "unpack_auxiliary": unpack_auxiliary,
            "unpack_readme_archives": unpack_readme_archives,
            "unpack_weblog_archives": unpack_weblog_archives,
            "unpack_other_archives": unpack_other_archives,
            "remove_archives_after_unpack": remove_archives_after_unpack,
            "selected_archives": [
                {
                    "kind": a.get("kind"),
                    "filename": a.get("filename"),
                    "local_path": a.get("local_path"),
                }
                for a in selected_archives
            ],
            "skipped_archives": skipped_archives,
            "recursive_unpack": recursive_summary,
        }
    )
    atomic_write_json(manifest_path, manifest)
    return manifest
