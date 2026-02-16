from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class MousRecord:
    project_code: str
    member_ous_uid: str
    group_ous_uid: str | None = None
    science_goal_uid: str | None = None
    eb_uids: list[str] = field(default_factory=list)
    band_list: list[str] = field(default_factory=list)
    release_date: str | None = None
    obs_date: str | None = None
    qa2_passed: bool | None = None
    qa0_status: str | None = None
    qa0_reasons: list[str] = field(default_factory=list)
    qa2_reasons: list[str] = field(default_factory=list)
    source_rows: int = 0
    archive_meta: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ArtifactInfo:
    kind: str
    url: str
    filename: str
    semantics: str | None = None
    content_type: str | None = None
    size_bytes: int | None = None
    checksum: str | None = None
    description: str | None = None


@dataclass(slots=True)
class ManifestEntry:
    kind: str
    filename: str
    url: str
    local_path: str
    size_bytes: int | None
    checksum: str | None
    status: str
    downloaded_at: str | None
    updated_at: str
    unpacked_to: str | None = None


@dataclass(slots=True)
class ToolRunContext:
    started_at: datetime
    tool_version: str
    command: str
    query_timestamp: str | None = None
