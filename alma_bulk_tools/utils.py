from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def uid_to_path_segment(uid: str) -> str:
    cleaned = uid.strip()
    cleaned = cleaned.replace("uid://", "uid___")
    cleaned = cleaned.replace("://", "___")
    cleaned = cleaned.replace("/", "_")
    cleaned = cleaned.replace(":", "_")
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", cleaned)
    if not cleaned.startswith("uid___"):
        cleaned = f"uid___{cleaned}"
    return cleaned


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def atomic_write_text(path: Path, content: str) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    os.replace(tmp, path)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def chunked(values: list[Any], size: int) -> Iterable[list[Any]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def parse_band_token(value: str) -> str:
    value = value.strip().upper()
    if value.startswith("BAND"):
        return value
    if value.startswith("B") and value[1:].isdigit():
        return f"BAND {value[1:]}"
    if value.isdigit():
        return f"BAND {value}"
    return value
