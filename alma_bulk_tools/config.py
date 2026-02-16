from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG: dict[str, Any] = {
    "paths": {
        "dest": None,
    },
    "archive": {
        "tap_sync_url": "https://almascience.nrao.edu/tap/sync",
        "datalink_sync_url": "https://almascience.nrao.edu/datalink/sync",
        "timeout_sec": 120,
        "user_agent": "alma-bulk-tools/0.1.0",
    },
    "filters": {
        "date_field": "release",
        "exclude_tp": False,
        "exclude_7m": False,
        "bands_include": [],
        "bands_exclude": [],
        "project_codes_include": [],
        "project_codes_exclude": [],
        "mous_include": [],
        "mous_exclude": [],
        "min_freq_ghz": None,
        "max_freq_ghz": None,
    },
    "download": {
        # If artifacts is null/omitted, the effective artifact set is derived from
        # download.deliverables + download.products.
        "artifacts": None,
        "deliverables": {
            "calibration": True,
            "scripts": True,
            "weblog": True,
            "qa_reports": True,
            "auxiliary": True,
            "readme": True,
            "raw": False,
        },
        "products": {
            "calibration_products": True,
            "continuum_images": False,
            "cubes": False,
            "admit": False,
        },
        "max_workers": 4,
        "rate_limit_sec": 0.0,
        "compute_sha256": False,
        "retry_count": 3,
    },
    "unpack": {
        "unpack_auxiliary": True,
        "unpack_readme_archives": True,
        "unpack_weblog_archives": True,
        "unpack_other_archives": False,
        "remove_archives_after_unpack": True,
        "recursive_unpack_enabled": True,
        "recursive_unpack_patterns": [
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
        ],
        "recursive_unpack_max_passes": 3,
    },
    "runtime": {
        "max_runtime_min": None,
        "log_level": "INFO",
    },
}


def _deep_update(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def load_config(config_path: str | Path | None) -> dict[str, Any]:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    if not config_path:
        return cfg
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError("Config root must be a mapping")
    _deep_update(cfg, payload)
    return cfg


def apply_cli_overrides(cfg: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    def _drop_none(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: _drop_none(v) for k, v in value.items() if v is not None}
        if isinstance(value, list):
            return [_drop_none(v) for v in value if v is not None]
        return value

    cleaned = _drop_none(overrides)
    _deep_update(cfg, cleaned)
    return cfg


def load_list_from_value(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        path = Path(value)
        if path.exists() and path.is_file():
            suffix = path.suffix.lower()
            if suffix in {".yaml", ".yml"}:
                payload = yaml.safe_load(path.read_text(encoding="utf-8"))
                if isinstance(payload, list):
                    return [str(v).strip() for v in payload if str(v).strip()]
                if isinstance(payload, dict):
                    out: list[str] = []
                    for v in payload.values():
                        if isinstance(v, list):
                            out.extend([str(i).strip() for i in v if str(i).strip()])
                        elif v is not None and str(v).strip():
                            out.append(str(v).strip())
                    return out
            if suffix == ".json":
                payload = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(payload, list):
                    return [str(v).strip() for v in payload if str(v).strip()]
                if isinstance(payload, dict):
                    return [str(v).strip() for v in payload.values() if str(v).strip()]
            if suffix == ".csv":
                out = []
                with path.open("r", encoding="utf-8") as handle:
                    reader = csv.reader(handle)
                    for row in reader:
                        for cell in row:
                            cell = cell.strip()
                            if cell:
                                out.append(cell)
                return out
            out = []
            for line in path.read_text(encoding="utf-8").splitlines():
                value = line.strip()
                if value and not value.startswith("#"):
                    out.append(value)
            return out
        return [s.strip() for s in value.split(",") if s.strip()]
    return [str(value).strip()]
