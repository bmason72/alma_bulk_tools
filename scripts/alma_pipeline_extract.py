#!/usr/bin/env python3
"""Best-effort extractor placeholder.

Use `alma-bulk summarize` to parse AQUA/flag artifacts discovered under delivered/.
"""

from pathlib import Path
import json
import argparse

from alma_bulk_tools.summarize import find_key_artifacts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--delivered", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    payload = find_key_artifacts(args.delivered)
    args.out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
