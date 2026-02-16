# AGENTS.md

This document is for human collaborators working on `alma-bulk-tools`.

## Scope

This repo builds CLI-first tools to:
- discover public ALMA MOUS records,
- download selected archive deliverables,
- unpack selected bundles,
- summarize QA/flag metadata,
- maintain per-MOUS state and a central SQLite index,
- support restartable shard workflows (local + HPC/TACC).

Primary entrypoint: `alma-bulk` (subcommands in `alma_bulk_tools/cli.py`).

## Non-negotiable Behavior

1. Keep runs idempotent and restartable.
2. Treat each MOUS as the atomic work unit.
3. Never require concurrent writers against one shared DB in shard mode.
4. Prefer additive updates to `almaBulkManifest.json` / `almaBulkSummary.json`; do not delete user-created content.
5. Keep naming/schema explicit and archive-aligned across stages.

## Repository Map

- `alma_bulk_tools/cli.py`: command orchestration
- `alma_bulk_tools/archive_query.py`: TAP/ADQL query build + MOUS grouping
- `alma_bulk_tools/downloader.py`: datalink enumeration + resumable downloads + manifest updates
- `alma_bulk_tools/unpack.py`: controlled unpack logic (default: auxiliary/readme archives in-place)
- `alma_bulk_tools/summarize.py`: AQUA + flag-template parsing to `almaBulkSummary.json`
- `alma_bulk_tools/index_db.py`: SQLite schema + upsert/ingest/merge helpers
- `alma_bulk_tools/index_merge.py`: merge shard DB/summary outputs
- `alma_bulk_tools/status.py`: dashboard report generation
- `alma_bulk_tools/layout.py`: directory conventions and MOUS path selection
- `tests/`: core parsing/layout/selection/unpack tests
- `config.example.workflow1.yaml`: full config example for high-priority workflow

## Layout and Path Invariants

Current preferred MOUS path:

`<dest>/<project_code>/science_goal.uid___<UID>/group.uid___<UID>/member.uid___<MOUSUID>/`

Required under MOUS dir:
- `delivered/`
- `run1/`
- `almaBulkManifest.json`
- `almaBulkSummary.json` (after summarize)

Compatibility rule:
- If a legacy path exists (`science_goal_.../group_obs_unit_set_.../member.uid___...`), reuse it instead of moving data.

## Data Contracts

### `almaBulkManifest.json` (operational state)

Owned primarily by downloader/unpack stages.

Must remain stable for re-runs and include:
- MOUS identity/metadata (`mous_uid`, `project_code`, UIDs, bands/dates where known)
- `artifacts[]`: per-downloadable item (`kind`, `filename`, `url`, `local_path`, `size`, status/error/checksum)
- unpack bookkeeping (`unpacked`, `unpacked_to`, `archive_removed_after_unpack`, errors)
- `history[]`: timestamped stage events and decisions

### `almaBulkSummary.json` (analysis/index view)

Owned primarily by summarize stage.

Contains:
- schema version + MOUS identity/release/band metadata
- QA2 status (`PASS`/`SEMIPASS`/`FAIL`/`UNKNOWN`) + reasons
- MOUS-level EB list with QA0 status per EB (explicit if available; otherwise suggested inference)
- run-level (`delivered`, `run1`) artifact evidence:
  - `pprequest` paths
  - `pipeline_aquareport.xml` paths
  - `applycalQA_outliers.txt` paths
  - weblog landing page path
  - flag-template parse metrics + manual-flag evidence
- run presence booleans (`has_delivered_products`, `has_run1_products`)
- provenance timestamps/tool info

## Artifact Taxonomy and Defaults

Canonical selection tokens:
- Deliverables: `calibration`, `scripts`, `weblog`, `qa_reports`, `auxiliary`, `readme`, `raw`
- Product categories: `calibration_products`, `continuum_images`, `cubes`, `admit`

Terminology:
- `auxiliary` is the top-level archive deliverable (commonly `*_auxiliary.tar`).
- nested `auxproducts.tgz` files are treated as contents to unpack, not as the top-level deliverable category.

Default behavior (when `download.artifacts` is null):
- ON: `calibration`, `scripts`, `weblog`, `qa_reports`, `auxiliary`, `readme`, `calibration_products`
- OFF: `raw`, `continuum_images`, `cubes`, `admit`

## Unpack Rules

Default unpack policy:
- unpack `auxiliary` archives (`unpack_auxiliary=true`)
- unpack readme archives (`unpack_readme_archives=true`)
- unpack weblog archives (`unpack_weblog_archives=true`)
- keep other archive unpack off by default (`unpack_other_archives=false`)
- unpack in place (same directory where archive lives, usually `delivered/`)
- strip archive-internal redundant ASA prefix (`project/science_goal/group/member`) when present
- remove archive after successful unpack (`remove_archives_after_unpack=true`)
- recursive unpack enabled by default for nested `*.auxproducts*`, `*.caltables*`, `*weblog*`, and `*readme*`
- default recursive unpack intentionally excludes `*flagversions.tgz`

If multiple archives of same kind exist, newest mtime wins; decision is recorded in manifest history.

## Indexing/Concurrency Pattern

- Central DB: `<dest>/alma_index.sqlite`
- Shard runs write per-shard DBs (e.g., `part-0007.sqlite`)
- `merge-index` deterministically upserts shard outputs into central DB
- `scan` can rebuild/populate DB from existing tree state

Do not introduce design that requires many workers writing to one SQLite file simultaneously.

## Archive/API Notes

Current implementation targets ALMA TAP/ObsCore + datalink patterns used in ALMA archive notebooks/manual:
- public filter via `data_rights='Public'`
- MOUS grouping by `member_ous_uid`
- datalink ID normalization supports `uid://...` -> `uid___...`
- band filters handle both numeric and `BAND N` style `band_list` values
- TP/7m exclusion uses `antenna_arrays` heuristics (best effort)

When changing archive fields/endpoints:
1. verify against current ALMA docs/notebooks,
2. keep fallback behavior explicit,
3. document assumptions in `README.md`.

## Coding Patterns

1. Keep functions small and stage-local; avoid cross-stage hidden coupling.
2. Update manifests/summaries atomically (`atomic_write_json`).
3. Use clean schema changes when explicitly requested and update code/docs/tests together.
4. Add tests for parsing/selection/layout behavior before refactors.
5. Preserve CLI config precedence: CLI overrides config.
6. Destination resolution precedence: `--dest` first, then `paths.dest` from config.

## Testing and Validation

Local dev:
- `pip install -e .[dev]`
- `pytest`
- `python -m compileall alma_bulk_tools tests` (quick syntax check)

At minimum, any PR touching core behavior should include:
- one unit test for new/changed logic,
- one README/config doc update if behavior changed.

## Common Change Checklist

Before merging:
1. Did you preserve idempotent reruns?
2. Did you avoid moving/deleting user data unexpectedly?
3. Did you keep naming/schema consistent across code/docs/tests?
4. Did you update tests for changed defaults/contracts?
5. Did you update README + `config.example.workflow1.yaml`?
6. If archive assumptions changed, did you cite docs/notebooks in README notes?

## Suggested Collaboration Workflow

1. Create focused branch.
2. Add/adjust tests first for behavior deltas.
3. Implement minimal code change.
4. Run tests and smoke-check key commands on a small candidate set.
5. Update docs/config examples.
6. Open PR with:
   - behavior summary,
   - migration/compat notes,
   - risk assessment (data movement, rerun behavior, API assumptions).
