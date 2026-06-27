# preprocessing/ — Stage 2 of the gold pipeline

Reads the immutable `raw` layer and writes the cleaned, point-in-time-aligned `staging` schema.
See the design spec: docs/superpowers/specs/2026-06-27-stage2-preprocessing-design.md

## Run
- `pip install -e ".[dev]"`, `docker compose up -d`, then
  `python -m gold_pipeline.preprocessing.run` (NEVER `python src/.../run.py` — src layout).
  Requires Stage 1 to have populated `raw` first.

## Boundaries (one job each)
- `calendar.py` — `trading_days(gold_df)`: the date backbone, from gold's own dates.
- `clean_gold.py` — per-`source` `log_return` + robust flag-only `is_outlier` (never mutate prices).
- `align_macro.py` — point-in-time reindex of each macro series onto the calendar via
  `merge_asof(direction="backward")` on `release_date`; adds `is_imputed`/`days_stale`/`is_anomaly`.
- `quality.py` — fail-fast gate before any write.
- `run.py` — wires the above; `run_preprocessing(...)` takes injectable reader seams for tests.

## Invariants
- Point-in-time: macro reindexed on `release_date`; gate asserts `release_date <= date`.
- Forward-fill only: values carried forward; pre-first-release rows stay NULL (no bfill/interpolate).
- Flag, don't mutate: raw stays immutable; staging only marks flags.
- Idempotent: composite-PK UPSERT; re-running the same dates never duplicates rows.
- Fail-fast: empty source or failed quality check raises before any DB write.
