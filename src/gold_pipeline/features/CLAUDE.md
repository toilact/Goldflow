# features/ — Stage 3 of the gold pipeline

Reads the `staging` layer and writes the wide, model-ready `features.gold_features`.
See the design spec: docs/superpowers/specs/2026-06-29-stage3-feature-engineering-design.md

## Run
- `pip install -e ".[dev]"`, `docker compose up -d`, then
  `python -m gold_pipeline.features.run` (NEVER `python src/.../run.py` — src layout).
  Requires Stage 1 + 2 to have populated `staging` first.

## Boundaries (one job each)
- `config.py` — FeatureConfig + the canonical column list (source of truth; must match migration 003).
- `technical.py` — per-source `ta` indicators + stationary `close_to_sma_*` ratios.
- `lagged.py` — stationary lags (`log_return`, `rsi_14`) via groupby shift(+k).
- `macro_features.py` — pivot staging.macro_aligned long->wide + carry is_imputed/is_anomaly.
- `target.py` — multi-horizon `target_logret_{h}` = groupby shift(-h) (only future-looking column).
- `assemble.py` — combine + readiness flags (has_features / has_target_{h}); normalizes date dtype.
- `quality.py` — fail-fast gate before any write.
- `run.py` — wires the above; `run_features(...)` takes injectable reader seams for tests.

## Invariants
- No look-ahead: every shift goes through groupby("source"); only targets see the future.
- Stationary inputs: never lag absolute `close`; use returns/ratios/oscillators.
- Flag, don't mutate: keep NaN -> NULL, add flags; never bfill/interpolate; never drop rows.
- config <-> schema: feature_table_columns must equal migration 003 columns (quality gate enforces).
- Idempotent: composite-PK UPSERT. Fail-fast: empty staging or failed check raises before any write.
