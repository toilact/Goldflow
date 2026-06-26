# ingestion/ — Stage 1 of the gold pipeline

Fetches gold prices + macro indicators and UPSERTs them into the PostgreSQL `raw` schema.
See the design spec: docs/superpowers/specs/2026-06-26-stage1-ingestion-design.md

## Run
- `pip install -e ".[dev]"` then `docker compose up -d`, then
  `python -m gold_pipeline.ingestion.run` (NEVER `python src/.../run.py` — src layout).

## Boundaries (one job each)
- `config.py` — env -> `Settings`.
- `http.py` — `with_retry` / `rate_limited`; ALL external calls go through these.
- `sources/` — return normalized DataFrames (see sources/CLAUDE.md).
- `storage/` — only path to the DB; idempotent UPSERT (see storage/CLAUDE.md).
- `quality.py` — fail-fast gate before any write.
- `run.py` — wires the above; `run_ingestion(...)` takes injectable seams for tests.

## Invariants
- Idempotent: re-running the same dates never duplicates rows (composite-PK UPSERT).
- Point-in-time: macro rows carry first-release `release_date`; Stage 1 only stores it.
- Fail-fast: empty source or failed quality check raises before any DB write.
