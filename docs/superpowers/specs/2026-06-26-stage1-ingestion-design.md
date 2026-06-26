# Design Spec: Stage 1 — Data Ingestion (Gold + Macro)

Date: 2026-06-26
Status: Approved design — ready for implementation plan
Scope: Stage ① of the gold data pipeline. Builds ingestion for **gold prices** and **macro
indicators**, writing to the PostgreSQL `raw` schema via Docker. News, staging, features, and
orchestration tools (Airflow/Prefect) are explicitly out of scope for this round.

## 1. Objective

Fetch raw market data from external APIs and persist it, immutable and idempotently, into the
PostgreSQL `raw` layer — as the foundation the later stages build on. Two sources this round:

- **Gold prices** — Yahoo Finance, ticker `GC=F` (gold futures), via `yfinance`.
- **Macro indicators** — FRED, via `fredapi`: `DGS10` (US 10Y yield), `DTWEXBGS` (broad USD index),
  `CPIAUCSL` (CPI).

## 2. Directory structure (per-folder CLAUDE.md for handoff)

```
Gold/
├── CLAUDE.md                       # project-wide (exists)
├── docker-compose.yml              # Postgres service (app DB + test DB)
├── pyproject.toml                  # package config → enables `python -m gold_pipeline...`
├── .env.example                    # FRED_API_KEY, DB DSN, date range
├── db/
│   ├── CLAUDE.md                   # migration conventions
│   └── migrations/
│       └── 001_raw_schema.sql
├── src/gold_pipeline/
│   └── ingestion/
│       ├── CLAUDE.md               # Stage-1 rules: retry, rate-limit, point-in-time, idempotency
│       ├── __init__.py
│       ├── config.py               # Settings dataclass from .env
│       ├── http.py                 # tenacity retry + rate-limit decorators (shared)
│       ├── sources/
│       │   ├── CLAUDE.md           # recipe for adding a new source
│       │   ├── __init__.py
│       │   ├── gold_prices.py      # yfinance
│       │   └── macro_fred.py       # FRED (with release_date)
│       ├── storage/
│       │   ├── CLAUDE.md           # raw-schema UPSERT writer
│       │   ├── __init__.py
│       │   └── raw_writer.py
│       └── run.py                  # CLI orchestrator entrypoint
└── tests/
    └── ingestion/
        ├── test_sources.py         # pandas-only, mocked API — no DB
        └── test_raw_writer.py      # integration — needs Postgres test DB
```

## 3. Units, responsibilities, interfaces

Each unit has one job and communicates through a normalized `pandas.DataFrame` so the orchestrator
and writer never need to know which source produced the data.

| Unit | File | Responsibility | Public interface |
|---|---|---|---|
| Config | `config.py` | Load `.env` into a frozen `Settings` dataclass (FRED key, DB DSN, date range). | `Settings.from_env() -> Settings` |
| HTTP/retry | `http.py` | Reusable retry (exponential backoff + jitter) and rate-limit decorators. | `@with_retry`, `@rate_limited(min_interval_s)` |
| Source: gold | `sources/gold_prices.py` | Fetch + normalize gold OHLCV. | `fetch_gold_prices(start, end, ticker="GC=F") -> DataFrame` |
| Source: macro | `sources/macro_fred.py` | Fetch FRED series **with point-in-time release date**. | `fetch_fred_series(fred, series_id) -> DataFrame` |
| Storage | `storage/raw_writer.py` | Idempotent UPSERT into `raw` schema. | `upsert_dataframe(engine, df, table, schema, pk) -> int` |
| Orchestrator | `run.py` | Wire sources → quality check → writer; log counts/ranges. | `python -m gold_pipeline.ingestion.run` |

### Normalized output columns
- Gold: `date, open, high, low, close, volume, source`
- Macro: `date, series_id, value, release_date`

## 4. Gotcha #1 — FRED release_date (point-in-time, leakage-critical)

`fred.get_series()` returns only `(observation_date, value)` — **no release date**. Storing the
observation date as if it were known on that day causes look-ahead leakage in Stage 2 (CPI for
reference-month May is not published until ~mid-June).

**Resolution:** `macro_fred.py` uses `fred.get_series_all_releases(series_id)`, which returns a
long-format frame with a `realtime_start` column (the date each value became publicly available).
The point-in-time **first-release date** per observation is:

```python
all_rel = fred.get_series_all_releases(series_id)   # columns: date, realtime_start, value
first = (all_rel.sort_values("realtime_start")
                .groupby("date", as_index=False)
                .first())                            # earliest realtime_start = first release
# → date (observation), value (first-released value), realtime_start (release_date)
```

- For **daily** series (`DGS10`, `DTWEXBGS`) the release lag is ~1 day with negligible revisions, so
  `release_date ≈ observation_date`. For **CPI** the lag is ~2 weeks and matters a lot.
- We use `get_series_all_releases` uniformly for all three series for a single consistent code path.
  It is slower than `get_series`, but ingestion runs once per day so the cost is irrelevant.
- Stage 1 only **stores** `release_date`. It does NOT join or shift on it — that is Stage 2's job.

## 5. Gotcha #2 — SQL dialect (test parity)

`sqlalchemy.dialects.postgresql.insert(...).on_conflict_do_update(...)` is Postgres-only and fails
on SQLite. **Resolution: tests use Postgres, not SQLite** — `docker-compose.yml` exposes a separate
`gold_test` database (or test schema) on the same container.

Test split keeps the fast path DB-free:
- `test_sources.py` — pure pandas, mocked `yfinance`/`fredapi` responses; asserts normalized columns,
  release_date derivation, and error handling on empty data. **No DB required.**
- `test_raw_writer.py` — integration against the Postgres test DB; asserts that running the same
  UPSERT twice yields no duplicate rows (idempotency).

## 6. Gotcha #3 — package/import resolution (not circular imports)

This is a **src-layout module-resolution** issue, not a circular import. Running
`python src/gold_pipeline/ingestion/run.py` leaves the package dir off `sys.path`, breaking
`from gold_pipeline.ingestion... import ...`.

**Resolution:**
- Declare the package in `pyproject.toml` (setuptools, `src` layout) and install editable:
  `pip install -e .`.
- Run via the module entrypoint, documented in CLAUDE.md:
  `python -m gold_pipeline.ingestion.run` — never `python src/.../run.py`.

## 7. Database schema (`raw`)

`db/migrations/001_raw_schema.sql`, idempotent (`CREATE ... IF NOT EXISTS`):

```sql
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.gold_prices (
    date        DATE        NOT NULL,
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4),
    volume      BIGINT,
    source      TEXT        NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, source)
);

CREATE TABLE IF NOT EXISTS raw.macro_indicators (
    date         DATE NOT NULL,         -- observation date
    series_id    TEXT NOT NULL,         -- 'DGS10' | 'DTWEXBGS' | 'CPIAUCSL'
    value        NUMERIC(14,6),
    release_date DATE,                  -- point-in-time first-release date (realtime_start)
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, series_id)
);
```

Idempotency relies on these composite PKs + `ON CONFLICT (pk) DO UPDATE`.

## 8. Error handling

- Transient network/connection errors → `tenacity` retry 5×, exponential backoff 2→30s with jitter.
  After exhaustion → re-raise and abort (fail-fast); no partial writes.
- Empty DataFrame from a source → raise `ValueError` immediately (catches bad ticker / date range).
- Rate limiting: a minimum interval between calls via `@rate_limited`; honor `Retry-After` if present.

## 9. Minimal data-quality checks (gate before writing to `raw`)

Just enough to keep garbage out of `raw`; deeper business checks belong to Stage 2:

- No duplicate rows on the primary key.
- `date` strictly increasing after sort.
- OHLC logic: `high >= low`, `low <= close <= high` (gold table).
- No NULLs in key columns (`date`, `source` / `series_id`).

Fail-fast: a failed check raises and aborts before any write.

## 10. Configuration & secrets

`.env` (gitignored) with `.env.example` committed:

```
FRED_API_KEY=
DATABASE_URL=postgresql+psycopg2://gold:gold@localhost:5432/gold
TEST_DATABASE_URL=postgresql+psycopg2://gold:gold@localhost:5432/gold_test
INGEST_START=2015-01-01
INGEST_END=2025-01-01
```

## 11. Dependencies

`yfinance`, `fredapi`, `pandas`, `numpy`, `sqlalchemy`, `psycopg2-binary`, `tenacity`,
`python-dotenv`; dev: `pytest`.

## 12. Out of scope (this round)

News ingestion · staging/alignment · feature engineering · packaging · workflow orchestration
(Airflow/Prefect) · Great Expectations / pandera. These come in later stages.

## 13. Open assumptions (made explicit)

- Ticker `GC=F` (gold futures) is the primary price source; `XAU/USD` spot can be added later as
  another `source` value without schema change.
- Postgres credentials `gold/gold` for local/dev only.
- Date range defaults to 2015–2025; overridable via `.env`.
```
