# Design Spec: Stage 2 — Preprocessing (Gold + Macro → `staging`)

Date: 2026-06-27
Status: Approved design — ready for implementation plan
Scope: Stage ② of the gold data pipeline. Reads the immutable `raw` layer (gold prices + 3 macro
series) and produces a cleaned, point-in-time-aligned `staging` layer on PostgreSQL. News, feature
engineering, packaging, and workflow orchestration remain out of scope.

## 1. Objective

Transform `raw` source data into a clean, leakage-safe `staging` layer:

- **Gold** — clean OHLCV, compute `log_return`, flag outliers (no value mutation). Gold's own
  trading dates define the pipeline's **trading calendar** (the backbone every other source aligns to).
- **Macro** (`DGS10`, `DTWEXBGS`, `CPIAUCSL`) — reindex each series onto the gold trading calendar
  using **point-in-time** semantics (`merge_asof(direction="backward")` on `release_date`), carrying
  the last *publicly released* value forward, flagging imputed/stale/anomalous rows.

The whole stage exists to enforce two invariants from CLAUDE.md: **point-in-time correctness** (only
data actually published by time `t` is visible at `t`) and **no look-ahead / forward-fill only**.

## 2. Inputs and outputs

| | Source | Shape |
|---|---|---|
| **Input** | `raw.gold_prices` (PK `date, source`) | daily OHLCV, trading days only |
| **Input** | `raw.macro_indicators` (PK `date, series_id`) | obs `date`, `value`, point-in-time `release_date` |
| **Output** | `staging.gold_prices` (PK `date, source`) | cleaned gold + `log_return`, `is_outlier` |
| **Output** | `staging.macro_aligned` (PK `date, series_id`) | one row per `(gold trading day × series)`, point-in-time value |

Stage 2 only **stores** the aligned point-in-time values; it does NOT pivot to wide form or compute
features — that is Stage 3. The `staging` tables stay long/normalized.

## 3. Directory structure

A shared `db/` module is extracted (a second stage now needs the writer/migration runner), and a new
`preprocessing/` package is added next to `ingestion/`.

```
src/gold_pipeline/
├── db/                      # NEW — shared across all stages
│   ├── __init__.py
│   ├── CLAUDE.md
│   ├── writer.py            # run_migrations, upsert_dataframe (moved from ingestion/storage/raw_writer.py)
│   └── reader.py            # read_table(engine, schema, table) -> DataFrame
├── ingestion/               # Stage 1 — imports updated to ..db.writer; storage/ removed; behavior unchanged
└── preprocessing/           # NEW — Stage 2
    ├── __init__.py
    ├── CLAUDE.md            # Stage-2 rules: calendar, point-in-time align, flag-don't-mutate
    ├── calendar.py          # trading_days(gold_df) -> sorted unique dates
    ├── clean_gold.py        # clean_gold(gold_df) -> cleaned + log_return + is_outlier
    ├── align_macro.py       # align_macro_series(macro_df, trading_days) -> point-in-time long frame
    ├── quality.py           # check_staging_gold / check_staging_macro (fail-fast gate)
    └── run.py               # orchestrator: read raw -> transform -> check -> upsert staging

db/migrations/002_staging_schema.sql   # NEW — staging schema + two tables (idempotent)

tests/
├── db/                      # integration (needs Postgres gold_test)
│   ├── test_writer.py       # moved from tests/ingestion/test_raw_writer.py
│   └── test_reader.py       # NEW
├── ingestion/               # unchanged (unit, no DB)
└── preprocessing/           # NEW (unit, pure pandas, no DB)
    ├── test_calendar.py
    ├── test_clean_gold.py    # incl. spike-then-revert must not flag t+1
    ├── test_align_macro.py   # incl. point-in-time: release_date <= date for every row
    └── test_quality.py
```

`Settings` (already exposes `database_url`) is reused as-is; no config changes.

## 4. Shared `db/` module (refactor)

`run_migrations` and `upsert_dataframe` are generic (they take `engine, df, table, schema, pk`) — they
are not raw-specific. They move to `db/writer.py` so both stages import from one place and we avoid a
backwards dependency (`preprocessing` → `ingestion`).

- `db/writer.py` — `run_migrations(engine, migrations_dir)`, `upsert_dataframe(engine, df, table, schema, pk)`.
  Identical logic to the current `raw_writer.py` (Postgres `INSERT ... ON CONFLICT (pk) DO UPDATE`).
- `db/reader.py` — `read_table(engine, schema, table) -> pd.DataFrame` (a thin `SELECT *`), used by
  Stage 2 to load `raw.gold_prices` / `raw.macro_indicators`.
- `ingestion/storage/` — **removed entirely** (no leftover re-export shim). Stage 1's only consumer,
  `ingestion/run.py:16`, switches its import to `from ..db.writer import run_migrations, upsert_dataframe`.

**Import-audit of existing Stage 1 code/tests (verified against the current tree):**

| File | Today | After refactor |
|---|---|---|
| `src/.../ingestion/run.py` | `from .storage.raw_writer import run_migrations, upsert_dataframe` | `from ..db.writer import run_migrations, upsert_dataframe` |
| `src/.../ingestion/storage/{raw_writer.py,__init__.py,CLAUDE.md}` | exists | deleted (logic + doc moved to `db/`) |
| `tests/ingestion/test_raw_writer.py` | `from gold_pipeline.ingestion.storage.raw_writer import ...` | moved → `tests/db/test_writer.py`, import `from gold_pipeline.db.writer import ...` |
| `tests/ingestion/test_run.py` | `monkeypatch.setattr(run_mod, "upsert_dataframe", ...)` | **unchanged** — patches `run.py`'s namespace, which still binds the name |
| `tests/ingestion/test_sources_*.py`, `test_config.py`, `test_http.py`, `test_quality.py` | no storage import | unchanged |

After the move, `tests/db/` holds `test_writer.py` (relocated) and `test_reader.py` (new) — both
integration tests against the Postgres `gold_test` DB that skip when `TEST_DATABASE_URL` is unset.
A full `pytest -q` run after the refactor (before any Stage 2 logic) is the gate that proves no
import is left dangling.

## 5. Database schema (`002_staging_schema.sql`)

Idempotent (`CREATE ... IF NOT EXISTS`); composite PKs mirror `raw` so re-running is idempotent via
`ON CONFLICT (pk) DO UPDATE`.

```sql
CREATE SCHEMA IF NOT EXISTS staging;

-- Gold, cleaned. `date` is the trading-calendar backbone; gold is never imputed
-- (it defines the calendar, so there are no internal days to fill for itself).
CREATE TABLE IF NOT EXISTS staging.gold_prices (
    date         DATE        NOT NULL,
    open         NUMERIC(12,4),
    high         NUMERIC(12,4),
    low          NUMERIC(12,4),
    close        NUMERIC(12,4),
    volume       BIGINT,
    log_return   NUMERIC(12,8),                  -- log(close / close.shift(1)); basis for outlier flag
    is_outlier   BOOLEAN     NOT NULL DEFAULT false,
    source       TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, source)
);

-- Macro, reindexed point-in-time onto each gold trading day (long-form).
CREATE TABLE IF NOT EXISTS staging.macro_aligned (
    date         DATE NOT NULL,                  -- gold trading day (calendar backbone)
    series_id    TEXT NOT NULL,
    value        NUMERIC(14,6),                  -- latest first-release value PUBLISHED on or before `date`
    release_date DATE,                           -- release_date of the value in use (invariant: <= date)
    is_imputed   BOOLEAN NOT NULL DEFAULT false, -- true when carried forward (release_date < date)
    days_stale   INTEGER,                        -- (date - release_date) in days
    is_anomaly   BOOLEAN NOT NULL DEFAULT false, -- out-of-range value or excessive staleness (flag only)
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, series_id)
);
```

Trading days *before* a series' first release have `value = NULL` and `release_date/is_imputed/
days_stale = NULL` — we never invent or back-fill past data (forward-fill only). These NULL-value rows
are permitted (DQ forbids NULLs only in key columns).

## 6. Gold cleaning (`clean_gold.py`)

`clean_gold(gold_df: pd.DataFrame) -> pd.DataFrame`

**All time-series operations are computed per `source` (`groupby("source")`)** — `raw.gold_prices` is
keyed `(date, source)` and a second source (`XAU/USD`) is a planned addition, so a bare `shift`/`rolling`
over the whole frame would splice one source's prices onto another's at the group boundary. Use
`transform` (not `apply(lambda)`) to keep index alignment exact.

1. Sort by `(source, date)`; assert no duplicate `(date, source)`.
2. Per source: `log_return = log(close / close.shift(1))` (first row of each source = NaN), e.g.
   `df.groupby("source")["close"].transform(lambda s: np.log(s / s.shift(1)))`.
3. **Outlier flag (flag-only, trailing, robust) — within each source:**
   - Robust z-score `z_t = 0.6745 · (ret_t − median) / (MAD + ε)`, with `ε = 1e-8` guarding against
     `MAD = 0` (flat-return stretches over holidays/illiquidity would otherwise divide by zero and
     emit `inf`/`NaN`, crashing the run). `median`/`MAD` are over a trailing rolling window
     (default 21, **past-only** — no centering, no future), computed per source. MAD = median absolute
     deviation. Rows with too-short a window (the first ~21 of each source) get `is_outlier = false`.
   - Candidate outliers: `|z_t| > k` (default `k = 5`). The `ε` floor keeps genuinely flat windows at
     `z ≈ 0` while still flagging a real jump that lands inside a flat stretch.
4. **Anti-propagation to `t+1` (spike-then-revert collapse), within each source:** a single bad price
   produces two anomalous returns — the spike at `t` and the mechanical reversion at `t+1` (opposite
   sign). When `t` and `t+1` are both candidates **with opposite signs** (and belong to the same
   source), flag `is_outlier` only at `t` (the price event) and clear the induced flag at `t+1`.
   Same-sign consecutive candidates are left as-is (a real two-day move, not a single-price artifact).
5. Values are **never mutated** — `is_outlier` is a flag downstream stages decide how to use.

Output columns: `date, open, high, low, close, volume, log_return, is_outlier, source`.

## 7. Macro point-in-time alignment (`align_macro.py`)

`align_macro_series(macro_series_df: pd.DataFrame, trading_days: pd.Series) -> pd.DataFrame`, applied
per `series_id`, results concatenated.

Point-in-time join — for each gold trading day `T`, take the macro value whose `release_date` is the
greatest value `<= T` (the most recently *published* observation as of `T`):

```python
left  = pd.DataFrame({"date": trading_days}).sort_values("date")          # gold calendar
right = series_df.sort_values("release_date")                             # one series, raw rows
out = pd.merge_asof(
    left, right,
    left_on="date", right_on="release_date",
    direction="backward",                                                 # latest release <= T
)
```

Derived columns:
- `is_imputed = release_date < date` (value carried forward onto a non-release day; `False` on a day
  whose own release lands exactly on `T`; `NULL` where there is no release yet).
- `days_stale = (date - release_date).days` (`NULL` before first release).
- Leading rows (trading days before the first `release_date`) keep `value = NULL`; never back-filled.

This carries CPI (monthly, ~2 weeks publication lag) forward across all trading days until the next
release, and gives daily series (`DGS10`, `DTWEXBGS`, released next-day in Stage 1) a one-day-lagged,
leakage-safe value. Stage 2 does not shift further; it only reindexes on the stored `release_date`.

### 7a. Macro sanity / anomaly flag (`is_anomaly`, flag-only)

Lightweight plausibility checks, flag (never hard-fail, never mutate):
- **Per-series value bounds** — out of range ⇒ `is_anomaly = true`:
  - `DGS10` (10Y yield, %): `[-2, 25]`
  - `DTWEXBGS` (broad USD index): `[50, 200]`
  - `CPIAUCSL` (index level): `> 0`
- **Staleness ceiling** — `days_stale` beyond the series' expected cadence ⇒ `is_anomaly = true`
  (suspected missed release). Defaults: monthly `CPIAUCSL` > 45 days; daily `DGS10`/`DTWEXBGS` > 7 days.

Bounds and ceilings live in a small per-series config dict in `align_macro.py` so they are easy to read
and adjust.

## 8. Data-quality gate (`quality.py`)

Fail-fast, before any write (structural correctness; deeper business checks belong to later stages).

`check_staging_gold(df)` raises `DataQualityError` on:
- NULL in key columns (`date`, `source`); duplicate `(date, source)`; `date` not monotonically
  increasing **within each source** (checked per `source` group, since multiple sources interleave).
- OHLC logic violation (`high < low`, or `close` outside `[low, high]`) on non-outlier rows.

`check_staging_macro(df)` raises `DataQualityError` on:
- NULL in key columns (`date`, `series_id`); duplicate `(date, series_id)`.
- **Point-in-time invariant:** any row with non-NULL `release_date` where `release_date > date`
  (a future-dated value leaking into the present) — this is the core leakage guard.
- **Cold-start co-null consistency:** every row must be in exactly one of two states — both `value`
  and `release_date` NULL (no release published yet), or both NOT NULL. A half-populated row (`value`
  present but `release_date` missing, or vice versa) raises. `days_stale`/`is_imputed` must follow
  `release_date`: NULL iff `release_date` is NULL.
- `days_stale < 0` for any non-NULL row.

`is_outlier` / `is_anomaly` are flags, NOT gate failures — flagged rows still pass and get written.

## 9. Orchestrator (`run.py`)

`run_preprocessing(engine, gold_reader, macro_reader) -> dict[str, int]` with injectable reader seams
for testing (mirrors Stage 1's `run_ingestion`):

1. `run_migrations(engine, db/migrations)` (applies `001` + `002`; idempotent).
2. Read `raw.gold_prices`, `raw.macro_indicators` via `db.reader.read_table`.
3. `trading_days = calendar.trading_days(gold_df)`.
4. `gold_staged = clean_gold(gold_df)`; `check_staging_gold(gold_staged)`.
5. `macro_staged = concat(align_macro_series(s, trading_days) for s in raw macro series)`;
   `check_staging_macro(macro_staged)`.
6. `upsert_dataframe` into `staging.gold_prices` (`pk=[date, source]`) and `staging.macro_aligned`
   (`pk=[date, series_id]`). Return rows-written per table.

`main()` wires the real engine from `Settings`. CLI entrypoint:
`python -m gold_pipeline.preprocessing.run` (src-layout; never `python src/.../run.py`).

## 10. Testing strategy

Mirrors Stage 1's split — fast unit path is DB-free.

- **Unit (pure pandas, no DB) — `tests/preprocessing/`:**
  - `test_calendar.py` — dedup/sort; calendar derives from gold dates only.
  - `test_clean_gold.py` — `log_return` correctness; **per-source isolation** (two sources don't bleed
    across the `groupby` boundary in `shift`/rolling); robust outlier flag; **flat-return window
    (MAD=0) does not crash and is not flagged** (ε guard); **spike-then-revert does NOT flag `t+1`**;
    same-sign consecutive moves both flagged.
  - `test_align_macro.py` — `merge_asof` backward picks latest release `<= T`; CPI carried forward with
    `is_imputed`/`days_stale`; leading rows co-NULL (`value` and `release_date` both NULL); **every row
    satisfies `release_date <= date`**; `is_anomaly` bounds + staleness.
  - `test_quality.py` — each gate raises on its violation, passes on clean data; PIT invariant raises
    when `release_date > date`; **co-null consistency raises on a half-populated row** (value without
    release_date and vice versa); per-source monotonic check.
- **Integration (Postgres `gold_test`) — `tests/db/`:**
  - `test_writer.py` (moved) — UPSERT idempotency + update-on-conflict.
  - `test_reader.py` — `read_table` round-trips a written frame.

## 11. Invariants honored (traceability to CLAUDE.md)

- **Point-in-time correctness** — macro reindexed on `release_date` via `merge_asof(backward)`; DQ
  asserts `release_date <= date` for every row.
- **No look-ahead / forward-fill only** — values carried forward only; leading rows stay NULL (never
  `bfill`/`interpolate`); outlier window is trailing/past-only.
- **Flag, don't mutate** — `raw` stays immutable; `staging` marks `is_outlier`/`is_imputed`/
  `is_anomaly` and never rewrites observed values.
- **Idempotency** — composite-PK UPSERT; re-running the same dates yields identical rows.
- **Fail-fast** — any structural DQ violation raises before any write; no partial writes.

## 12. Out of scope (this round)

Wide/pivoted feature matrix · technical indicators · lagged/target features · scaling · train/test
split · news/sentiment · `features`/packaging layers · workflow orchestration. These are Stages 3–4.

## 13. Open assumptions (made explicit)

- Outlier window `21`, threshold `k = 5`, MAD floor `ε = 1e-8`, and the macro bounds/staleness ceilings
  in §7a are starting defaults, tunable later; they are flags, so changing them never corrupts values.
- All gold time-series math is grouped by `source`; today only `GC=F` exists, but the grouping is the
  cheap, correct default that keeps a future `XAU/USD` source from contaminating returns/outliers.
- The trading calendar is exactly the set of dates present in `raw.gold_prices` (no external exchange
  calendar dependency).
- Stage 2 reads the full `raw` history each run and UPSERTs; incremental/windowed runs are a later
  optimization, not needed now (idempotency makes a full re-run safe).
