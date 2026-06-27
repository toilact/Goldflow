# Stage 2 — Preprocessing (Gold + Macro → `staging`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Read the immutable `raw` layer and produce a cleaned, point-in-time-aligned `staging` layer in PostgreSQL — gold prices flagged for outliers, macro series reindexed onto the gold trading calendar without look-ahead.

**Architecture:** A shared `gold_pipeline.db` module (extracted from Stage 1's writer) provides migration + UPSERT + read helpers for every stage. A new `gold_pipeline.preprocessing` package derives the trading calendar from gold's own dates, cleans gold (per-`source` log-return + robust outlier flag), reindexes macro point-in-time via `merge_asof(direction="backward")` on `release_date`, runs a fail-fast DQ gate, and UPSERTs both `staging` tables. Each transform is a pure pandas function with an injectable seam so the fast test path needs no DB.

**Tech Stack:** Python 3.11+, pandas, numpy, SQLAlchemy 2.x, psycopg2-binary; PostgreSQL 16 via Docker Compose; pytest.

## Global Constraints

- Package import root is `gold_pipeline` under `src/`; always run via `python -m gold_pipeline.preprocessing.run`, never `python src/.../run.py`.
- Install editable before running/testing: `pip install -e ".[dev]"`.
- **Point-in-time:** macro is reindexed on `release_date` via `merge_asof(direction="backward")`; the DQ gate asserts `release_date <= date` for every non-NULL row.
- **No look-ahead / forward-fill only:** values are carried forward only; trading days before a series' first release stay `value = NULL` (never `bfill`/`interpolate`); the outlier rolling window is trailing/past-only.
- **Flag, don't mutate:** `raw` stays immutable; `staging` only marks `is_outlier`/`is_imputed`/`is_anomaly` and never rewrites observed values.
- **Per-`source` grouping:** all gold time-series math (`shift`, rolling, spike-revert) is computed within `groupby("source")` — `raw.gold_prices` PK is `(date, source)` and a second source is planned.
- **Idempotency:** all writes use composite-PK UPSERT (`staging.gold_prices` PK `(date, source)`, `staging.macro_aligned` PK `(date, series_id)`). Re-running must not duplicate rows.
- **Fail-fast:** any DQ violation or empty source raises and aborts before any DB write. No partial writes.
- Tests requiring a DB use the Postgres `gold_test` database, never SQLite. They read `TEST_DATABASE_URL` and skip if unset.
- Normalized columns — staging gold: `date, open, high, low, close, volume, log_return, is_outlier, source`; staging macro: `date, series_id, value, release_date, is_imputed, days_stale, is_anomaly`.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/gold_pipeline/db/__init__.py` | Package marker |
| `src/gold_pipeline/db/writer.py` | `run_migrations`, `upsert_dataframe` (moved from `ingestion/storage/raw_writer.py`) |
| `src/gold_pipeline/db/reader.py` | `read_table(engine, schema, table) -> DataFrame` |
| `src/gold_pipeline/db/CLAUDE.md` | Shared DB-layer conventions |
| `src/gold_pipeline/ingestion/run.py` | **Modify** import to `..db.writer` |
| `src/gold_pipeline/ingestion/storage/` | **Delete** (logic + doc moved to `db/`) |
| `db/migrations/002_staging_schema.sql` | `staging` schema + two tables (idempotent) |
| `src/gold_pipeline/preprocessing/__init__.py` | Package marker |
| `src/gold_pipeline/preprocessing/calendar.py` | `trading_days(gold_df)` |
| `src/gold_pipeline/preprocessing/clean_gold.py` | `clean_gold(gold_df)` — log_return + outlier flag |
| `src/gold_pipeline/preprocessing/align_macro.py` | `align_macro_series(series_df, trading_days)` — point-in-time + anomaly |
| `src/gold_pipeline/preprocessing/quality.py` | `check_staging_gold`, `check_staging_macro`, `DataQualityError` |
| `src/gold_pipeline/preprocessing/run.py` | Orchestrator CLI |
| `src/gold_pipeline/preprocessing/CLAUDE.md` | Stage-2 handoff doc |
| `tests/db/test_writer.py` | Moved from `tests/ingestion/test_raw_writer.py` |
| `tests/db/test_reader.py` | Reader round-trip (integration) |
| `tests/preprocessing/test_*.py` | Unit tests (pure pandas, no DB) |

---

## Task 1: Extract shared `db/` module (refactor, no behavior change)

**Files:**
- Create: `src/gold_pipeline/db/__init__.py`, `src/gold_pipeline/db/writer.py`, `src/gold_pipeline/db/CLAUDE.md`
- Modify: `src/gold_pipeline/ingestion/run.py:16`
- Delete: `src/gold_pipeline/ingestion/storage/raw_writer.py`, `src/gold_pipeline/ingestion/storage/__init__.py`, `src/gold_pipeline/ingestion/storage/CLAUDE.md`
- Move: `tests/ingestion/test_raw_writer.py` → `tests/db/test_writer.py`
- Create: `tests/db/__init__.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `gold_pipeline.db.writer.run_migrations(engine, migrations_dir)` and `gold_pipeline.db.writer.upsert_dataframe(engine, df, table, schema, pk) -> int` — identical signatures/behavior to the old `raw_writer`.

- [ ] **Step 1: Create `db/__init__.py` (empty) and `db/writer.py`**

`src/gold_pipeline/db/__init__.py`: empty file.

`src/gold_pipeline/db/writer.py`:
```python
"""Idempotent DB writer shared across pipeline stages (Postgres ON CONFLICT UPSERT)."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


def run_migrations(engine: Engine, migrations_dir: Path) -> None:
    """Apply every *.sql file in filename order. Migrations must be idempotent."""
    for sql_file in sorted(Path(migrations_dir).glob("*.sql")):
        sql = sql_file.read_text()
        with engine.begin() as conn:
            conn.execute(text(sql))
        log.info("applied migration %s", sql_file.name)


def upsert_dataframe(
    engine: Engine, df: pd.DataFrame, table: str, schema: str, pk: list[str]
) -> int:
    """INSERT ... ON CONFLICT (pk) DO UPDATE for non-pk columns. Returns rows written."""
    if df.empty:
        return 0
    meta = MetaData()
    tbl = Table(table, meta, schema=schema, autoload_with=engine)
    records = df.to_dict(orient="records")
    stmt = insert(tbl).values(records)
    update_cols = {c: stmt.excluded[c] for c in df.columns if c not in pk}
    stmt = stmt.on_conflict_do_update(index_elements=pk, set_=update_cols)
    with engine.begin() as conn:
        conn.execute(stmt)
    return len(records)
```

- [ ] **Step 2: Update Stage 1 import**

In `src/gold_pipeline/ingestion/run.py`, change line 16 from:
```python
from .storage.raw_writer import run_migrations, upsert_dataframe
```
to:
```python
from ..db.writer import run_migrations, upsert_dataframe
```

- [ ] **Step 3: Delete the old storage package**

```bash
git rm src/gold_pipeline/ingestion/storage/raw_writer.py \
       src/gold_pipeline/ingestion/storage/__init__.py \
       src/gold_pipeline/ingestion/storage/CLAUDE.md
```

- [ ] **Step 4: Move the writer integration test**

```bash
mkdir -p tests/db && touch tests/db/__init__.py
git mv tests/ingestion/test_raw_writer.py tests/db/test_writer.py
```
Then in `tests/db/test_writer.py` update the import line:
```python
from gold_pipeline.ingestion.storage.raw_writer import run_migrations, upsert_dataframe
```
to:
```python
from gold_pipeline.db.writer import run_migrations, upsert_dataframe
```
The `MIGRATIONS` path in that file is `Path(__file__).resolve().parents[2] / "db" / "migrations"`. The file moved from `tests/ingestion/` to `tests/db/` — both are two levels under the repo root, so `parents[2]` is still the repo root. **Leave the path unchanged.**

- [ ] **Step 5: Write `db/CLAUDE.md`**

```markdown
# db/ — shared database layer (all stages)

The single place data reaches Postgres. Stage-agnostic: ingestion writes `raw`,
preprocessing writes `staging`, both through these helpers.

- `writer.run_migrations(engine, migrations_dir)` — applies `db/migrations/*.sql` in filename
  order; safe every run because migrations are idempotent (`CREATE ... IF NOT EXISTS`).
- `writer.upsert_dataframe(engine, df, table, schema, pk)` — `INSERT ... ON CONFLICT (pk) DO UPDATE`
  for non-pk columns. pk is the composite key, e.g. `["date", "source"]` (gold) or
  `["date", "series_id"]` (macro). Re-running the same rows is idempotent.
- `reader.read_table(engine, schema, table)` — `SELECT *` into a DataFrame.

UPSERT uses `sqlalchemy.dialects.postgresql.insert` — Postgres-only by design. Tests run against
the `gold_test` Postgres DB, never SQLite (the dialect would not match).
```

- [ ] **Step 6: Verify the whole Stage 1 suite still passes (the no-dangling-import gate)**

Run (DB up):
```bash
docker compose up -d && sleep 5
TEST_DATABASE_URL="postgresql+psycopg2://gold:gold@localhost:5432/gold_test" pytest -q
```
Expected: all previously-passing tests pass; `tests/db/test_writer.py` collected at its new path; no `ModuleNotFoundError` for `ingestion.storage`.

Also confirm nothing still references the deleted module:
```bash
grep -rn "ingestion.storage\|raw_writer" src tests
```
Expected: no matches.

- [ ] **Step 7: Commit**

```bash
git add src/gold_pipeline/db tests/db src/gold_pipeline/ingestion/run.py
git commit -m "refactor: extract shared gold_pipeline.db module from ingestion storage"
```

---

## Task 2: Reader helper

**Files:**
- Create: `src/gold_pipeline/db/reader.py`
- Test: `tests/db/test_reader.py`

**Interfaces:**
- Consumes: a SQLAlchemy `Engine`, `gold_pipeline.db.writer.run_migrations` / `upsert_dataframe` (for the test).
- Produces: `read_table(engine, schema: str, table: str) -> pd.DataFrame`.

- [ ] **Step 1: Write the failing test**

`tests/db/test_reader.py`:
```python
import os
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from gold_pipeline.db.writer import run_migrations, upsert_dataframe
from gold_pipeline.db.reader import read_table

TEST_URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not TEST_URL, reason="TEST_DATABASE_URL not set / Postgres not up")

MIGRATIONS = Path(__file__).resolve().parents[2] / "db" / "migrations"


@pytest.fixture
def engine():
    eng = create_engine(TEST_URL)
    run_migrations(eng, MIGRATIONS)
    with eng.begin() as c:
        c.execute(text("TRUNCATE raw.gold_prices"))
    yield eng
    eng.dispose()


def test_read_table_round_trips(engine):
    df = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-02"]), "open": [1.0], "high": [3.0],
        "low": [0.5], "close": [2.0], "volume": [10], "source": ["GC=F"],
    })
    upsert_dataframe(engine, df, "gold_prices", "raw", ["date", "source"])
    out = read_table(engine, "raw", "gold_prices")
    assert len(out) == 1
    assert float(out.iloc[0]["close"]) == 2.0
    assert out.iloc[0]["source"] == "GC=F"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `TEST_DATABASE_URL="postgresql+psycopg2://gold:gold@localhost:5432/gold_test" pytest tests/db/test_reader.py -v`
Expected: FAIL with `ImportError` for `read_table`.

- [ ] **Step 3: Write minimal implementation**

`src/gold_pipeline/db/reader.py`:
```python
"""Read helper for loading whole tables into DataFrames (used by later stages)."""
from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine


def read_table(engine: Engine, schema: str, table: str) -> pd.DataFrame:
    """Return the full contents of `schema.table` as a DataFrame."""
    return pd.read_sql_table(table, engine, schema=schema)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `TEST_DATABASE_URL="postgresql+psycopg2://gold:gold@localhost:5432/gold_test" pytest tests/db/test_reader.py -v`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/db/reader.py tests/db/test_reader.py
git commit -m "feat: db.read_table helper"
```

---

## Task 3: Staging schema migration

**Files:**
- Create: `db/migrations/002_staging_schema.sql`

**Interfaces:**
- Consumes: `run_migrations` (already applies every `*.sql` in order).
- Produces: `staging.gold_prices` (PK `date, source`) and `staging.macro_aligned` (PK `date, series_id`).

- [ ] **Step 1: Write the migration**

`db/migrations/002_staging_schema.sql`:
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
    log_return   NUMERIC(12,8),
    is_outlier   BOOLEAN     NOT NULL DEFAULT false,
    source       TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, source)
);

-- Macro, reindexed point-in-time onto each gold trading day (long-form).
CREATE TABLE IF NOT EXISTS staging.macro_aligned (
    date         DATE NOT NULL,
    series_id    TEXT NOT NULL,
    value        NUMERIC(14,6),
    release_date DATE,
    is_imputed   BOOLEAN NOT NULL DEFAULT false,
    days_stale   INTEGER,
    is_anomaly   BOOLEAN NOT NULL DEFAULT false,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, series_id)
);
```

- [ ] **Step 2: Apply and verify both tables exist**

Run:
```bash
docker compose up -d && sleep 5
docker compose exec -T db psql -U gold -d gold -c "\dt staging.*"
```
Expected: lists `staging.gold_prices` and `staging.macro_aligned`. (Migration is applied by any `run_migrations` call; this manual `psql` apply is optional — the integration tests in later tasks will also create it.)

If you want to apply it directly now:
```bash
docker compose exec -T db psql -U gold -d gold -f /dev/stdin < db/migrations/002_staging_schema.sql
```

- [ ] **Step 3: Commit**

```bash
git add db/migrations/002_staging_schema.sql
git commit -m "feat: staging schema migration (gold_prices + macro_aligned)"
```

---

## Task 4: Trading calendar

**Files:**
- Create: `src/gold_pipeline/preprocessing/__init__.py` (empty), `src/gold_pipeline/preprocessing/calendar.py`
- Test: `tests/preprocessing/__init__.py` (empty), `tests/preprocessing/test_calendar.py`

**Interfaces:**
- Consumes: a gold DataFrame with a `date` column.
- Produces: `trading_days(gold_df: pd.DataFrame) -> pd.Series` — sorted, de-duplicated, tz-naive `datetime64[ns]` Series of distinct gold trading dates, index reset.

- [ ] **Step 1: Write the failing test**

`tests/preprocessing/test_calendar.py`:
```python
import pandas as pd
from gold_pipeline.preprocessing.calendar import trading_days


def test_trading_days_dedups_and_sorts():
    gold = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-03", "2020-01-02", "2020-01-03"]),
        "source": ["GC=F", "GC=F", "XAU/USD"],
    })
    days = trading_days(gold)
    assert list(days) == [pd.Timestamp("2020-01-02"), pd.Timestamp("2020-01-03")]
    assert list(days.index) == [0, 1]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/preprocessing/test_calendar.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Write minimal implementation**

`src/gold_pipeline/preprocessing/calendar.py`:
```python
"""The gold trading calendar — the date backbone every other source aligns to.

Derived purely from the dates present in raw.gold_prices (no external exchange
calendar dependency), so it can never diverge from the data we actually have.
"""
from __future__ import annotations

import pandas as pd


def trading_days(gold_df: pd.DataFrame) -> pd.Series:
    """Distinct gold trading dates, sorted ascending, tz-naive, index reset."""
    days = (
        pd.to_datetime(gold_df["date"])
        .dt.tz_localize(None)
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    return days
```
Note: `.dt.tz_localize(None)` is a no-op when already tz-naive and strips tz when present, keeping the merge keys in Task 6 tz-consistent.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/preprocessing/test_calendar.py -v`
Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/preprocessing/__init__.py src/gold_pipeline/preprocessing/calendar.py tests/preprocessing/__init__.py tests/preprocessing/test_calendar.py
git commit -m "feat: trading-day calendar from gold dates"
```

---

## Task 5: Gold cleaning (log_return + robust outlier flag, per source)

**Files:**
- Create: `src/gold_pipeline/preprocessing/clean_gold.py`
- Test: `tests/preprocessing/test_clean_gold.py`

**Interfaces:**
- Consumes: a gold DataFrame `date, open, high, low, close, volume, source`.
- Produces: `clean_gold(gold_df, window=21, k=5.0) -> pd.DataFrame` with added `log_return` (float) and `is_outlier` (bool), sorted by `(source, date)`, index reset. Raises `ValueError` on duplicate `(date, source)`. All time-series math is per `source`.

- [ ] **Step 1: Write the failing tests**

`tests/preprocessing/test_clean_gold.py`:
```python
import numpy as np
import pandas as pd
import pytest
from gold_pipeline.preprocessing.clean_gold import clean_gold


def _series(closes, source="GC=F", start="2020-01-01"):
    n = len(closes)
    dates = pd.bdate_range(start, periods=n)
    return pd.DataFrame({
        "date": dates,
        "open": closes, "high": [c * 1.01 for c in closes],
        "low": [c * 0.99 for c in closes], "close": closes,
        "volume": [100] * n, "source": [source] * n,
    })


def test_log_return_is_per_source():
    # Two sources interleaved; the first row of EACH source must be NaN, not a
    # cross-source ratio.
    a = _series([10, 11], source="GC=F")
    b = _series([200, 220], source="XAU/USD")
    out = clean_gold(pd.concat([a, b], ignore_index=True))
    first_per_source = out.groupby("source")["log_return"].first()
    assert first_per_source.isna().all()
    gc = out[out["source"] == "GC=F"].reset_index(drop=True)
    assert gc.loc[1, "log_return"] == pytest.approx(np.log(11 / 10))


def test_flat_returns_do_not_crash_or_flag():
    # 30 identical closes -> all log_returns 0 -> rolling MAD == 0. The epsilon
    # floor must prevent inf/NaN and must NOT flag these rows.
    out = clean_gold(_series([100.0] * 30))
    assert out["log_return"].fillna(0).abs().max() == 0.0
    assert not out["is_outlier"].any()


def test_single_spike_flags_t_not_revert_tp1():
    # Calm series then one bad print that reverts next day -> two opposite-sign
    # return anomalies; only the spike day (t) should be flagged.
    closes = [100.0] * 30 + [130.0, 100.0] + [100.0] * 5
    out = clean_gold(_series(closes)).reset_index(drop=True)
    spike_i = 30   # close jumps 100 -> 130
    revert_i = 31  # close reverts 130 -> 100
    assert bool(out.loc[spike_i, "is_outlier"]) is True
    assert bool(out.loc[revert_i, "is_outlier"]) is False


def test_duplicate_key_raises():
    df = _series([1.0, 2.0])
    df.loc[1, "date"] = df.loc[0, "date"]
    with pytest.raises(ValueError, match="duplicate"):
        clean_gold(df)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/preprocessing/test_clean_gold.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Write minimal implementation**

`src/gold_pipeline/preprocessing/clean_gold.py`:
```python
"""Clean staged gold: per-source log-return + robust, flag-only outlier marking.

raw stays immutable — we never rewrite prices, only add `log_return` and an
`is_outlier` flag. All time-series ops are grouped by `source` because the table
is keyed (date, source) and a second source (XAU/USD) is planned.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

OUTLIER_WINDOW = 21      # trailing rolling window (past-only) for the robust z-score
OUTLIER_K = 5.0          # |z| threshold to flag
_MAD_EPS = 1e-8          # guards MAD == 0 (flat returns) against divide-by-zero


def clean_gold(
    gold_df: pd.DataFrame, window: int = OUTLIER_WINDOW, k: float = OUTLIER_K
) -> pd.DataFrame:
    """Add `log_return` and `is_outlier`; sort by (source, date). Never mutate prices."""
    df = gold_df.sort_values(["source", "date"]).reset_index(drop=True)
    if df.duplicated(subset=["date", "source"]).any():
        raise ValueError("duplicate (date, source) rows in gold input")

    df["log_return"] = df.groupby("source")["close"].transform(
        lambda s: np.log(s / s.shift(1))
    )
    df["is_outlier"] = _flag_outliers(df, window, k)
    return df


def _flag_outliers(df: pd.DataFrame, window: int, k: float) -> pd.Series:
    """Per-source robust z-score on log_return; collapse spike-then-revert pairs."""
    flags = pd.Series(False, index=df.index)
    for _src, idx in df.groupby("source").groups.items():
        ret = df.loc[idx, "log_return"]
        med = ret.rolling(window).median()
        mad = (ret - med).abs().rolling(window).median()
        z = 0.6745 * (ret - med) / (mad + _MAD_EPS)
        cand = (z.abs() > k).fillna(False)
        flags.loc[idx] = _collapse_spike_revert(ret, cand)
    return flags


def _collapse_spike_revert(ret: pd.Series, cand: pd.Series) -> pd.Series:
    """If t and t+1 are both candidates with opposite signs, keep only t.

    A single bad price yields a spike at t and a mechanical reversion at t+1
    (opposite sign). Attribute the anomaly to the price event (t) and drop the
    induced flag at t+1. Same-sign consecutive candidates stay (a real 2-day move).
    """
    r = ret.to_numpy()
    c = cand.to_numpy().copy()
    for i in range(len(c) - 1):
        if c[i] and c[i + 1] and np.sign(r[i]) != np.sign(r[i + 1]) and np.sign(r[i]) != 0:
            c[i + 1] = False
    return pd.Series(c, index=cand.index)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/preprocessing/test_clean_gold.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/preprocessing/clean_gold.py tests/preprocessing/test_clean_gold.py
git commit -m "feat: gold cleaning with per-source robust outlier flag"
```

---

## Task 6: Macro point-in-time alignment (+ anomaly flag)

**Files:**
- Create: `src/gold_pipeline/preprocessing/align_macro.py`
- Test: `tests/preprocessing/test_align_macro.py`

**Interfaces:**
- Consumes: a single-series macro DataFrame `date, series_id, value, release_date` (as written to `raw.macro_indicators`); `trading_days` from Task 4.
- Produces: `align_macro_series(series_df: pd.DataFrame, trading_days: pd.Series) -> pd.DataFrame` with columns `date, series_id, value, release_date, is_imputed, days_stale, is_anomaly`, one row per trading day. `series_id` is read from `series_df`. Module constants `MACRO_BOUNDS` and `STALENESS_CEILING`.
- Reconciliation note: schema column `is_imputed` is `NOT NULL`; on cold-start rows (no release yet) it is `False`. `days_stale` is `NULL` on those rows. The co-null invariant in Task 7 is on `value` ↔ `release_date`.

- [ ] **Step 1: Write the failing tests**

`tests/preprocessing/test_align_macro.py`:
```python
import pandas as pd
from gold_pipeline.preprocessing.align_macro import (
    align_macro_series, MACRO_BOUNDS, STALENESS_CEILING,
)


def _trading_days(dates):
    return pd.to_datetime(pd.Series(dates)).sort_values().reset_index(drop=True)


def _cpi(rows):
    # rows: list of (obs_date, value, release_date)
    return pd.DataFrame({
        "date": pd.to_datetime([r[0] for r in rows]),
        "series_id": ["CPIAUCSL"] * len(rows),
        "value": [r[1] for r in rows],
        "release_date": pd.to_datetime([r[2] for r in rows]),
    })


def test_backward_join_picks_latest_release_on_or_before_T():
    series = _cpi([("2020-05-01", 256.0, "2020-05-12"),
                   ("2020-06-01", 257.0, "2020-06-10")])
    days = _trading_days(["2020-05-11", "2020-05-12", "2020-06-09", "2020-06-10"])
    out = align_macro_series(series, days).set_index("date")
    # before first release -> cold start (value & release_date NULL)
    assert pd.isna(out.loc["2020-05-11", "value"])
    assert pd.isna(out.loc["2020-05-11", "release_date"])
    # on/after first release -> 256.0 carried until the June release lands
    assert float(out.loc["2020-05-12", "value"]) == 256.0
    assert float(out.loc["2020-06-09", "value"]) == 256.0
    assert float(out.loc["2020-06-10", "value"]) == 257.0


def test_imputed_and_days_stale():
    series = _cpi([("2020-05-01", 256.0, "2020-05-12")])
    days = _trading_days(["2020-05-12", "2020-05-15"])
    out = align_macro_series(series, days).set_index("date")
    # release day: fresh, not imputed, 0 stale
    assert bool(out.loc["2020-05-12", "is_imputed"]) is False
    assert int(out.loc["2020-05-12", "days_stale"]) == 0
    # 3 days later: carried forward
    assert bool(out.loc["2020-05-15", "is_imputed"]) is True
    assert int(out.loc["2020-05-15", "days_stale"]) == 3


def test_release_date_never_after_date():
    series = _cpi([("2020-05-01", 256.0, "2020-05-12"),
                   ("2020-06-01", 257.0, "2020-06-10")])
    days = _trading_days(["2020-05-12", "2020-05-20", "2020-06-10", "2020-06-30"])
    out = align_macro_series(series, days)
    rd = out["release_date"].dropna()
    assert (rd <= out.loc[rd.index, "date"]).all()


def test_anomaly_out_of_bounds():
    # DGS10 plausible range is in MACRO_BOUNDS; 999 is absurd -> flagged.
    series = pd.DataFrame({
        "date": pd.to_datetime(["2020-05-01"]),
        "series_id": ["DGS10"],
        "value": [999.0],
        "release_date": pd.to_datetime(["2020-05-02"]),
    })
    days = _trading_days(["2020-05-04"])
    out = align_macro_series(series, days)
    assert bool(out.iloc[0]["is_anomaly"]) is True


def test_anomaly_staleness_ceiling():
    series = _cpi([("2020-01-01", 256.0, "2020-01-15")])
    far = (pd.Timestamp("2020-01-15") + pd.Timedelta(days=STALENESS_CEILING["CPIAUCSL"] + 5))
    days = _trading_days(["2020-01-15", far.strftime("%Y-%m-%d")])
    out = align_macro_series(series, days).set_index("date")
    assert bool(out.loc["2020-01-15", "is_anomaly"]) is False
    assert bool(out.loc[far.normalize(), "is_anomaly"]) is True


def test_constants_present():
    assert set(MACRO_BOUNDS) >= {"DGS10", "DTWEXBGS", "CPIAUCSL"}
    assert set(STALENESS_CEILING) >= {"DGS10", "DTWEXBGS", "CPIAUCSL"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/preprocessing/test_align_macro.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Write minimal implementation**

`src/gold_pipeline/preprocessing/align_macro.py`:
```python
"""Reindex a macro series onto the gold trading calendar, point-in-time.

For each trading day T we take the most recently PUBLISHED observation — the row
whose release_date is the greatest value <= T (merge_asof backward on release_date).
Values are carried forward only; days before the first release stay NULL (no
back-fill). Stage 2 only reindexes on the stored release_date; it never shifts further.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# Per-series plausibility bounds (inclusive). Out of range -> is_anomaly.
MACRO_BOUNDS = {
    "DGS10": (-2.0, 25.0),       # 10Y yield, percent
    "DTWEXBGS": (50.0, 200.0),   # broad USD index
    "CPIAUCSL": (0.0, np.inf),   # CPI index level, strictly positive
}

# Max plausible age (days) of the carried value before we suspect a missed release.
STALENESS_CEILING = {
    "DGS10": 7,
    "DTWEXBGS": 7,
    "CPIAUCSL": 45,
}


def align_macro_series(series_df: pd.DataFrame, trading_days: pd.Series) -> pd.DataFrame:
    """Point-in-time reindex one macro series onto `trading_days`."""
    series_id = str(series_df["series_id"].iloc[0])

    left = pd.DataFrame(
        {"date": pd.to_datetime(pd.Series(trading_days)).sort_values().to_numpy()}
    )
    right = (
        series_df[["release_date", "value"]]
        .assign(release_date=pd.to_datetime(series_df["release_date"]))
        .dropna(subset=["release_date"])
        .sort_values("release_date")
        .reset_index(drop=True)
    )

    merged = pd.merge_asof(
        left, right, left_on="date", right_on="release_date", direction="backward"
    )
    merged["series_id"] = series_id

    has = merged["release_date"].notna()
    merged["is_imputed"] = False
    merged.loc[has, "is_imputed"] = (
        merged.loc[has, "release_date"] < merged.loc[has, "date"]
    )
    merged["days_stale"] = pd.Series(pd.NA, index=merged.index, dtype="object")
    merged.loc[has, "days_stale"] = (
        (merged.loc[has, "date"] - merged.loc[has, "release_date"]).dt.days
    )
    merged["is_anomaly"] = _flag_anomaly(merged, series_id)

    return merged[
        ["date", "series_id", "value", "release_date", "is_imputed", "days_stale", "is_anomaly"]
    ]


def _flag_anomaly(merged: pd.DataFrame, series_id: str) -> pd.Series:
    """Flag implausible values or excessive staleness (flag only, never mutate)."""
    lo, hi = MACRO_BOUNDS[series_id]
    ceiling = STALENESS_CEILING[series_id]
    val = merged["value"]
    out_of_range = val.notna() & ((val < lo) | (val > hi))
    stale_days = pd.to_numeric(merged["days_stale"], errors="coerce")
    too_stale = stale_days.notna() & (stale_days > ceiling)
    return (out_of_range | too_stale).fillna(False)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/preprocessing/test_align_macro.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/preprocessing/align_macro.py tests/preprocessing/test_align_macro.py
git commit -m "feat: point-in-time macro alignment with anomaly flag"
```

---

## Task 7: Staging data-quality gate

**Files:**
- Create: `src/gold_pipeline/preprocessing/quality.py`
- Test: `tests/preprocessing/test_quality.py`

**Interfaces:**
- Consumes: staged gold/macro DataFrames (Tasks 5, 6).
- Produces: `class DataQualityError(Exception)`; `check_staging_gold(df) -> None`; `check_staging_macro(df) -> None`. Both raise `DataQualityError` on violation, return `None` on clean data.

- [ ] **Step 1: Write the failing tests**

`tests/preprocessing/test_quality.py`:
```python
import pandas as pd
import pytest
from gold_pipeline.preprocessing.quality import (
    check_staging_gold, check_staging_macro, DataQualityError,
)


def _good_gold():
    return pd.DataFrame({
        "date": pd.to_datetime(["2020-01-02", "2020-01-03"]),
        "open": [1.0, 2.0], "high": [3.0, 4.0], "low": [0.5, 1.5],
        "close": [2.0, 3.0], "volume": [10, 20],
        "log_return": [None, 0.4], "is_outlier": [False, False],
        "source": ["GC=F", "GC=F"],
    })


def _good_macro():
    return pd.DataFrame({
        "date": pd.to_datetime(["2020-05-11", "2020-05-12"]),
        "series_id": ["CPIAUCSL", "CPIAUCSL"],
        "value": [None, 256.0],
        "release_date": pd.to_datetime([None, "2020-05-12"]),
        "is_imputed": [False, False],
        "days_stale": [None, 0],
        "is_anomaly": [False, False],
    })


def test_gold_passes_on_valid():
    check_staging_gold(_good_gold())


def test_gold_flags_non_monotonic_per_source():
    df = _good_gold()
    df.loc[1, "date"] = pd.Timestamp("2019-12-01")  # goes backwards within GC=F
    with pytest.raises(DataQualityError, match="monoton"):
        check_staging_gold(df)


def test_gold_ohlc_violation_on_non_outlier():
    df = _good_gold()
    df.loc[0, "close"] = 99.0  # close > high, and not flagged
    with pytest.raises(DataQualityError, match="OHLC"):
        check_staging_gold(df)


def test_macro_passes_on_valid():
    check_staging_macro(_good_macro())


def test_macro_pit_invariant_raises():
    df = _good_macro()
    df.loc[1, "release_date"] = pd.Timestamp("2020-06-01")  # release after date
    with pytest.raises(DataQualityError, match="release_date"):
        check_staging_macro(df)


def test_macro_co_null_half_populated_raises():
    df = _good_macro()
    df.loc[0, "value"] = 256.0  # value present but release_date still NULL
    with pytest.raises(DataQualityError, match="both NULL or both"):
        check_staging_macro(df)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/preprocessing/test_quality.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Write minimal implementation**

`src/gold_pipeline/preprocessing/quality.py`:
```python
"""Fail-fast data-quality gate for the staging layer (structural correctness only).

Flags (is_outlier / is_imputed / is_anomaly) are NOT failures — flagged rows pass
and get written. Deeper business checks belong to later stages.
"""
from __future__ import annotations

import pandas as pd


class DataQualityError(Exception):
    pass


def _require_no_nulls(df: pd.DataFrame, cols: list[str]) -> None:
    nulls = df[cols].isna().sum()
    bad = nulls[nulls > 0]
    if not bad.empty:
        raise DataQualityError(f"NULL in key columns: {bad.to_dict()}")


def _require_no_duplicates(df: pd.DataFrame, keys: list[str]) -> None:
    n = int(df.duplicated(subset=keys).sum())
    if n:
        raise DataQualityError(f"{n} duplicate rows on keys {keys}")


def check_staging_gold(df: pd.DataFrame) -> None:
    _require_no_nulls(df, ["date", "source"])
    _require_no_duplicates(df, ["date", "source"])
    for source, g in df.groupby("source"):
        if not g["date"].is_monotonic_increasing:
            raise DataQualityError(f"date not monotonically increasing for source {source}")
    chk = df[~df["is_outlier"].astype(bool)]
    bad = chk[(chk["high"] < chk["low"]) | (chk["close"] > chk["high"]) | (chk["close"] < chk["low"])]
    if not bad.empty:
        raise DataQualityError(f"{len(bad)} non-outlier rows violate OHLC logic")


def check_staging_macro(df: pd.DataFrame) -> None:
    _require_no_nulls(df, ["date", "series_id"])
    _require_no_duplicates(df, ["date", "series_id"])

    date = pd.to_datetime(df["date"])
    rd = pd.to_datetime(df["release_date"])

    # Point-in-time invariant: a value's release_date must be on or before the day it is visible.
    pit_bad = df[rd.notna() & (rd > date)]
    if not pit_bad.empty:
        raise DataQualityError(f"{len(pit_bad)} rows have release_date > date (look-ahead)")

    # Cold-start co-null consistency: value and release_date both NULL, or both NOT NULL.
    v_null = df["value"].isna()
    r_null = df["release_date"].isna()
    half = df[v_null != r_null]
    if not half.empty:
        raise DataQualityError(
            f"{len(half)} rows half-populated: value and release_date must be both NULL or both set"
        )

    # days_stale must follow release_date (NULL iff release_date NULL) and be non-negative.
    ds_null = df["days_stale"].isna()
    if (ds_null != r_null).any():
        raise DataQualityError("days_stale must be NULL iff release_date is NULL")
    stale = pd.to_numeric(df["days_stale"], errors="coerce")
    if (stale.notna() & (stale < 0)).any():
        raise DataQualityError("days_stale must be >= 0")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/preprocessing/test_quality.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/preprocessing/quality.py tests/preprocessing/test_quality.py
git commit -m "feat: staging data-quality gate (PIT + co-null invariants)"
```

---

## Task 8: Orchestrator CLI + preprocessing CLAUDE.md

**Files:**
- Create: `src/gold_pipeline/preprocessing/run.py`, `src/gold_pipeline/preprocessing/CLAUDE.md`
- Test: `tests/preprocessing/test_run.py`

**Interfaces:**
- Consumes: `db.reader.read_table`, `db.writer.run_migrations`/`upsert_dataframe`, `calendar.trading_days`, `clean_gold`, `align_macro_series`, `check_staging_gold`/`check_staging_macro`.
- Produces: `run_preprocessing(engine, gold_reader, macro_reader) -> dict[str, int]` (rows written per staging table) and `main()`. `gold_reader()` / `macro_reader()` are zero-arg seams returning the raw gold / raw macro DataFrames, so the test injects fakes without a DB.

- [ ] **Step 1: Write the failing test**

`tests/preprocessing/test_run.py`:
```python
import pandas as pd
from gold_pipeline.preprocessing.run import run_preprocessing


def _raw_gold():
    dates = pd.bdate_range("2020-01-01", periods=3)
    return pd.DataFrame({
        "date": dates, "open": [10, 11, 12], "high": [10.1, 11.1, 12.1],
        "low": [9.9, 10.9, 11.9], "close": [10, 11, 12], "volume": [100, 100, 100],
        "source": ["GC=F"] * 3,
    })


def _raw_macro():
    # two series, one row each, released before the gold dates above
    return pd.DataFrame({
        "date": pd.to_datetime(["2019-12-01", "2019-12-01"]),
        "series_id": ["DGS10", "CPIAUCSL"],
        "value": [1.8, 256.0],
        "release_date": pd.to_datetime(["2019-12-02", "2019-12-15"]),
    })


def test_run_preprocessing_writes_each_table(monkeypatch):
    written = {}

    def fake_upsert(engine, df, table, schema, pk):
        written[table] = len(df)
        return len(df)

    import gold_pipeline.preprocessing.run as run_mod
    monkeypatch.setattr(run_mod, "upsert_dataframe", fake_upsert)

    counts = run_preprocessing(
        engine=None,
        gold_reader=_raw_gold,
        macro_reader=_raw_macro,
    )
    # 3 gold trading days
    assert counts["gold_prices"] == 3
    # 2 series x 3 trading days each, aligned long-form
    assert counts["macro_aligned"] == 6
    assert written["gold_prices"] == 3
    assert written["macro_aligned"] == 6
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/preprocessing/test_run.py -v`
Expected: FAIL with `ImportError`.

- [ ] **Step 3: Write minimal implementation**

`src/gold_pipeline/preprocessing/run.py`:
```python
"""Stage 2 orchestrator: read raw -> clean/align -> quality-check -> UPSERT into staging.

Run with:  python -m gold_pipeline.preprocessing.run
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..db.reader import read_table
from ..db.writer import run_migrations, upsert_dataframe
from ..ingestion.config import Settings
from .align_macro import align_macro_series
from .calendar import trading_days
from .clean_gold import clean_gold
from .quality import check_staging_gold, check_staging_macro

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("preprocessing.run")

_MIGRATIONS = Path(__file__).resolve().parents[3] / "db" / "migrations"


def run_preprocessing(engine, gold_reader, macro_reader) -> dict[str, int]:
    """Transform raw -> staging. gold_reader/macro_reader are zero-arg seams."""
    raw_gold = gold_reader()
    raw_macro = macro_reader()

    gold_staged = clean_gold(raw_gold)
    check_staging_gold(gold_staged)

    days = trading_days(raw_gold)
    macro_staged = pd.concat(
        [
            align_macro_series(g, days)
            for _sid, g in raw_macro.groupby("series_id")
        ],
        ignore_index=True,
    )
    check_staging_macro(macro_staged)

    counts = {
        "gold_prices": upsert_dataframe(
            engine, gold_staged, "gold_prices", "staging", ["date", "source"]
        ),
        "macro_aligned": upsert_dataframe(
            engine, macro_staged, "macro_aligned", "staging", ["date", "series_id"]
        ),
    }
    log.info("preprocessed %s", counts)
    return counts


def main() -> None:
    from sqlalchemy import create_engine

    settings = Settings.from_env()
    engine = create_engine(settings.database_url)
    run_migrations(engine, _MIGRATIONS)

    def gold_reader():
        return read_table(engine, "raw", "gold_prices")

    def macro_reader():
        return read_table(engine, "raw", "macro_indicators")

    run_preprocessing(engine, gold_reader=gold_reader, macro_reader=macro_reader)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/preprocessing/test_run.py -v`
Expected: 1 passed.

- [ ] **Step 5: Write `preprocessing/CLAUDE.md`**

```markdown
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
```

- [ ] **Step 6: Run the full unit suite + commit**

Run: `pytest -q -k "not test_writer and not test_reader"`
Expected: all preprocessing + ingestion unit tests pass (DB-backed tests excluded here).

```bash
git add src/gold_pipeline/preprocessing/run.py src/gold_pipeline/preprocessing/CLAUDE.md tests/preprocessing/test_run.py
git commit -m "feat: Stage 2 preprocessing orchestrator CLI"
```

---

## Task 9: End-to-end smoke + docs

**Files:**
- Modify: `CLAUDE.md` (root) — add Stage-2 run command
- Modify: `README.md` — add Stage-2 note

**Interfaces:**
- Consumes: the whole Stage 2 pipeline + a populated `raw` layer.
- Produces: documented run path; a real end-to-end run writing `staging`.

- [ ] **Step 1: Add Stage-2 commands to root `CLAUDE.md`**

Under the existing `## Commands (Stage 1)` section, append a new section:
```markdown
## Commands (Stage 2)

- Run preprocessing: `python -m gold_pipeline.preprocessing.run` (needs `raw` populated by Stage 1)
- Unit tests (no DB): `pytest -q -k "not test_writer and not test_reader"`
- DB integration tests: `TEST_DATABASE_URL="postgresql+psycopg2://gold:gold@localhost:5432/gold_test" pytest -q tests/db`
```

- [ ] **Step 2: Add a Stage-2 note to `README.md`**

Append:
```markdown
## Stage 2 — Preprocessing (`staging`)

Reads `raw`, cleans gold (per-source log-return + robust outlier flag), and reindexes macro onto the
gold trading calendar point-in-time (`merge_asof` backward on `release_date`). Run after Stage 1:
`python -m gold_pipeline.preprocessing.run`.
```

- [ ] **Step 3: Manual end-to-end (requires `raw` populated + DB up)**

Run:
```bash
docker compose up -d
python -m gold_pipeline.preprocessing.run
docker compose exec -T db psql -U gold -d gold -c "SELECT count(*) FROM staging.gold_prices; SELECT series_id, count(*), sum(is_imputed::int) FROM staging.macro_aligned GROUP BY series_id;"
```
Expected: non-zero gold rows; each macro series spans the full trading calendar with a plausible imputed-row count (CPI mostly imputed, daily series rarely).

- [ ] **Step 4: Full suite green (DB up)**

Run:
```bash
TEST_DATABASE_URL="postgresql+psycopg2://gold:gold@localhost:5432/gold_test" pytest -q
```
Expected: all unit + integration tests pass.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "docs: Stage 2 run commands + README"
```

---

## Self-Review

**Spec coverage:**
- §3 directory structure (shared `db/`, `preprocessing/`, test relocation) → Tasks 1, 2, 4–8. ✓
- §4 shared `db/` refactor + import audit + clean removal of `ingestion/storage` → Task 1. ✓
- §5 staging schema (both tables, `is_anomaly`) → Task 3. ✓
- §6 gold cleaning: per-`source` groupby, `log_return`, robust z-score with `ε`, spike-revert collapse → Task 5. ✓
- §7 macro point-in-time `merge_asof(backward)`, `is_imputed`/`days_stale`, leading NULL → Task 6. ✓
- §7a macro bounds + staleness ceiling → Task 6 (`MACRO_BOUNDS`, `STALENESS_CEILING`). ✓
- §8 DQ gate: PIT invariant, co-null consistency, per-source monotonic, OHLC on non-outliers → Task 7. ✓
- §9 orchestrator with reader seams → Task 8. ✓
- §10 testing split (unit no-DB in `tests/preprocessing/`, integration in `tests/db/`) → Tasks 1, 2, 4–8. ✓
- §11 invariants honored → enforced across Tasks 5–7. ✓
- `db/reader.read_table` → Task 2. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code. ✓

**Type consistency:**
- `run_migrations(engine, migrations_dir)` / `upsert_dataframe(engine, df, table, schema, pk)` consistent across Tasks 1, 2, 8. ✓
- `read_table(engine, schema, table)` consistent Tasks 2, 8. ✓
- `trading_days(gold_df) -> Series` consistent Tasks 4, 6 (as `trading_days` arg), 8. ✓
- `clean_gold(gold_df, window, k)` consistent Tasks 5, 8. ✓
- `align_macro_series(series_df, trading_days)` consistent Tasks 6, 8. ✓
- `check_staging_gold` / `check_staging_macro` consistent Tasks 7, 8. ✓
- `run_preprocessing(engine, gold_reader, macro_reader)` consistent Task 8 + its test. ✓
- Staging columns match the schema in Task 3 across Tasks 5–8. ✓
