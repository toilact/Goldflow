# Stage 1 — Ingestion (Gold + Macro) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fetch gold prices (yfinance) and macro indicators (FRED) and persist them idempotently into a PostgreSQL `raw` schema running in Docker.

**Architecture:** A `src`-layout Python package `gold_pipeline`. Each source returns a normalized pandas DataFrame; a shared `http` module provides retry + rate-limit; a `raw_writer` does Postgres `ON CONFLICT` UPSERT; `run.py` orchestrates source → quality check → write. Per-folder `CLAUDE.md` files document boundaries for handoff.

**Tech Stack:** Python 3.11+, yfinance, fredapi, pandas, numpy, SQLAlchemy, psycopg2-binary, tenacity, python-dotenv, pytest; PostgreSQL 16 via Docker Compose.

## Global Constraints

- Package import root is `gold_pipeline` under `src/`; always run via `python -m gold_pipeline.ingestion.run`, never `python src/.../run.py`.
- Install editable before running/testing: `pip install -e ".[dev]"`.
- Point-in-time: macro rows MUST carry a `release_date` derived from FRED `get_series_all_releases` (earliest `realtime_start` per observation). Stage 1 only stores it; never join/shift on it.
- Idempotency: all writes use composite PK UPSERT (`raw.gold_prices` PK `(date, source)`, `raw.macro_indicators` PK `(date, series_id)`). Re-running must not duplicate rows.
- Fail-fast: any data-quality check failure or empty source raises and aborts before any DB write. No partial writes.
- Tests requiring a DB use the Postgres `gold_test` database (created by init script), never SQLite.
- Normalized columns — gold: `date, open, high, low, close, volume, source`; macro: `date, series_id, value, release_date`.

---

## File Structure

| File | Responsibility |
|---|---|
| `pyproject.toml` | Package config (`src` layout), deps, `[dev]` extra |
| `.env.example` / `.gitignore` | Config template; ignore `.env`, caches |
| `docker-compose.yml` | Postgres service + init-script mount |
| `db/init/01_create_test_db.sql` | Create `gold_test` DB on first container init |
| `db/migrations/001_raw_schema.sql` | `raw` schema + two tables (idempotent) |
| `db/CLAUDE.md` | Migration conventions |
| `src/gold_pipeline/__init__.py` | Package marker |
| `src/gold_pipeline/ingestion/__init__.py` | Subpackage marker |
| `src/gold_pipeline/ingestion/config.py` | `Settings` dataclass from `.env` |
| `src/gold_pipeline/ingestion/http.py` | `with_retry`, `rate_limited` decorators |
| `src/gold_pipeline/ingestion/quality.py` | Minimal DQ checks before write |
| `src/gold_pipeline/ingestion/sources/gold_prices.py` | yfinance fetch + normalize |
| `src/gold_pipeline/ingestion/sources/macro_fred.py` | FRED fetch + release_date |
| `src/gold_pipeline/ingestion/storage/raw_writer.py` | Postgres UPSERT writer + migration runner |
| `src/gold_pipeline/ingestion/run.py` | CLI orchestrator |
| `src/gold_pipeline/ingestion/CLAUDE.md` etc. | Per-folder handoff docs |
| `tests/ingestion/test_*.py` | Unit (no DB) + integration (Postgres) tests |

---

## Task 1: Project skeleton & packaging

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `.env.example`
- Create: `src/gold_pipeline/__init__.py`, `src/gold_pipeline/ingestion/__init__.py`, `src/gold_pipeline/ingestion/sources/__init__.py`, `src/gold_pipeline/ingestion/storage/__init__.py`
- Create: `tests/__init__.py`, `tests/ingestion/__init__.py`

**Interfaces:**
- Consumes: nothing.
- Produces: importable package `gold_pipeline`; `pip install -e ".[dev]"` works.

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "gold-pipeline"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "yfinance>=0.2.40",
    "fredapi>=0.5.2",
    "pandas>=2.2",
    "numpy>=1.26",
    "SQLAlchemy>=2.0",
    "psycopg2-binary>=2.9",
    "tenacity>=8.3",
    "python-dotenv>=1.0",
]

[project.optional-dependencies]
dev = ["pytest>=8.2"]

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 2: Write `.gitignore`**

```gitignore
.env
__pycache__/
*.pyc
.pytest_cache/
*.egg-info/
.venv/
```

- [ ] **Step 3: Write `.env.example`**

```
FRED_API_KEY=
DATABASE_URL=postgresql+psycopg2://gold:gold@localhost:5432/gold
TEST_DATABASE_URL=postgresql+psycopg2://gold:gold@localhost:5432/gold_test
INGEST_START=2015-01-01
INGEST_END=2025-01-01
```

- [ ] **Step 4: Create empty package marker files**

Create all six `__init__.py` files listed above with no content.

- [ ] **Step 5: Install and verify import**

Run: `pip install -e ".[dev]" && python -c "import gold_pipeline; print('ok')"`
Expected: prints `ok`

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml .gitignore .env.example src tests
git commit -m "chore: scaffold gold_pipeline package"
```

---

## Task 2: Docker Postgres + raw schema migration

**Files:**
- Create: `docker-compose.yml`, `db/init/01_create_test_db.sql`, `db/migrations/001_raw_schema.sql`, `db/CLAUDE.md`

**Interfaces:**
- Consumes: nothing.
- Produces: a running Postgres with databases `gold` and `gold_test`; migration SQL applied to create `raw.gold_prices` and `raw.macro_indicators`.

- [ ] **Step 1: Write `docker-compose.yml`**

```yaml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: gold
      POSTGRES_PASSWORD: gold
      POSTGRES_DB: gold
    ports:
      - "5432:5432"
    volumes:
      - ./db/init:/docker-entrypoint-initdb.d:ro
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U gold"]
      interval: 5s
      timeout: 3s
      retries: 10

volumes:
  pgdata:
```

- [ ] **Step 2: Write `db/init/01_create_test_db.sql`**

```sql
-- Runs once on first container init (empty data volume) to add the test DB.
CREATE DATABASE gold_test;
```

- [ ] **Step 3: Write `db/migrations/001_raw_schema.sql`**

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
    date         DATE NOT NULL,
    series_id    TEXT NOT NULL,
    value        NUMERIC(14,6),
    release_date DATE,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, series_id)
);
```

- [ ] **Step 4: Write `db/CLAUDE.md`**

```markdown
# db/ — database layer

Migrations are plain SQL, numbered `NNN_description.sql`, and MUST be idempotent
(`CREATE ... IF NOT EXISTS`). They are applied by `storage/raw_writer.run_migrations()`,
which executes every file in `migrations/` in filename order on each ingestion run.

`init/` scripts run ONLY by the postgres container on first init of an empty data volume —
used here to create the `gold_test` database. After editing `init/`, recreate with
`docker compose down -v && docker compose up -d`.

Schemas map to pipeline layers: `raw` (immutable source data) now; `staging`, `features` later.
```

- [ ] **Step 5: Bring up Postgres and verify both DBs exist**

Run:
```bash
docker compose up -d
sleep 8
docker compose exec -T db psql -U gold -lqt | cut -d'|' -f1 | grep -qw gold_test && echo "gold_test ok"
```
Expected: prints `gold_test ok`

- [ ] **Step 6: Commit**

```bash
git add docker-compose.yml db/
git commit -m "feat: postgres compose + raw schema migration"
```

---

## Task 3: Config loader

**Files:**
- Create: `src/gold_pipeline/ingestion/config.py`
- Test: `tests/ingestion/test_config.py`

**Interfaces:**
- Consumes: environment variables / `.env`.
- Produces: `Settings` frozen dataclass with fields `fred_api_key: str`, `database_url: str`, `test_database_url: str`, `ingest_start: str`, `ingest_end: str`; classmethod `Settings.from_env(environ: Mapping | None = None) -> Settings`.

- [ ] **Step 1: Write the failing test**

```python
# tests/ingestion/test_config.py
import pytest
from gold_pipeline.ingestion.config import Settings

def test_from_env_reads_values():
    env = {
        "FRED_API_KEY": "abc",
        "DATABASE_URL": "postgresql+psycopg2://u:p@h:5432/gold",
        "TEST_DATABASE_URL": "postgresql+psycopg2://u:p@h:5432/gold_test",
        "INGEST_START": "2020-01-01",
        "INGEST_END": "2021-01-01",
    }
    s = Settings.from_env(env)
    assert s.fred_api_key == "abc"
    assert s.ingest_start == "2020-01-01"

def test_from_env_missing_required_raises():
    with pytest.raises(ValueError, match="FRED_API_KEY"):
        Settings.from_env({})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/ingestion/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError` / `ImportError` for `Settings`

- [ ] **Step 3: Write minimal implementation**

```python
# src/gold_pipeline/ingestion/config.py
"""Typed configuration loaded from environment / .env."""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    fred_api_key: str
    database_url: str
    test_database_url: str
    ingest_start: str
    ingest_end: str

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "Settings":
        if environ is None:
            load_dotenv()
            environ = os.environ

        def require(key: str) -> str:
            val = environ.get(key)
            if not val:
                raise ValueError(f"Missing required env var: {key}")
            return val

        return cls(
            fred_api_key=require("FRED_API_KEY"),
            database_url=require("DATABASE_URL"),
            test_database_url=environ.get("TEST_DATABASE_URL", ""),
            ingest_start=environ.get("INGEST_START", "2015-01-01"),
            ingest_end=environ.get("INGEST_END", "2025-01-01"),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/ingestion/test_config.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/ingestion/config.py tests/ingestion/test_config.py
git commit -m "feat: ingestion config loader"
```

---

## Task 4: Shared HTTP retry + rate-limit decorators

**Files:**
- Create: `src/gold_pipeline/ingestion/http.py`
- Test: `tests/ingestion/test_http.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `with_retry(max_attempts: int = 5)` — decorator retrying on `Exception` with exponential backoff (tenacity), re-raising after exhaustion.
  - `rate_limited(min_interval_s: float)` — decorator enforcing a minimum wall-clock gap between successive calls of the wrapped function.

- [ ] **Step 1: Write the failing test**

```python
# tests/ingestion/test_http.py
import time
from gold_pipeline.ingestion.http import with_retry, rate_limited

def test_with_retry_eventually_succeeds():
    calls = {"n": 0}

    @with_retry(max_attempts=3)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("boom")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3

def test_with_retry_reraises_after_exhaustion():
    @with_retry(max_attempts=2)
    def always_fail():
        raise ConnectionError("nope")

    try:
        always_fail()
        assert False, "should have raised"
    except ConnectionError:
        pass

def test_rate_limited_enforces_gap():
    @rate_limited(min_interval_s=0.2)
    def quick():
        return time.monotonic()

    t1 = quick()
    t2 = quick()
    assert t2 - t1 >= 0.2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/ingestion/test_http.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/gold_pipeline/ingestion/http.py
"""Shared resilience helpers: retry with backoff and simple rate limiting.

Every external API call in sources/ should go through these so retry and
rate-limit policy lives in exactly one place.
"""
from __future__ import annotations

import time
from functools import wraps
from typing import Callable

from tenacity import retry, stop_after_attempt, wait_exponential_jitter


def with_retry(max_attempts: int = 5) -> Callable:
    """Retry on any Exception with exponential backoff + jitter (2..30s)."""
    def decorator(fn: Callable) -> Callable:
        wrapped = retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential_jitter(initial=2, max=30),
            reraise=True,
        )(fn)
        return wrapped
    return decorator


def rate_limited(min_interval_s: float) -> Callable:
    """Ensure at least `min_interval_s` between successive calls of the wrapped fn."""
    def decorator(fn: Callable) -> Callable:
        last = {"t": 0.0}

        @wraps(fn)
        def inner(*args, **kwargs):
            elapsed = time.monotonic() - last["t"]
            if elapsed < min_interval_s:
                time.sleep(min_interval_s - elapsed)
            try:
                return fn(*args, **kwargs)
            finally:
                last["t"] = time.monotonic()
        return inner
    return decorator
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/ingestion/test_http.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/ingestion/http.py tests/ingestion/test_http.py
git commit -m "feat: shared retry + rate-limit decorators"
```

---

## Task 5: Data-quality checks

**Files:**
- Create: `src/gold_pipeline/ingestion/quality.py`
- Test: `tests/ingestion/test_quality.py`

**Interfaces:**
- Consumes: normalized DataFrames.
- Produces:
  - `class DataQualityError(Exception)`
  - `check_gold(df: pd.DataFrame) -> None` — raises `DataQualityError` on dup `(date, source)`, non-increasing dates, OHLC violations (`high < low` or `close` outside `[low, high]`), or NULL `date`/`source`.
  - `check_macro(df: pd.DataFrame) -> None` — raises on dup `(date, series_id)` or NULL `date`/`series_id`.

- [ ] **Step 1: Write the failing test**

```python
# tests/ingestion/test_quality.py
import pandas as pd
import pytest
from gold_pipeline.ingestion.quality import check_gold, check_macro, DataQualityError

def _good_gold():
    return pd.DataFrame({
        "date": pd.to_datetime(["2020-01-01", "2020-01-02"]),
        "open": [1, 2], "high": [3, 4], "low": [0.5, 1.5],
        "close": [2, 3], "volume": [10, 20], "source": ["GC=F", "GC=F"],
    })

def test_check_gold_passes_on_valid():
    check_gold(_good_gold())  # no raise

def test_check_gold_flags_bad_ohlc():
    df = _good_gold()
    df.loc[0, "close"] = 99  # close > high
    with pytest.raises(DataQualityError, match="OHLC"):
        check_gold(df)

def test_check_gold_flags_duplicates():
    df = pd.concat([_good_gold().iloc[[0]], _good_gold().iloc[[0]]])
    with pytest.raises(DataQualityError, match="duplicate"):
        check_gold(df)

def test_check_macro_flags_null_key():
    df = pd.DataFrame({"date": [None], "series_id": ["DGS10"], "value": [1.0], "release_date": [None]})
    with pytest.raises(DataQualityError, match="NULL"):
        check_macro(df)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/ingestion/test_quality.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/gold_pipeline/ingestion/quality.py
"""Minimal fail-fast data-quality gate before writing to the raw layer.

Deeper business checks belong to Stage 2; here we only keep structurally
broken data out of raw.
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


def check_gold(df: pd.DataFrame) -> None:
    _require_no_nulls(df, ["date", "source"])
    _require_no_duplicates(df, ["date", "source"])
    if not df["date"].is_monotonic_increasing:
        raise DataQualityError("date column is not monotonically increasing")
    bad = df[(df["high"] < df["low"]) | (df["close"] > df["high"]) | (df["close"] < df["low"])]
    if not bad.empty:
        raise DataQualityError(f"{len(bad)} rows violate OHLC logic")


def check_macro(df: pd.DataFrame) -> None:
    _require_no_nulls(df, ["date", "series_id"])
    _require_no_duplicates(df, ["date", "series_id"])
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/ingestion/test_quality.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/ingestion/quality.py tests/ingestion/test_quality.py
git commit -m "feat: minimal data-quality checks"
```

---

## Task 6: Gold price source (yfinance)

**Files:**
- Create: `src/gold_pipeline/ingestion/sources/gold_prices.py`
- Create: `src/gold_pipeline/ingestion/sources/CLAUDE.md`
- Test: `tests/ingestion/test_sources_gold.py`

**Interfaces:**
- Consumes: `with_retry`, `rate_limited` from `http`.
- Produces: `fetch_gold_prices(start: str, end: str, ticker: str = "GC=F", downloader: Callable | None = None) -> pd.DataFrame` with columns `date, open, high, low, close, volume, source`. `downloader` defaults to `yfinance.download`; injectable for tests. Raises `ValueError` on empty result.

- [ ] **Step 1: Write the failing test**

```python
# tests/ingestion/test_sources_gold.py
import pandas as pd
import pytest
from gold_pipeline.ingestion.sources.gold_prices import fetch_gold_prices

def _fake_yf(*_args, **_kwargs):
    idx = pd.to_datetime(["2020-01-02", "2020-01-03"])
    return pd.DataFrame(
        {"Open": [1, 2], "High": [3, 4], "Low": [0.5, 1.5], "Close": [2, 3], "Volume": [10, 20]},
        index=idx,
    )

def test_fetch_gold_normalizes_columns():
    df = fetch_gold_prices("2020-01-01", "2020-01-04", downloader=_fake_yf)
    assert list(df.columns) == ["date", "open", "high", "low", "close", "volume", "source"]
    assert (df["source"] == "GC=F").all()
    assert len(df) == 2

def test_fetch_gold_empty_raises():
    def empty(*_a, **_k):
        return pd.DataFrame()
    with pytest.raises(ValueError, match="Empty"):
        fetch_gold_prices("2020-01-01", "2020-01-04", downloader=empty)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/ingestion/test_sources_gold.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/gold_pipeline/ingestion/sources/gold_prices.py
"""Gold OHLCV ingestion from Yahoo Finance (ticker GC=F by default)."""
from __future__ import annotations

import logging
from typing import Callable

import pandas as pd

from ..http import rate_limited, with_retry

log = logging.getLogger(__name__)

_COLUMN_MAP = {"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}


@with_retry()
@rate_limited(min_interval_s=1.0)
def fetch_gold_prices(
    start: str,
    end: str,
    ticker: str = "GC=F",
    downloader: Callable | None = None,
) -> pd.DataFrame:
    """Fetch daily gold OHLCV; return normalized columns. Raise ValueError if empty."""
    if downloader is None:
        import yfinance as yf
        downloader = yf.download

    raw = downloader(ticker, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
    if raw is None or raw.empty:
        raise ValueError(f"Empty data for {ticker} ({start}..{end}) — check ticker/date range")

    # yfinance can return a MultiIndex column frame for a single ticker; flatten it.
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    df = raw.rename(columns=_COLUMN_MAP)[list(_COLUMN_MAP.values())].copy()
    df.insert(0, "date", pd.to_datetime(raw.index).tz_localize(None))
    df["source"] = ticker
    return df.reset_index(drop=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/ingestion/test_sources_gold.py -v`
Expected: 2 passed

- [ ] **Step 5: Write `sources/CLAUDE.md`**

```markdown
# sources/ — external data fetchers

Every source is a function returning a NORMALIZED pandas DataFrame. The orchestrator and
writer never learn where data came from.

Contract for a new source:
- Signature: `fetch_<name>(...) -> pd.DataFrame`, with an injectable client/`downloader`
  argument defaulting to the real library (so tests pass a fake — no network in unit tests).
- Decorate external calls with `@with_retry()` and `@rate_limited(...)` from `..http`.
- Raise `ValueError` on empty results (fail-fast on bad ticker/series/date range).
- Normalized columns:
  - gold-style: `date, open, high, low, close, volume, source`
  - macro-style: `date, series_id, value, release_date`

Active sources: `gold_prices.py` (yfinance, ticker `GC=F`),
`macro_fred.py` (FRED series `DGS10`, `DTWEXBGS`, `CPIAUCSL`).

Point-in-time rule: macro sources MUST populate `release_date` from FRED
`get_series_all_releases` (earliest `realtime_start` per observation), never the observation date.
```

- [ ] **Step 6: Commit**

```bash
git add src/gold_pipeline/ingestion/sources/gold_prices.py src/gold_pipeline/ingestion/sources/CLAUDE.md tests/ingestion/test_sources_gold.py
git commit -m "feat: gold price source via yfinance"
```

---

## Task 7: Macro FRED source (with point-in-time release_date)

**Files:**
- Create: `src/gold_pipeline/ingestion/sources/macro_fred.py`
- Test: `tests/ingestion/test_sources_macro.py`

**Interfaces:**
- Consumes: `with_retry`, `rate_limited` from `http`.
- Produces: `fetch_fred_series(series_id: str, client) -> pd.DataFrame` with columns `date, series_id, value, release_date`. `client` is an object exposing `get_series_all_releases(series_id) -> DataFrame[date, realtime_start, value]` (the real `fredapi.Fred`, or a fake in tests). Computes first-release per observation via earliest `realtime_start`. Also: `MACRO_SERIES = ("DGS10", "DTWEXBGS", "CPIAUCSL")`.

- [ ] **Step 1: Write the failing test**

```python
# tests/ingestion/test_sources_macro.py
import pandas as pd
import pytest
from gold_pipeline.ingestion.sources.macro_fred import fetch_fred_series, MACRO_SERIES

class FakeFred:
    def get_series_all_releases(self, series_id):
        # Two vintages for the same observation 2020-05-01: keep the earliest realtime_start.
        return pd.DataFrame({
            "date": pd.to_datetime(["2020-05-01", "2020-05-01", "2020-06-01"]),
            "realtime_start": pd.to_datetime(["2020-06-12", "2020-07-12", "2020-07-12"]),
            "value": [2.5, 2.7, 2.8],
        })

def test_fetch_fred_takes_first_release():
    df = fetch_fred_series("CPIAUCSL", client=FakeFred())
    assert list(df.columns) == ["date", "series_id", "value", "release_date"]
    row = df[df["date"] == pd.Timestamp("2020-05-01")].iloc[0]
    assert row["value"] == 2.5  # earliest realtime_start vintage
    assert row["release_date"] == pd.Timestamp("2020-06-12")
    assert (df["series_id"] == "CPIAUCSL").all()

def test_fetch_fred_empty_raises():
    class Empty:
        def get_series_all_releases(self, _):
            return pd.DataFrame(columns=["date", "realtime_start", "value"])
    with pytest.raises(ValueError, match="Empty"):
        fetch_fred_series("DGS10", client=Empty())

def test_macro_series_constant():
    assert MACRO_SERIES == ("DGS10", "DTWEXBGS", "CPIAUCSL")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/ingestion/test_sources_macro.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/gold_pipeline/ingestion/sources/macro_fred.py
"""Macro indicator ingestion from FRED, preserving point-in-time release dates.

fred.get_series() omits the release date, which Stage 2 needs to avoid look-ahead
leakage (e.g. CPI is published ~2 weeks after its reference month). We therefore use
get_series_all_releases() and keep the EARLIEST realtime_start per observation date.
"""
from __future__ import annotations

import logging

import pandas as pd

from ..http import rate_limited, with_retry

log = logging.getLogger(__name__)

MACRO_SERIES = ("DGS10", "DTWEXBGS", "CPIAUCSL")


@with_retry()
@rate_limited(min_interval_s=0.5)
def fetch_fred_series(series_id: str, client) -> pd.DataFrame:
    """Return normalized macro rows with the first-release date per observation."""
    raw = client.get_series_all_releases(series_id)
    if raw is None or len(raw) == 0:
        raise ValueError(f"Empty data for FRED series {series_id}")

    raw = raw.copy()
    raw["date"] = pd.to_datetime(raw["date"])
    raw["realtime_start"] = pd.to_datetime(raw["realtime_start"])

    first = (
        raw.sort_values("realtime_start")
        .groupby("date", as_index=False)
        .first()
    )
    out = pd.DataFrame({
        "date": first["date"],
        "series_id": series_id,
        "value": first["value"],
        "release_date": first["realtime_start"],
    })
    return out.sort_values("date").reset_index(drop=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/ingestion/test_sources_macro.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/ingestion/sources/macro_fred.py tests/ingestion/test_sources_macro.py
git commit -m "feat: FRED macro source with point-in-time release_date"
```

---

## Task 8: Raw writer (Postgres UPSERT) + migration runner

**Files:**
- Create: `src/gold_pipeline/ingestion/storage/raw_writer.py`
- Create: `src/gold_pipeline/ingestion/storage/CLAUDE.md`
- Test: `tests/ingestion/test_raw_writer.py`

**Interfaces:**
- Consumes: a SQLAlchemy `Engine`, normalized DataFrames.
- Produces:
  - `run_migrations(engine, migrations_dir: Path) -> None` — execute each `*.sql` in filename order.
  - `upsert_dataframe(engine, df: pd.DataFrame, table: str, schema: str, pk: list[str]) -> int` — INSERT ... ON CONFLICT (pk) DO UPDATE for all non-pk columns; returns row count written.

**Test note:** integration test — requires the Postgres `gold_test` DB (`docker compose up -d`). Reads `TEST_DATABASE_URL`; skips if unset.

- [ ] **Step 1: Write the failing test**

```python
# tests/ingestion/test_raw_writer.py
import os
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from gold_pipeline.ingestion.storage.raw_writer import run_migrations, upsert_dataframe

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

def _row():
    return pd.DataFrame({
        "date": pd.to_datetime(["2020-01-02"]), "open": [1.0], "high": [3.0],
        "low": [0.5], "close": [2.0], "volume": [10], "source": ["GC=F"],
    })

def test_upsert_is_idempotent(engine):
    upsert_dataframe(engine, _row(), "gold_prices", "raw", ["date", "source"])
    upsert_dataframe(engine, _row(), "gold_prices", "raw", ["date", "source"])
    with engine.begin() as c:
        n = c.execute(text("SELECT count(*) FROM raw.gold_prices")).scalar()
    assert n == 1

def test_upsert_updates_value(engine):
    upsert_dataframe(engine, _row(), "gold_prices", "raw", ["date", "source"])
    changed = _row()
    changed.loc[0, "close"] = 9.0
    upsert_dataframe(engine, changed, "gold_prices", "raw", ["date", "source"])
    with engine.begin() as c:
        close = c.execute(text("SELECT close FROM raw.gold_prices")).scalar()
    assert float(close) == 9.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `docker compose up -d && sleep 5 && TEST_DATABASE_URL="postgresql+psycopg2://gold:gold@localhost:5432/gold_test" pytest tests/ingestion/test_raw_writer.py -v`
Expected: FAIL with `ImportError` for `raw_writer`

- [ ] **Step 3: Write minimal implementation**

```python
# src/gold_pipeline/ingestion/storage/raw_writer.py
"""Idempotent writer for the raw layer using Postgres ON CONFLICT UPSERT."""
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

- [ ] **Step 4: Run test to verify it passes**

Run: `TEST_DATABASE_URL="postgresql+psycopg2://gold:gold@localhost:5432/gold_test" pytest tests/ingestion/test_raw_writer.py -v`
Expected: 2 passed

- [ ] **Step 5: Write `storage/CLAUDE.md`**

```markdown
# storage/ — raw-layer persistence

`raw_writer.py` is the ONLY way data reaches the database. It never deletes-then-inserts;
it UPSERTs via Postgres `INSERT ... ON CONFLICT (pk) DO UPDATE`, so re-running ingestion for
the same dates is idempotent.

- `run_migrations(engine, migrations_dir)` applies `db/migrations/*.sql` in order; safe to call
  every run because migrations are idempotent.
- `upsert_dataframe(engine, df, table, schema, pk)` — pk is the composite key:
  `["date", "source"]` for `gold_prices`, `["date", "series_id"]` for `macro_indicators`.

The UPSERT uses `sqlalchemy.dialects.postgresql.insert` — Postgres-only by design. Tests run
against the `gold_test` Postgres DB, never SQLite (the dialect would not match).
```

- [ ] **Step 6: Commit**

```bash
git add src/gold_pipeline/ingestion/storage/raw_writer.py src/gold_pipeline/ingestion/storage/CLAUDE.md tests/ingestion/test_raw_writer.py
git commit -m "feat: idempotent raw-layer Postgres writer"
```

---

## Task 9: Orchestrator CLI + ingestion CLAUDE.md

**Files:**
- Create: `src/gold_pipeline/ingestion/run.py`
- Create: `src/gold_pipeline/ingestion/CLAUDE.md`
- Test: `tests/ingestion/test_run.py`

**Interfaces:**
- Consumes: everything above.
- Produces: `run_ingestion(settings, gold_fetcher, fred_client, engine) -> dict[str, int]` (counts per table) and `main()` entrypoint wiring real dependencies. The seam args let `test_run.py` inject fakes without network or DB.

- [ ] **Step 1: Write the failing test**

```python
# tests/ingestion/test_run.py
import pandas as pd
from gold_pipeline.ingestion.run import run_ingestion

class FakeSettings:
    fred_api_key = "x"; ingest_start = "2020-01-01"; ingest_end = "2020-01-05"

def fake_gold(start, end):
    return pd.DataFrame({
        "date": pd.to_datetime(["2020-01-02"]), "open": [1.0], "high": [3.0],
        "low": [0.5], "close": [2.0], "volume": [10], "source": ["GC=F"],
    })

class FakeFred:
    def get_series_all_releases(self, series_id):
        return pd.DataFrame({
            "date": pd.to_datetime(["2020-01-02"]),
            "realtime_start": pd.to_datetime(["2020-01-03"]),
            "value": [1.5],
        })

def test_run_ingestion_writes_each_table(monkeypatch):
    written = {}
    def fake_upsert(engine, df, table, schema, pk):
        written[table] = len(df)
        return len(df)
    import gold_pipeline.ingestion.run as run_mod
    monkeypatch.setattr(run_mod, "upsert_dataframe", fake_upsert)

    counts = run_ingestion(FakeSettings(), gold_fetcher=fake_gold, fred_client=FakeFred(), engine=None)
    assert counts["gold_prices"] == 1
    assert counts["macro_indicators"] == 3  # 3 series concatenated
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/ingestion/test_run.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write minimal implementation**

```python
# src/gold_pipeline/ingestion/run.py
"""Stage 1 orchestrator: fetch sources -> quality-check -> UPSERT into raw.

Run with:  python -m gold_pipeline.ingestion.run
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .config import Settings
from .quality import check_gold, check_macro
from .sources.gold_prices import fetch_gold_prices
from .sources.macro_fred import MACRO_SERIES, fetch_fred_series
from .storage.raw_writer import run_migrations, upsert_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("ingestion.run")

_MIGRATIONS = Path(__file__).resolve().parents[3] / "db" / "migrations"


def run_ingestion(settings, gold_fetcher, fred_client, engine) -> dict[str, int]:
    """Fetch, validate, and UPSERT both sources. Returns rows written per table."""
    gold = gold_fetcher(settings.ingest_start, settings.ingest_end)
    check_gold(gold)

    macro = pd.concat(
        [fetch_fred_series(s, client=fred_client) for s in MACRO_SERIES],
        ignore_index=True,
    )
    check_macro(macro)

    counts = {
        "gold_prices": upsert_dataframe(engine, gold, "gold_prices", "raw", ["date", "source"]),
        "macro_indicators": upsert_dataframe(
            engine, macro, "macro_indicators", "raw", ["date", "series_id"]
        ),
    }
    log.info("ingested %s", counts)
    return counts


def main() -> None:
    from fredapi import Fred
    from sqlalchemy import create_engine

    settings = Settings.from_env()
    engine = create_engine(settings.database_url)
    run_migrations(engine, _MIGRATIONS)
    fred = Fred(api_key=settings.fred_api_key)

    def gold_fetcher(start, end):
        return fetch_gold_prices(start, end)

    run_ingestion(settings, gold_fetcher=gold_fetcher, fred_client=fred, engine=engine)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/ingestion/test_run.py -v`
Expected: 1 passed

- [ ] **Step 5: Write `ingestion/CLAUDE.md`**

```markdown
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
```

- [ ] **Step 6: Run the full suite + commit**

Run: `pytest -q` (integration test auto-skips if `TEST_DATABASE_URL` unset)
Expected: all pass (raw_writer test passes if Postgres up, else skipped)

```bash
git add src/gold_pipeline/ingestion/run.py src/gold_pipeline/ingestion/CLAUDE.md tests/ingestion/test_run.py
git commit -m "feat: ingestion orchestrator CLI"
```

---

## Task 10: End-to-end smoke + README note

**Files:**
- Modify: `CLAUDE.md` (root) — add Stage-1 run commands
- Create: `README.md`

**Interfaces:**
- Consumes: the whole pipeline.
- Produces: documented run path; a real end-to-end run against live APIs (manual, needs FRED key).

- [ ] **Step 1: Add a Commands section to root `CLAUDE.md`**

Append:

```markdown
## Commands (Stage 1)

- Setup: `pip install -e ".[dev]"`
- DB up: `docker compose up -d`
- Run ingestion: `python -m gold_pipeline.ingestion.run`
- Unit tests (no DB): `pytest -q -k "not raw_writer"`
- All tests (DB up): `TEST_DATABASE_URL="postgresql+psycopg2://gold:gold@localhost:5432/gold_test" pytest -q`
- Single test: `pytest tests/ingestion/test_sources_macro.py::test_fetch_fred_takes_first_release -v`
```

- [ ] **Step 2: Write `README.md`**

```markdown
# Gold Data Pipeline — Stage 1 (Ingestion)

Fetches XAU/USD gold prices (yfinance) and macro indicators (FRED: DGS10, DTWEXBGS, CPIAUCSL)
into a PostgreSQL `raw` schema. See `docs/superpowers/specs/` for design, `docs/superpowers/plans/`
for the implementation plan.

## Quickstart
1. `cp .env.example .env` and set `FRED_API_KEY` (free: https://fred.stlouisfed.org/docs/api/api_key.html)
2. `pip install -e ".[dev]"`
3. `docker compose up -d`
4. `python -m gold_pipeline.ingestion.run`

## Test
- `pytest -q -k "not raw_writer"` — fast unit tests, no DB
- `TEST_DATABASE_URL=postgresql+psycopg2://gold:gold@localhost:5432/gold_test pytest -q` — incl. integration
```

- [ ] **Step 3: Manual end-to-end (requires real FRED key + DB up)**

Run:
```bash
docker compose up -d
python -m gold_pipeline.ingestion.run
docker compose exec -T db psql -U gold -d gold -c "SELECT count(*) FROM raw.gold_prices; SELECT series_id, count(*) FROM raw.macro_indicators GROUP BY series_id;"
```
Expected: non-zero gold rows; three macro series with non-zero counts.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "docs: Stage 1 run commands + README"
```

---

## Self-Review

**Spec coverage:**
- §2 directory structure → Tasks 1, 6, 8, 9 create every folder + CLAUDE.md. ✓
- §3 units/interfaces → Tasks 3–9 (config, http, sources, storage, run). ✓
- §4 FRED release_date (`get_series_all_releases`, first release) → Task 7. ✓
- §5 dialect / test parity (Postgres, source tests DB-free) → Tasks 6,7 (no DB) + Task 8 (Postgres). ✓
- §6 packaging / `python -m` → Task 1 (`pyproject.toml`) + Tasks 9,10 (run command). ✓
- §7 raw schema → Task 2. ✓
- §8 error handling (retry, empty raises) → Task 4 + Tasks 6,7. ✓
- §9 minimal DQ checks → Task 5, applied in Task 9. ✓
- §10 config/secrets → Tasks 1, 3. ✓
- §11 dependencies → Task 1. ✓
- Init-script for `gold_test` (added to spec §5) → Task 2. ✓

**Placeholder scan:** No TBD/TODO; every code step shows full code. ✓
**Type consistency:** `upsert_dataframe(engine, df, table, schema, pk)` consistent across Tasks 8, 9; `fetch_fred_series(series_id, client=...)` consistent Tasks 7, 9; `fetch_gold_prices(start, end, ...)` consistent Tasks 6, 9. ✓
```
