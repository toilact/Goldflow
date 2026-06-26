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
