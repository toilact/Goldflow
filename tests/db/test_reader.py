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
