import os

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from gold_pipeline.db.writer import run_migrations, upsert_dataframe
from gold_pipeline.features.run import _MIGRATIONS, run_features

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"), reason="needs TEST_DATABASE_URL"
)


def _seed_staging(engine):
    dates = pd.bdate_range("2018-01-01", periods=60)
    closes = list(100 + np.arange(60) * 0.5)
    logret = [np.nan] + list(np.diff(np.log(closes)))
    gold = pd.DataFrame({
        "date": dates, "open": closes, "high": [c * 1.01 for c in closes],
        "low": [c * 0.99 for c in closes], "close": closes, "volume": [100] * 60,
        "log_return": logret, "is_outlier": [False] * 60, "source": "GC=F",
    })
    macro_rows = []
    for d in dates:
        for sid, val in [("DGS10", 1.5), ("DTWEXBGS", 120.0), ("CPIAUCSL", 260.0)]:
            macro_rows.append({"date": d, "series_id": sid, "value": val,
                               "release_date": d, "is_imputed": False,
                               "days_stale": 0, "is_anomaly": False})
    macro = pd.DataFrame(macro_rows)
    upsert_dataframe(engine, gold, "gold_prices", "staging", ["date", "source"])
    upsert_dataframe(engine, macro, "macro_aligned", "staging", ["date", "series_id"])


def test_run_features_idempotent_upsert():
    engine = create_engine(os.environ["TEST_DATABASE_URL"])
    run_migrations(engine, _MIGRATIONS)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE staging.gold_prices, staging.macro_aligned, features.gold_features"))
    _seed_staging(engine)

    def gold_reader():
        from gold_pipeline.db.reader import read_table
        return read_table(engine, "staging", "gold_prices")

    def macro_reader():
        from gold_pipeline.db.reader import read_table
        return read_table(engine, "staging", "macro_aligned")

    first = run_features(engine, gold_reader, macro_reader)
    second = run_features(engine, gold_reader, macro_reader)  # re-run
    assert first["gold_features"] == 60
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT count(*) FROM features.gold_features")).scalar()
    assert rows == 60  # idempotent: no duplication
