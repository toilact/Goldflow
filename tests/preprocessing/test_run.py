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
