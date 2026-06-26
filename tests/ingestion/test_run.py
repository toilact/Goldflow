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
