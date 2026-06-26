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

    def get_series(self, series_id):
        idx = pd.to_datetime(["2020-05-01", "2020-06-01"])
        return pd.Series([2.5, 2.8], index=idx)

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
        def get_series(self, _):
            return pd.Series(dtype=float)
    with pytest.raises(ValueError, match="Empty"):
        fetch_fred_series("DGS10", client=Empty())

def test_fetch_fred_daily_drops_nan_holidays():
    # Daily series carry NaN on market holidays; those must not become NULL-value rows.
    class NanFred:
        def get_series(self, _):
            idx = pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"])
            return pd.Series([1.5, float("nan"), 1.7], index=idx)
    df = fetch_fred_series("DGS10", client=NanFred())
    assert len(df) == 2  # the NaN holiday row is dropped
    assert df["value"].notna().all()
    # next-day release approximation
    assert df.iloc[0]["release_date"] == pd.Timestamp("2020-01-02")

def test_fetch_fred_all_daily_no_nan_passes():
    # A daily series with no NaN keeps every observation.
    class CleanFred:
        def get_series(self, _):
            idx = pd.to_datetime(["2020-01-02", "2020-01-03"])
            return pd.Series([1.5, 1.6], index=idx)
    df = fetch_fred_series("DTWEXBGS", client=CleanFred())
    assert len(df) == 2
    assert list(df.columns) == ["date", "series_id", "value", "release_date"]

def test_macro_series_constant():
    assert MACRO_SERIES == ("DGS10", "DTWEXBGS", "CPIAUCSL")
