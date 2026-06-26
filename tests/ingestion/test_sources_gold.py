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
