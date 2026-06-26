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

def test_check_macro_flags_null_release_date():
    df = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-01"]),
        "series_id": ["DGS10"],
        "value": [1.0],
        "release_date": [None],
    })
    with pytest.raises(DataQualityError, match="NULL"):
        check_macro(df)
