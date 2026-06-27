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


# --- New tests: guard raise branches ---

def test_gold_null_key_raises():
    df = _good_gold()
    df.loc[0, "date"] = None
    with pytest.raises(DataQualityError, match="NULL"):
        check_staging_gold(df)


def test_gold_duplicate_key_raises():
    df = _good_gold()
    df.loc[1, "date"] = df.loc[0, "date"]  # same (date, source) on rows 0 and 1
    with pytest.raises(DataQualityError, match="duplicate"):
        check_staging_gold(df)


def test_macro_null_key_raises():
    df = _good_macro()
    df.loc[0, "series_id"] = None
    with pytest.raises(DataQualityError, match="NULL"):
        check_staging_macro(df)


def test_macro_duplicate_key_raises():
    df = _good_macro()
    df.loc[1, "date"] = df.loc[0, "date"]  # same (date, series_id) on rows 0 and 1
    with pytest.raises(DataQualityError, match="duplicate"):
        check_staging_macro(df)


def test_macro_co_null_other_direction_raises():
    # value is NULL but release_date is NOT NULL — opposite half-populated direction.
    # release_date must be <= date to pass the PIT check first.
    df = _good_macro()
    # Row 0: value NULL, release_date NULL (cold-start). Set release_date non-NULL but keep value NULL.
    df.loc[0, "release_date"] = pd.Timestamp("2020-05-11")  # <= date row 0 (2020-05-11), passes PIT
    # days_stale also needs to match: set non-NULL so it doesn't trip the days_stale guard first.
    # But co-null fires before days_stale guard, so this is fine.
    with pytest.raises(DataQualityError, match="both NULL or both"):
        check_staging_macro(df)


def test_macro_days_stale_not_following_release_date_raises():
    # release_date NOT NULL but days_stale NULL — trips "days_stale must be NULL iff release_date is NULL".
    # Start from the populated row (row 1) which already has value and release_date set; just clear days_stale.
    df = _good_macro()
    df.loc[1, "days_stale"] = None
    with pytest.raises(DataQualityError, match="days_stale"):
        check_staging_macro(df)


def test_macro_negative_days_stale_raises():
    # Valid release_date <= date, value set, but days_stale = -1.
    df = _good_macro()
    df.loc[1, "days_stale"] = -1
    with pytest.raises(DataQualityError, match="days_stale"):
        check_staging_macro(df)
