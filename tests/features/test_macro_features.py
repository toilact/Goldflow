import pandas as pd
import pytest
from gold_pipeline.features.macro_features import build_macro_wide


def _long():
    # Two trading days, two series.
    rows = []
    for d in ["2020-01-01", "2020-01-02"]:
        rows.append({"date": d, "series_id": "DGS10", "value": 1.5,
                     "release_date": "2019-12-31", "is_imputed": True,
                     "days_stale": 2, "is_anomaly": False})
        rows.append({"date": d, "series_id": "DTWEXBGS", "value": 120.0,
                     "release_date": "2020-01-01", "is_imputed": False,
                     "days_stale": 0, "is_anomaly": False})
        rows.append({"date": d, "series_id": "CPIAUCSL", "value": 260.0,
                     "release_date": "2019-12-15", "is_imputed": True,
                     "days_stale": 18, "is_anomaly": False})
    return pd.DataFrame(rows)


def test_pivots_values_to_lowercased_columns():
    wide = build_macro_wide(_long())
    assert wide.loc[wide["date"] == pd.Timestamp("2020-01-01"), "dgs10"].iloc[0] == 1.5
    assert "dtwexbgs" in wide.columns and "cpiaucsl" in wide.columns
    assert len(wide) == 2  # one row per date


def test_carries_confidence_flags():
    wide = build_macro_wide(_long())
    # Use iloc to avoid boolean indexing bus error on pandas 3.0.4
    # First row is 2020-01-01 after sorted()
    row = wide.iloc[0]
    assert bool(row["dgs10_is_imputed"]) is True
    assert bool(row["dtwexbgs_is_imputed"]) is False
    assert bool(row["cpiaucsl_is_anomaly"]) is False

    # Assert boolean dtype to catch float-coercion regressions
    assert pd.api.types.is_bool_dtype(wide["dgs10_is_imputed"])
    assert pd.api.types.is_bool_dtype(wide["dgs10_is_anomaly"])


def test_duplicate_key_raises():
    df = _long()
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate"):
        build_macro_wide(df)


def test_missing_series_raises_clearly():
    df = _long()
    df = df[df["series_id"] != "CPIAUCSL"]  # drop an expected macro series
    with pytest.raises(ValueError, match="CPIAUCSL"):
        build_macro_wide(df)
