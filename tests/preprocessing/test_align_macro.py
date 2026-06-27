import pandas as pd
from gold_pipeline.preprocessing.align_macro import (
    align_macro_series, MACRO_BOUNDS, STALENESS_CEILING,
)


def _trading_days(dates):
    return pd.to_datetime(pd.Series(dates)).sort_values().reset_index(drop=True)


def _cpi(rows):
    # rows: list of (obs_date, value, release_date)
    return pd.DataFrame({
        "date": pd.to_datetime([r[0] for r in rows]),
        "series_id": ["CPIAUCSL"] * len(rows),
        "value": [r[1] for r in rows],
        "release_date": pd.to_datetime([r[2] for r in rows]),
    })


def test_backward_join_picks_latest_release_on_or_before_T():
    series = _cpi([("2020-05-01", 256.0, "2020-05-12"),
                   ("2020-06-01", 257.0, "2020-06-10")])
    days = _trading_days(["2020-05-11", "2020-05-12", "2020-06-09", "2020-06-10"])
    out = align_macro_series(series, days).set_index("date")
    # before first release -> cold start (value & release_date NULL)
    assert pd.isna(out.loc["2020-05-11", "value"])
    assert pd.isna(out.loc["2020-05-11", "release_date"])
    # on/after first release -> 256.0 carried until the June release lands
    assert float(out.loc["2020-05-12", "value"]) == 256.0
    assert float(out.loc["2020-06-09", "value"]) == 256.0
    assert float(out.loc["2020-06-10", "value"]) == 257.0


def test_imputed_and_days_stale():
    series = _cpi([("2020-05-01", 256.0, "2020-05-12")])
    days = _trading_days(["2020-05-12", "2020-05-15"])
    out = align_macro_series(series, days).set_index("date")
    # release day: fresh, not imputed, 0 stale
    assert bool(out.loc["2020-05-12", "is_imputed"]) is False
    assert int(out.loc["2020-05-12", "days_stale"]) == 0
    # 3 days later: carried forward
    assert bool(out.loc["2020-05-15", "is_imputed"]) is True
    assert int(out.loc["2020-05-15", "days_stale"]) == 3


def test_release_date_never_after_date():
    series = _cpi([("2020-05-01", 256.0, "2020-05-12"),
                   ("2020-06-01", 257.0, "2020-06-10")])
    days = _trading_days(["2020-05-12", "2020-05-20", "2020-06-10", "2020-06-30"])
    out = align_macro_series(series, days)
    rd = out["release_date"].dropna()
    assert (rd <= out.loc[rd.index, "date"]).all()


def test_anomaly_out_of_bounds():
    # DGS10 plausible range is in MACRO_BOUNDS; 999 is absurd -> flagged.
    series = pd.DataFrame({
        "date": pd.to_datetime(["2020-05-01"]),
        "series_id": ["DGS10"],
        "value": [999.0],
        "release_date": pd.to_datetime(["2020-05-02"]),
    })
    days = _trading_days(["2020-05-04"])
    out = align_macro_series(series, days)
    assert bool(out.iloc[0]["is_anomaly"]) is True


def test_anomaly_staleness_ceiling():
    series = _cpi([("2020-01-01", 256.0, "2020-01-15")])
    far = (pd.Timestamp("2020-01-15") + pd.Timedelta(days=STALENESS_CEILING["CPIAUCSL"] + 5))
    days = _trading_days(["2020-01-15", far.strftime("%Y-%m-%d")])
    out = align_macro_series(series, days).set_index("date")
    assert bool(out.loc["2020-01-15", "is_anomaly"]) is False
    assert bool(out.loc[far.normalize(), "is_anomaly"]) is True


def test_constants_present():
    assert set(MACRO_BOUNDS) >= {"DGS10", "DTWEXBGS", "CPIAUCSL"}
    assert set(STALENESS_CEILING) >= {"DGS10", "DTWEXBGS", "CPIAUCSL"}
