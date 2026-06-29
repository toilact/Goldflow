import numpy as np
import pandas as pd
from gold_pipeline.features.lagged import add_lagged


def _df(n, source="GC=F", start="2020-01-01", base=0.0):
    dates = pd.bdate_range(start, periods=n)
    return pd.DataFrame({
        "date": dates,
        "source": source,
        "log_return": [base + i for i in range(n)],
        "rsi_14": [50 + i for i in range(n)],
    })


def test_lag_is_past_value():
    out = add_lagged(_df(5)).reset_index(drop=True)
    # logret_lag_1 at row k equals log_return at row k-1
    assert out.loc[2, "logret_lag_1"] == out.loc[1, "log_return"]
    assert np.isnan(out.loc[0, "logret_lag_1"])


def test_no_close_lag_columns():
    out = add_lagged(_df(5))
    assert not any(c.startswith("close_lag") for c in out.columns)


def test_lag_does_not_bleed_across_sources():
    a = _df(4, source="GC=F", base=0.0)
    b = _df(4, source="XAU/USD", base=100.0)
    out = add_lagged(pd.concat([a, b], ignore_index=True))
    # Find rows for source B (indices 4-7 after concat)
    b_rows = [i for i, s in enumerate(out["source"]) if s == "XAU/USD"]
    # First row of source B must be NaN, NOT the last value of source A.
    assert np.isnan(out.iloc[b_rows[0]]["logret_lag_1"])
    assert out.iloc[b_rows[1]]["logret_lag_1"] == out.iloc[b_rows[0]]["log_return"]
