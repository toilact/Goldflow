import numpy as np
import pandas as pd
from gold_pipeline.features.target import add_targets


def _df(n, source="GC=F", start="2020-01-01", base=0.0):
    dates = pd.bdate_range(start, periods=n)
    return pd.DataFrame({
        "date": dates, "source": source,
        "log_return": [base + i for i in range(n)],
    })


def test_target_is_future_return():
    out = add_targets(_df(6)).reset_index(drop=True)
    # target_logret_1 at row k equals log_return at row k+1
    assert out.loc[2, "target_logret_1"] == out.loc[3, "log_return"]
    # tail rows lack a future value
    assert np.isnan(out.loc[5, "target_logret_1"])
    assert np.isnan(out.loc[5, "target_logret_5"])


def test_all_horizons_present():
    out = add_targets(_df(8))
    assert "target_logret_1" in out.columns
    assert "target_logret_5" in out.columns


def test_target_does_not_bleed_across_sources():
    a = _df(4, source="GC=F", base=0.0)
    b = _df(4, source="XAU/USD", base=100.0)
    out = add_targets(pd.concat([a, b], ignore_index=True))
    # Last row of source A (index 3) must be NaN, NOT the first value of source B.
    assert np.isnan(out.loc[3, "target_logret_1"])
