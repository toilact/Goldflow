import numpy as np
import pandas as pd
import pytest
from gold_pipeline.preprocessing.clean_gold import clean_gold


def _series(closes, source="GC=F", start="2020-01-01"):
    n = len(closes)
    dates = pd.bdate_range(start, periods=n)
    return pd.DataFrame({
        "date": dates,
        "open": closes, "high": [c * 1.01 for c in closes],
        "low": [c * 0.99 for c in closes], "close": closes,
        "volume": [100] * n, "source": [source] * n,
    })


def test_log_return_is_per_source():
    # Two sources interleaved; the first row of EACH source must be NaN, not a
    # cross-source ratio.
    a = _series([10, 11], source="GC=F")
    b = _series([200, 220], source="XAU/USD")
    out = clean_gold(pd.concat([a, b], ignore_index=True))
    first_per_source = out.groupby("source", sort=False).head(1)["log_return"]
    assert first_per_source.isna().all()
    gc = out[out["source"] == "GC=F"].reset_index(drop=True)
    assert gc.loc[1, "log_return"] == pytest.approx(np.log(11 / 10))


def test_flat_returns_do_not_crash_or_flag():
    # 30 identical closes -> all log_returns 0 -> rolling MAD == 0. The epsilon
    # floor must prevent inf/NaN and must NOT flag these rows.
    out = clean_gold(_series([100.0] * 30))
    assert out["log_return"].fillna(0).abs().max() == 0.0
    assert not out["is_outlier"].any()


def test_single_spike_flags_t_not_revert_tp1():
    # Calm series then one bad print that reverts next day -> two opposite-sign
    # return anomalies; only the spike day (t) should be flagged.
    closes = [100.0] * 30 + [130.0, 100.0] + [100.0] * 5
    out = clean_gold(_series(closes)).reset_index(drop=True)
    spike_i = 30   # close jumps 100 -> 130
    revert_i = 31  # close reverts 130 -> 100
    assert bool(out.loc[spike_i, "is_outlier"]) is True
    assert bool(out.loc[revert_i, "is_outlier"]) is False


def test_duplicate_key_raises():
    df = _series([1.0, 2.0])
    df.loc[1, "date"] = df.loc[0, "date"]
    with pytest.raises(ValueError, match="duplicate"):
        clean_gold(df)
