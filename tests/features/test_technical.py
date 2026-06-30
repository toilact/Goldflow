import numpy as np
import pandas as pd
import ta
from gold_pipeline.features.config import DEFAULT_CONFIG
from gold_pipeline.features.technical import add_technical


def _gold(closes, source="GC=F", start="2020-01-01"):
    dates = pd.bdate_range(start, periods=len(closes))
    return pd.DataFrame({"date": dates, "close": closes, "source": source})


def test_rsi_matches_ta_reference():
    closes = list(np.linspace(100, 130, 40))
    out = add_technical(_gold(closes))
    ref = ta.momentum.rsi(pd.Series(closes), window=14)
    np.testing.assert_allclose(
        out["rsi_14"].to_numpy(dtype=float), ref.to_numpy(dtype=float), equal_nan=True
    )


def test_ratio_feature_is_close_over_sma():
    closes = list(np.linspace(100, 130, 40))
    out = add_technical(_gold(closes))
    sma10 = ta.trend.sma_indicator(pd.Series(closes), window=10)
    expected = pd.Series(closes) / sma10
    np.testing.assert_allclose(
        out["close_to_sma_10"].to_numpy(dtype=float),
        expected.to_numpy(dtype=float),
        equal_nan=True,
    )


def test_per_source_isolation_no_bleed():
    # Two sources; SMA of source B within the COMBINED frame must equal the
    # SMA computed on source B alone (no bleed from source A across the boundary).
    a = _gold(list(np.linspace(100, 130, 40)), source="GC=F")
    b_closes = list(np.linspace(2000, 2050, 40))
    b = _gold(b_closes, source="XAU/USD")
    out = add_technical(pd.concat([a, b], ignore_index=True))
    # Extract source B rows using iloc (last 40 rows after concat, since B was second)
    b_out = out.iloc[40:].sort_values("date").reset_index(drop=True)
    ref_b = ta.trend.sma_indicator(pd.Series(b_closes), window=10)
    np.testing.assert_allclose(
        b_out["sma_10"].to_numpy(dtype=float), ref_b.to_numpy(dtype=float), equal_nan=True
    )


def test_causality_future_change_does_not_affect_today():
    # Perturbation: changing row t+1 must not change indicators at row t.
    closes = list(np.linspace(100, 130, 40))
    base = add_technical(_gold(closes))
    perturbed_closes = closes.copy()
    perturbed_closes[30] = 999.0  # change the future relative to row 25
    pert = add_technical(_gold(perturbed_closes))
    cols = ["sma_10", "ema_12", "rsi_14", "macd"]
    pd.testing.assert_frame_equal(base.loc[:25, cols], pert.loc[:25, cols])
