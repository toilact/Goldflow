"""Clean staged gold: per-source log-return + robust, flag-only outlier marking.

raw stays immutable — we never rewrite prices, only add `log_return` and an
`is_outlier` flag. All time-series ops are grouped by `source` because the table
is keyed (date, source) and a second source (XAU/USD) is planned.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

OUTLIER_WINDOW = 21      # trailing rolling window (past-only) for the robust z-score
OUTLIER_K = 5.0          # |z| threshold to flag
_MAD_EPS = 1e-8          # guards MAD == 0 (flat returns) against divide-by-zero


def clean_gold(
    gold_df: pd.DataFrame, window: int = OUTLIER_WINDOW, k: float = OUTLIER_K
) -> pd.DataFrame:
    """Add `log_return` and `is_outlier`; sort by (source, date). Never mutate prices."""
    df = gold_df.sort_values(["source", "date"]).reset_index(drop=True)
    if df.duplicated(subset=["date", "source"]).any():
        raise ValueError("duplicate (date, source) rows in gold input")

    df["log_return"] = df.groupby("source")["close"].transform(
        lambda s: np.log(s / s.shift(1))
    )
    df["is_outlier"] = _flag_outliers(df, window, k)
    return df


def _flag_outliers(df: pd.DataFrame, window: int, k: float) -> pd.Series:
    """Per-source robust z-score on log_return; collapse spike-then-revert pairs."""
    flags = pd.Series(False, index=df.index)
    for _src, idx in df.groupby("source").groups.items():
        ret = df.loc[idx, "log_return"]
        # `med` keeps the default full-window warmup, so it stays NaN for the first
        # `window` rows and those rows are never flagged (spec: "first ~21 false").
        # `mad`'s min_periods=1 is LOAD-BEARING, not cosmetic: once `med` goes live the
        # deviation series has only ~`window` recent non-NaN values in the next window,
        # so a default-warmup `mad` would stay NaN for a SECOND window and miss a spike
        # landing just after warmup. Do not "symmetrize" these two rolling calls.
        med = ret.rolling(window).median()
        mad = (ret - med).abs().rolling(window, min_periods=1).median()
        z = 0.6745 * (ret - med) / (mad + _MAD_EPS)
        cand = (z.abs() > k).fillna(False)
        flags.loc[idx] = _collapse_spike_revert(ret, cand)
    return flags


def _collapse_spike_revert(ret: pd.Series, cand: pd.Series) -> pd.Series:
    """If t and t+1 are both candidates with opposite signs, keep only t.

    A single bad price yields a spike at t and a mechanical reversion at t+1
    (opposite sign). Attribute the anomaly to the price event (t) and drop the
    induced flag at t+1. Same-sign consecutive candidates stay (a real 2-day move).
    """
    r = ret.to_numpy()
    c = cand.to_numpy().copy()
    for i in range(len(c) - 1):
        if c[i] and c[i + 1] and np.sign(r[i]) != np.sign(r[i + 1]) and np.sign(r[i]) != 0:
            c[i + 1] = False
    return pd.Series(c, index=cand.index)
