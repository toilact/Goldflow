"""Lagged features — stationary inputs only, shifted per source (no cross-source bleed)."""
from __future__ import annotations

import pandas as pd

from .config import DEFAULT_CONFIG, FeatureConfig, _alias


def add_lagged(df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG) -> pd.DataFrame:
    out = df.sort_values(["source", "date"]).reset_index(drop=True)
    grouped = out.groupby("source")
    for col in cfg.lag_columns:
        for k in cfg.lags:
            out[f"{_alias(col)}_lag_{k}"] = grouped[col].shift(k)
    return out
