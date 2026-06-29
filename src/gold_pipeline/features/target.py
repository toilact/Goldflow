"""Targets — the ONLY place future values appear. Future return per source.

groupby("source") shift(-h) so the tail of one source never borrows the head of
the next source's history (look-ahead at the join boundary).
"""
from __future__ import annotations

import pandas as pd

from .config import DEFAULT_CONFIG, FeatureConfig


def add_targets(df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG) -> pd.DataFrame:
    out = df.sort_values(["source", "date"]).reset_index(drop=True)
    grouped = out.groupby("source")
    for h in cfg.horizons:
        out[f"target_logret_{h}"] = grouped["log_return"].shift(-h)
    return out
