"""Assemble the wide feature table: gold features + macro + targets + readiness flags.

Order of ops matters: technical (adds rsi_14) BEFORE lagged (lags rsi_14).
Gold date is normalized to tz-naive datetime64 before the macro merge.
build_macro_wide handles datetime64 dates directly via pd.DatetimeIndex internally.
"""
from __future__ import annotations

import pandas as pd

from .config import DEFAULT_CONFIG, FeatureConfig, feature_table_columns, warmup_columns
from .lagged import add_lagged
from .macro_features import build_macro_wide
from .target import add_targets
from .technical import add_technical


def assemble_features(
    gold_df: pd.DataFrame, macro_df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG
) -> pd.DataFrame:
    gold = gold_df.copy()
    gold["date"] = pd.to_datetime(gold["date"]).dt.tz_localize(None)

    df = add_technical(gold, cfg)   # adds rsi_14 etc.
    df = add_lagged(df, cfg)        # lags rsi_14/log_return
    df = add_targets(df, cfg)

    macro_wide = build_macro_wide(macro_df.copy())
    df = df.merge(macro_wide, on="date", how="left")

    df["has_features"] = df[warmup_columns(cfg)].notna().all(axis=1)
    for h in cfg.horizons:
        df[f"has_target_{h}"] = df[f"target_logret_{h}"].notna()

    return df[feature_table_columns(cfg)].sort_values(["source", "date"]).reset_index(drop=True)
