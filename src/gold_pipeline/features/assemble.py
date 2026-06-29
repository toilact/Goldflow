"""Assemble the wide feature table: gold features + macro + targets + readiness flags.

Order of ops matters: technical (adds rsi_14) BEFORE lagged (lags rsi_14).
All date columns are normalized to tz-naive datetime64 before the macro merge so
a DATE-vs-Timestamp dtype mismatch can't silently empty the join.
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

    # Normalize macro date to ISO string before pivot to avoid a pandas Bus error
    # on Python 3.13/macOS when pivot_table processes a datetime64 index directly.
    # build_macro_wide internally calls pd.to_datetime so the output is datetime64.
    macro_input = macro_df.copy()
    # Use strftime via apply to avoid pd.to_datetime crash on datetime64[us] in pytest.
    d = macro_input["date"]
    if hasattr(d, "dt"):
        macro_input["date"] = d.dt.strftime("%Y-%m-%d")
    else:
        macro_input["date"] = d.astype(str)
    macro_wide = build_macro_wide(macro_input)
    df = df.merge(macro_wide, on="date", how="left")

    df["has_features"] = df[warmup_columns(cfg)].notna().all(axis=1)
    for h in cfg.horizons:
        df[f"has_target_{h}"] = df[f"target_logret_{h}"].notna()

    return df[feature_table_columns(cfg)].sort_values(["source", "date"]).reset_index(drop=True)
