"""Per-source technical indicators + stationary ratio features (via `ta`).

`ta` operates on a Series, not a groupby object, so we compute per source group
and assign back by index — no cross-source bleed.
"""
from __future__ import annotations

import pandas as pd
import ta

from .config import DEFAULT_CONFIG, FeatureConfig


def add_technical(gold_df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG) -> pd.DataFrame:
    df = gold_df.copy()
    df = df.sort_values(["source", "date"]).reset_index(drop=True)

    groups = []
    for _src, src_df in df.groupby("source", sort=False):
        src_df = src_df.copy()
        close = src_df["close"].astype(float).reset_index(drop=True)

        for w in cfg.sma_windows:
            src_df[f"sma_{w}"] = ta.trend.sma_indicator(close, window=w).values
        for w in cfg.ema_windows:
            src_df[f"ema_{w}"] = ta.trend.ema_indicator(close, window=w).values
        src_df[f"rsi_{cfg.rsi_window}"] = ta.momentum.rsi(close, window=cfg.rsi_window).values

        fast, slow, sign = cfg.macd
        macd = ta.trend.MACD(close, window_slow=slow, window_fast=fast, window_sign=sign)
        src_df["macd"] = macd.macd().values
        src_df["macd_signal"] = macd.macd_signal().values
        src_df["macd_diff"] = macd.macd_diff().values

        bb = ta.volatility.BollingerBands(close, window=cfg.bb_window, window_dev=cfg.bb_dev)
        src_df["bb_high"] = bb.bollinger_hband().values
        src_df["bb_mid"] = bb.bollinger_mavg().values
        src_df["bb_low"] = bb.bollinger_lband().values

        for w in cfg.ratio_sma_windows:
            sma = ta.trend.sma_indicator(close, window=w).values
            src_df[f"close_to_sma_{w}"] = (close / sma).values

        groups.append(src_df)

    return pd.concat(groups, ignore_index=True)
