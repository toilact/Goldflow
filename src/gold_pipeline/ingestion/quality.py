"""Minimal fail-fast data-quality gate before writing to the raw layer.

Deeper business checks belong to Stage 2; here we only keep structurally
broken data out of raw.
"""
from __future__ import annotations

import pandas as pd


class DataQualityError(Exception):
    pass


def _require_no_nulls(df: pd.DataFrame, cols: list[str]) -> None:
    nulls = df[cols].isna().sum()
    bad = nulls[nulls > 0]
    if not bad.empty:
        raise DataQualityError(f"NULL in key columns: {bad.to_dict()}")


def _require_no_duplicates(df: pd.DataFrame, keys: list[str]) -> None:
    n = int(df.duplicated(subset=keys).sum())
    if n:
        raise DataQualityError(f"{n} duplicate rows on keys {keys}")


def check_gold(df: pd.DataFrame) -> None:
    _require_no_nulls(df, ["date", "source"])
    _require_no_duplicates(df, ["date", "source"])
    if not df["date"].is_monotonic_increasing:
        raise DataQualityError("date column is not monotonically increasing")
    bad = df[(df["high"] < df["low"]) | (df["close"] > df["high"]) | (df["close"] < df["low"])]
    if not bad.empty:
        raise DataQualityError(f"{len(bad)} rows violate OHLC logic")


def check_macro(df: pd.DataFrame) -> None:
    _require_no_nulls(df, ["date", "series_id", "release_date"])
    _require_no_duplicates(df, ["date", "series_id"])
