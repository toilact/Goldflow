"""Fail-fast data-quality gate for the staging layer (structural correctness only).

Flags (is_outlier / is_imputed / is_anomaly) are NOT failures — flagged rows pass
and get written. Deeper business checks belong to later stages.
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


def check_staging_gold(df: pd.DataFrame) -> None:
    _require_no_nulls(df, ["date", "source"])
    _require_no_duplicates(df, ["date", "source"])
    for source, g in df.groupby("source"):
        if not g["date"].is_monotonic_increasing:
            raise DataQualityError(f"date not monotonically increasing for source {source}")
    chk = df[~df["is_outlier"].astype(bool)]
    bad = chk[(chk["high"] < chk["low"]) | (chk["close"] > chk["high"]) | (chk["close"] < chk["low"])]
    if not bad.empty:
        raise DataQualityError(f"{len(bad)} non-outlier rows violate OHLC logic")


def check_staging_macro(df: pd.DataFrame) -> None:
    _require_no_nulls(df, ["date", "series_id"])
    _require_no_duplicates(df, ["date", "series_id"])

    date = pd.to_datetime(df["date"])
    rd = pd.to_datetime(df["release_date"])

    # Point-in-time invariant: a value's release_date must be on or before the day it is visible.
    pit_bad = df[rd.notna() & (rd > date)]
    if not pit_bad.empty:
        raise DataQualityError(f"{len(pit_bad)} rows have release_date > date (look-ahead)")

    # Cold-start co-null consistency: value and release_date both NULL, or both NOT NULL.
    v_null = df["value"].isna()
    r_null = df["release_date"].isna()
    half = df[v_null != r_null]
    if not half.empty:
        raise DataQualityError(
            f"{len(half)} rows half-populated: value and release_date must be both NULL or both set"
        )

    # days_stale must follow release_date (NULL iff release_date NULL) and be non-negative.
    ds_null = df["days_stale"].isna()
    if (ds_null != r_null).any():
        raise DataQualityError("days_stale must be NULL iff release_date is NULL")
    stale = pd.to_numeric(df["days_stale"], errors="coerce")
    if (stale.notna() & (stale < 0)).any():
        raise DataQualityError("days_stale must be >= 0")
