"""Reindex a macro series onto the gold trading calendar, point-in-time.

For each trading day T we take the most recently PUBLISHED observation — the row
whose release_date is the greatest value <= T (merge_asof backward on release_date).
Values are carried forward only; days before the first release stay NULL (no
back-fill). Stage 2 only reindexes on the stored release_date; it never shifts further.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# Per-series plausibility bounds (inclusive). Out of range -> is_anomaly.
MACRO_BOUNDS = {
    "DGS10": (-2.0, 25.0),       # 10Y yield, percent
    "DTWEXBGS": (50.0, 200.0),   # broad USD index
    "CPIAUCSL": (0.0, np.inf),   # CPI index level, strictly positive
}

# Max plausible age (days) of the carried value before we suspect a missed release.
STALENESS_CEILING = {
    "DGS10": 7,
    "DTWEXBGS": 7,
    "CPIAUCSL": 45,
}


def align_macro_series(series_df: pd.DataFrame, trading_days: pd.Series) -> pd.DataFrame:
    """Point-in-time reindex one macro series onto `trading_days`."""
    series_id = str(series_df["series_id"].iloc[0])

    left = pd.DataFrame(
        {"date": pd.to_datetime(pd.Series(trading_days)).sort_values().to_numpy()}
    )
    right = (
        series_df[["release_date", "value"]]
        .assign(release_date=pd.to_datetime(series_df["release_date"]))
        .dropna(subset=["release_date"])
        .sort_values("release_date")
        .reset_index(drop=True)
    )

    merged = pd.merge_asof(
        left, right, left_on="date", right_on="release_date", direction="backward"
    )
    merged["series_id"] = series_id

    has = merged["release_date"].notna()
    merged["is_imputed"] = False
    merged.loc[has, "is_imputed"] = (
        merged.loc[has, "release_date"] < merged.loc[has, "date"]
    )
    merged["days_stale"] = pd.Series(pd.NA, index=merged.index, dtype="object")
    merged.loc[has, "days_stale"] = (
        (merged.loc[has, "date"] - merged.loc[has, "release_date"]).dt.days
    )
    merged["is_anomaly"] = _flag_anomaly(merged, series_id)

    return merged[
        ["date", "series_id", "value", "release_date", "is_imputed", "days_stale", "is_anomaly"]
    ]


def _flag_anomaly(merged: pd.DataFrame, series_id: str) -> pd.Series:
    """Flag implausible values or excessive staleness (flag only, never mutate)."""
    lo, hi = MACRO_BOUNDS[series_id]
    ceiling = STALENESS_CEILING[series_id]
    val = merged["value"]
    out_of_range = val.notna() & ((val < lo) | (val > hi))
    stale_days = pd.to_numeric(merged["days_stale"], errors="coerce")
    too_stale = stale_days.notna() & (stale_days > ceiling)
    return (out_of_range | too_stale).fillna(False)
