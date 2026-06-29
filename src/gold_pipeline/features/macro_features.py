"""Pivot the long staging.macro_aligned into a wide, per-date frame.

Stage 2 already point-in-time aligned macro onto trading days, so here we only
reshape long→wide (one column per series) and carry the confidence flags. We do
NOT shift or re-align — that would risk re-introducing look-ahead.
"""
from __future__ import annotations

import pandas as pd

from .config import MACRO_SERIES_IDS


def build_macro_wide(macro_df: pd.DataFrame) -> pd.DataFrame:
    df = macro_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

    # Group by date, then pivot series to columns
    pivoted = df.pivot_table(
        index="date",
        columns="series_id",
        values=["value", "is_imputed", "is_anomaly"],
        aggfunc="first"  # No aggregation needed; just structure
    )

    # Flatten multi-level columns
    result = pd.DataFrame()
    result["date"] = pivoted.index
    result.reset_index(drop=True, inplace=True)

    # Build value and flag columns in a predictable order
    for sid in MACRO_SERIES_IDS:
        col = sid.lower()
        if ("value", sid) in pivoted.columns:
            result[col] = pivoted[("value", sid)].values
        if ("is_imputed", sid) in pivoted.columns:
            result[f"{col}_is_imputed"] = pivoted[("is_imputed", sid)].values
        if ("is_anomaly", sid) in pivoted.columns:
            result[f"{col}_is_anomaly"] = pivoted[("is_anomaly", sid)].values

    return result
