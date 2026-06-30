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
    # Use DatetimeIndex constructor to avoid pd.to_datetime internal .map() which
    # triggers a Bus error on Python 3.13/macOS Apple Silicon with datetime64 input.
    dti = pd.DatetimeIndex(df["date"])
    if dti.tz is not None:
        dti = dti.tz_convert(None)
    df["date"] = dti.as_unit("ns")

    if df.duplicated(subset=["date", "series_id"]).any():
        raise ValueError("duplicate (date, series_id) rows in macro input")

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

    # Build value and flag columns in a predictable order. Every expected series
    # must be present — a missing one means upstream staging is incomplete, which
    # we surface explicitly rather than letting it fall through to a confusing
    # "column mismatch" in the quality gate.
    for sid in MACRO_SERIES_IDS:
        col = sid.lower()
        if ("value", sid) not in pivoted.columns:
            raise ValueError(f"macro series {sid} missing from staging.macro_aligned")
        result[col] = pivoted[("value", sid)].values
        result[f"{col}_is_imputed"] = pivoted[("is_imputed", sid)].values
        result[f"{col}_is_anomaly"] = pivoted[("is_anomaly", sid)].values

    return result
