"""Macro indicator ingestion from FRED, preserving point-in-time release dates.

fred.get_series() omits the release date, which Stage 2 needs to avoid look-ahead
leakage (e.g. CPI is published ~2 weeks after its reference month). We therefore use
get_series_all_releases() and keep the EARLIEST realtime_start per observation date.
"""
from __future__ import annotations

import logging

import pandas as pd

from ..http import rate_limited, with_retry

log = logging.getLogger(__name__)

MACRO_SERIES = ("DGS10", "DTWEXBGS", "CPIAUCSL")


@with_retry()
@rate_limited(min_interval_s=0.5)
def fetch_fred_series(series_id: str, client) -> pd.DataFrame:
    """Return normalized macro rows with the first-release date per observation."""
    if series_id == "CPIAUCSL":
        raw = client.get_series_all_releases(series_id)
        if raw is None or len(raw) == 0:
            raise ValueError(f"Empty data for FRED series {series_id}")
        raw = raw.copy()
        raw["date"] = pd.to_datetime(raw["date"])
        raw["realtime_start"] = pd.to_datetime(raw["realtime_start"])
    else:
        # For daily series, get_series_all_releases hits FRED's 2000 vintage limit.
        # Daily series are not revised, so we use get_series and assume next-day release.
        s = client.get_series(series_id)
        if s is None or s.empty:
            raise ValueError(f"Empty data for FRED series {series_id}")
        raw = pd.DataFrame({
            "date": pd.to_datetime(s.index),
            "realtime_start": pd.to_datetime(s.index) + pd.Timedelta(days=1),
            "value": s.values,
        })

    first = (
        raw.sort_values("realtime_start")
        .groupby("date", as_index=False)
        .first()
    )
    out = pd.DataFrame({
        "date": first["date"],
        "series_id": series_id,
        "value": first["value"],
        "release_date": first["realtime_start"],
    })
    return out.sort_values("date").reset_index(drop=True)

