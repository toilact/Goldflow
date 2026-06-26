"""Macro indicator ingestion from FRED, preserving point-in-time release dates.

Stage 2 needs the date each value was actually published to avoid look-ahead leakage.
Two strategies, by series type:

- Revised/vintaged series (e.g. CPI, published ~2 weeks after its reference month and
  later revised): use get_series_all_releases() and keep the EARLIEST realtime_start per
  observation date — the true first-release date.
- Daily, non-revised series (DGS10, DTWEXBGS): get_series_all_releases() would hit FRED's
  ~2000-vintage response limit, so we use get_series() and approximate the release as the
  next day. Assuming availability one day LATER than the observation never leaks the future.
"""
from __future__ import annotations

import logging

import pandas as pd

from ..http import rate_limited, with_retry

log = logging.getLogger(__name__)

MACRO_SERIES = ("DGS10", "DTWEXBGS", "CPIAUCSL")

# Series that are revised over time, so their true first-release date must come from the
# full vintage history. Everything else is treated as a daily, non-revised series.
VINTAGED_SERIES = frozenset({"CPIAUCSL"})


@with_retry()
@rate_limited(min_interval_s=0.5)
def fetch_fred_series(series_id: str, client) -> pd.DataFrame:
    """Return normalized macro rows with the first-release date per observation."""
    if series_id in VINTAGED_SERIES:
        raw = client.get_series_all_releases(series_id)
        if raw is None or len(raw) == 0:
            raise ValueError(f"Empty data for FRED series {series_id}")
        raw = raw.copy()
        raw["date"] = pd.to_datetime(raw["date"])
        raw["realtime_start"] = pd.to_datetime(raw["realtime_start"])
    else:
        # Daily series: get_series_all_releases hits FRED's ~2000-vintage limit. They are
        # not revised, so use get_series and assume next-day release. Drop NaN observations
        # (FRED returns NaN for market holidays) so no NULL-value rows reach the raw layer.
        s = client.get_series(series_id)
        if s is not None:
            s = s.dropna()
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
