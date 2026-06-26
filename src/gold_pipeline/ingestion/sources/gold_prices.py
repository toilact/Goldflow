"""Gold OHLCV ingestion from Yahoo Finance (ticker GC=F by default)."""
from __future__ import annotations

import logging
from typing import Callable

import pandas as pd

from ..http import rate_limited, with_retry

log = logging.getLogger(__name__)

_COLUMN_MAP = {"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}


@with_retry()
@rate_limited(min_interval_s=1.0)
def fetch_gold_prices(
    start: str,
    end: str,
    ticker: str = "GC=F",
    downloader: Callable | None = None,
) -> pd.DataFrame:
    """Fetch daily gold OHLCV; return normalized columns. Raise ValueError if empty."""
    if downloader is None:
        import yfinance as yf
        downloader = yf.download

    raw = downloader(ticker, start=start, end=end, interval="1d", auto_adjust=False, progress=False)
    if raw is None or raw.empty:
        raise ValueError(f"Empty data for {ticker} ({start}..{end}) — check ticker/date range")

    # yfinance can return a MultiIndex column frame for a single ticker; flatten it.
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    df = raw.rename(columns=_COLUMN_MAP)[list(_COLUMN_MAP.values())].copy()
    df.insert(0, "date", pd.to_datetime(raw.index).tz_localize(None))
    df["source"] = ticker
    return df.reset_index(drop=True)
