"""The gold trading calendar — the date backbone every other source aligns to.

Derived purely from the dates present in raw.gold_prices (no external exchange
calendar dependency), so it can never diverge from the data we actually have.
"""
from __future__ import annotations

import pandas as pd


def trading_days(gold_df: pd.DataFrame) -> pd.Series:
    """Distinct gold trading dates, sorted ascending, tz-naive, index reset."""
    days = (
        pd.to_datetime(gold_df["date"])
        .dt.tz_localize(None)
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    return days
