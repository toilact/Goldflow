# sources/ — external data fetchers

Every source is a function returning a NORMALIZED pandas DataFrame. The orchestrator and
writer never learn where data came from.

Contract for a new source:
- Signature: `fetch_<name>(...) -> pd.DataFrame`, with an injectable client/`downloader`
  argument defaulting to the real library (so tests pass a fake — no network in unit tests).
- Decorate external calls with `@with_retry()` and `@rate_limited(...)` from `..http`.
- Raise `ValueError` on empty results (fail-fast on bad ticker/series/date range).
- Normalized columns:
  - gold-style: `date, open, high, low, close, volume, source`
  - macro-style: `date, series_id, value, release_date`

Active sources: `gold_prices.py` (yfinance, ticker `GC=F`),
`macro_fred.py` (FRED series `DGS10`, `DTWEXBGS`, `CPIAUCSL`).

Point-in-time rule: macro sources MUST populate `release_date` from FRED
`get_series_all_releases` (earliest `realtime_start` per observation), never the observation date.
