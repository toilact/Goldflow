"""Stage 1 orchestrator: fetch sources -> quality-check -> UPSERT into raw.

Run with:  python -m gold_pipeline.ingestion.run
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .config import Settings
from .quality import check_gold, check_macro
from .sources.gold_prices import fetch_gold_prices
from .sources.macro_fred import MACRO_SERIES, fetch_fred_series
from ..db.writer import run_migrations, upsert_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("ingestion.run")

_MIGRATIONS = Path(__file__).resolve().parents[3] / "db" / "migrations"


def run_ingestion(settings, gold_fetcher, fred_client, engine) -> dict[str, int]:
    """Fetch, validate, and UPSERT both sources. Returns rows written per table."""
    gold = gold_fetcher(settings.ingest_start, settings.ingest_end)
    check_gold(gold)

    macro = pd.concat(
        [fetch_fred_series(s, client=fred_client) for s in MACRO_SERIES],
        ignore_index=True,
    )
    check_macro(macro)

    counts = {
        "gold_prices": upsert_dataframe(engine, gold, "gold_prices", "raw", ["date", "source"]),
        "macro_indicators": upsert_dataframe(
            engine, macro, "macro_indicators", "raw", ["date", "series_id"]
        ),
    }
    log.info("ingested %s", counts)
    return counts


def main() -> None:
    from fredapi import Fred
    from sqlalchemy import create_engine

    settings = Settings.from_env()
    engine = create_engine(settings.database_url)
    run_migrations(engine, _MIGRATIONS)
    fred = Fred(api_key=settings.fred_api_key)

    def gold_fetcher(start, end):
        return fetch_gold_prices(start, end)

    run_ingestion(settings, gold_fetcher=gold_fetcher, fred_client=fred, engine=engine)


if __name__ == "__main__":
    main()
