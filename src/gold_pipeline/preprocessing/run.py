"""Stage 2 orchestrator: read raw -> clean/align -> quality-check -> UPSERT into staging.

Run with:  python -m gold_pipeline.preprocessing.run
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..db.reader import read_table
from ..db.writer import run_migrations, upsert_dataframe
from ..ingestion.config import Settings
from .align_macro import align_macro_series
from .calendar import trading_days
from .clean_gold import clean_gold
from .quality import check_staging_gold, check_staging_macro

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("preprocessing.run")

_MIGRATIONS = Path(__file__).resolve().parents[3] / "db" / "migrations"


def run_preprocessing(engine, gold_reader, macro_reader) -> dict[str, int]:
    """Transform raw -> staging. gold_reader/macro_reader are zero-arg seams."""
    raw_gold = gold_reader()
    raw_macro = macro_reader()

    gold_staged = clean_gold(raw_gold)
    check_staging_gold(gold_staged)

    days = trading_days(raw_gold)
    macro_staged = pd.concat(
        [
            align_macro_series(g, days)
            for _sid, g in raw_macro.groupby("series_id")
        ],
        ignore_index=True,
    )
    check_staging_macro(macro_staged)

    counts = {
        "gold_prices": upsert_dataframe(
            engine, gold_staged, "gold_prices", "staging", ["date", "source"]
        ),
        "macro_aligned": upsert_dataframe(
            engine, macro_staged, "macro_aligned", "staging", ["date", "series_id"]
        ),
    }
    log.info("preprocessed %s", counts)
    return counts


def main() -> None:
    from sqlalchemy import create_engine

    settings = Settings.from_env()
    engine = create_engine(settings.database_url)
    run_migrations(engine, _MIGRATIONS)

    def gold_reader():
        return read_table(engine, "raw", "gold_prices")

    def macro_reader():
        return read_table(engine, "raw", "macro_indicators")

    run_preprocessing(engine, gold_reader=gold_reader, macro_reader=macro_reader)


if __name__ == "__main__":
    main()
