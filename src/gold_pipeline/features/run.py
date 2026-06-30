"""Stage 3 orchestrator: read staging -> assemble -> quality-check -> UPSERT features.

Run with:  python -m gold_pipeline.features.run
"""
from __future__ import annotations

import logging
from pathlib import Path

from ..db.reader import read_table
from ..db.writer import run_migrations, upsert_dataframe
from ..ingestion.config import Settings
from .assemble import assemble_features
from .config import DEFAULT_CONFIG, FeatureConfig
from .quality import check_features

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("features.run")

_MIGRATIONS = Path(__file__).resolve().parents[3] / "db" / "migrations"


def run_features(engine, gold_reader, macro_reader, cfg: FeatureConfig = DEFAULT_CONFIG) -> dict[str, int]:
    """Transform staging -> features. gold_reader/macro_reader are zero-arg seams."""
    gold = gold_reader()
    macro = macro_reader()
    if gold.empty:
        raise ValueError("staging.gold_prices is empty; run Stage 2 first")

    feats = assemble_features(gold, macro, cfg)
    check_features(feats, cfg)

    n = upsert_dataframe(engine, feats, "gold_features", "features", ["date", "source"])
    counts = {"gold_features": n}
    log.info("featurized %s", counts)
    return counts


def main() -> None:
    from sqlalchemy import create_engine

    settings = Settings.from_env()
    engine = create_engine(settings.database_url)
    run_migrations(engine, _MIGRATIONS)

    def gold_reader():
        return read_table(engine, "staging", "gold_prices")

    def macro_reader():
        return read_table(engine, "staging", "macro_aligned")

    run_features(engine, gold_reader=gold_reader, macro_reader=macro_reader)


if __name__ == "__main__":
    main()
