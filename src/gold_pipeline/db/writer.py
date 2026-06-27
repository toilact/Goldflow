"""Idempotent DB writer shared across pipeline stages (Postgres ON CONFLICT UPSERT)."""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)


def run_migrations(engine: Engine, migrations_dir: Path) -> None:
    """Apply every *.sql file in filename order. Migrations must be idempotent."""
    for sql_file in sorted(Path(migrations_dir).glob("*.sql")):
        sql = sql_file.read_text()
        with engine.begin() as conn:
            conn.execute(text(sql))
        log.info("applied migration %s", sql_file.name)


def upsert_dataframe(
    engine: Engine, df: pd.DataFrame, table: str, schema: str, pk: list[str]
) -> int:
    """INSERT ... ON CONFLICT (pk) DO UPDATE for non-pk columns. Returns rows written."""
    if df.empty:
        return 0
    meta = MetaData()
    tbl = Table(table, meta, schema=schema, autoload_with=engine)
    records = df.to_dict(orient="records")
    stmt = insert(tbl).values(records)
    update_cols = {c: stmt.excluded[c] for c in df.columns if c not in pk}
    stmt = stmt.on_conflict_do_update(index_elements=pk, set_=update_cols)
    with engine.begin() as conn:
        conn.execute(stmt)
    return len(records)
