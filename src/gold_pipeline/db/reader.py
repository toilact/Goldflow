"""Read helper for loading whole tables into DataFrames (used by later stages)."""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def read_table(engine: Engine, schema: str, table: str) -> pd.DataFrame:
    """Return the full contents of `schema.table` as a DataFrame.

    Uses a raw SQL SELECT to avoid a pandas/numpy SIGBUS crash on Python 3.13 +
    macOS ARM64 that occurs in pd.read_sql_table's datetime-harmonization path
    for DATE columns with multiple rows.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {schema}.{table}"))  # noqa: S608
        rows = result.mappings().fetchall()
        cols = list(result.keys())
    return pd.DataFrame([dict(r) for r in rows], columns=cols)
