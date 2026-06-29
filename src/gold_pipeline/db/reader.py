"""Read helper for loading whole tables into DataFrames (used by later stages)."""
from __future__ import annotations

import decimal

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def read_table(engine: Engine, schema: str, table: str) -> pd.DataFrame:
    """Return the full contents of `schema.table` as a DataFrame.

    Uses mappings().fetchall() to avoid a pandas/numpy SIGBUS crash on
    Python 3.13 + macOS ARM64 that occurs in pd.read_sql / pd.read_sql_table's
    datetime-harmonization path for DATE columns with multiple rows.

    psycopg2 returns PostgreSQL NUMERIC columns as decimal.Decimal (dtype=object).
    We coerce those columns to float64 so that downstream np.log / arithmetic
    (e.g. in clean_gold.py) works without explicit casting at call sites.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {schema}.{table}"))  # noqa: S608
        rows = result.mappings().fetchall()
        cols = list(result.keys())

    df = pd.DataFrame([dict(r) for r in rows], columns=cols)

    # Coerce any Decimal columns → float64 (psycopg2 maps NUMERIC to Decimal).
    for col in df.columns:
        if not df[col].empty and df[col].map(lambda v: isinstance(v, decimal.Decimal)).any():
            df[col] = df[col].astype(float)

    return df
