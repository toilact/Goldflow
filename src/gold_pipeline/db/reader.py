"""Read helper for loading whole tables into DataFrames (used by later stages)."""
from __future__ import annotations

import pandas as pd
from sqlalchemy.engine import Engine


def read_table(engine: Engine, schema: str, table: str) -> pd.DataFrame:
    """Return the full contents of `schema.table` as a DataFrame."""
    return pd.read_sql_table(table, engine, schema=schema)
