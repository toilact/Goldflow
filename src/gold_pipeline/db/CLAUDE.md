# db/ — shared database layer (all stages)

The single place data reaches Postgres. Stage-agnostic: ingestion writes `raw`,
preprocessing writes `staging`, both through these helpers.

- `writer.run_migrations(engine, migrations_dir)` — applies `db/migrations/*.sql` in filename
  order; safe every run because migrations are idempotent (`CREATE ... IF NOT EXISTS`).
- `writer.upsert_dataframe(engine, df, table, schema, pk)` — `INSERT ... ON CONFLICT (pk) DO UPDATE`
  for non-pk columns. pk is the composite key, e.g. `["date", "source"]` (gold) or
  `["date", "series_id"]` (macro). Re-running the same rows is idempotent.
- `reader.read_table(engine, schema, table)` — `SELECT *` into a DataFrame.

UPSERT uses `sqlalchemy.dialects.postgresql.insert` — Postgres-only by design. Tests run against
the `gold_test` Postgres DB, never SQLite (the dialect would not match).
