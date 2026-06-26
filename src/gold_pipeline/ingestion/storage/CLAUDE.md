# storage/ — raw-layer persistence

`raw_writer.py` is the ONLY way data reaches the database. It never deletes-then-inserts;
it UPSERTs via Postgres `INSERT ... ON CONFLICT (pk) DO UPDATE`, so re-running ingestion for
the same dates is idempotent.

- `run_migrations(engine, migrations_dir)` applies `db/migrations/*.sql` in order; safe to call
  every run because migrations are idempotent.
- `upsert_dataframe(engine, df, table, schema, pk)` — pk is the composite key:
  `["date", "source"]` for `gold_prices`, `["date", "series_id"]` for `macro_indicators`.

The UPSERT uses `sqlalchemy.dialects.postgresql.insert` — Postgres-only by design. Tests run
against the `gold_test` Postgres DB, never SQLite (the dialect would not match).
