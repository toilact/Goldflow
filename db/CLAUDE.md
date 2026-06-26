# db/ — database layer

Migrations are plain SQL, numbered `NNN_description.sql`, and MUST be idempotent
(`CREATE ... IF NOT EXISTS`). They are applied by `storage/raw_writer.run_migrations()`,
which executes every file in `migrations/` in filename order on each ingestion run.

`init/` scripts run ONLY by the postgres container on first init of an empty data volume —
used here to create the `gold_test` database. After editing `init/`, recreate with
`docker compose down -v && docker compose up -d`.

Schemas map to pipeline layers: `raw` (immutable source data) now; `staging`, `features` later.
