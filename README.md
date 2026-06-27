# Gold Data Pipeline — Stage 1 (Ingestion)

Fetches XAU/USD gold prices (yfinance) and macro indicators (FRED: DGS10, DTWEXBGS, CPIAUCSL)
into a PostgreSQL `raw` schema. See `docs/superpowers/specs/` for design, `docs/superpowers/plans/`
for the implementation plan.

## Quickstart
1. `cp .env.example .env` and set `FRED_API_KEY` (free: https://fred.stlouisfed.org/docs/api/api_key.html)
2. `pip install -e ".[dev]"`
3. `docker compose up -d`
4. `python -m gold_pipeline.ingestion.run`

## Test
- `pytest -q -k "not raw_writer"` — fast unit tests, no DB
- `TEST_DATABASE_URL=postgresql+psycopg2://gold:gold@localhost:5432/gold_test pytest -q` — incl. integration

## Stage 2 — Preprocessing (`staging`)

Reads `raw`, cleans gold (per-source log-return + robust outlier flag), and reindexes macro onto the
gold trading calendar point-in-time (`merge_asof` backward on `release_date`). Run after Stage 1:
`python -m gold_pipeline.preprocessing.run`.
