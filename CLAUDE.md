# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

This is an **early-stage / pre-implementation** repository. It currently holds only a design
spec — there is no application code, build system, or test suite yet. Do not invent build/lint/test
commands; add them to this file once the toolchain (likely `pip`/`venv` + `pytest`) actually exists.

The owner's communication preference is **Vietnamese** for coding work in this project (see godchi toolkit).

## What this project is

A personal **learning** project that builds a professional-grade data pipeline for **XAU/USD (gold)**
price forecasting. It ingests gold prices + macro indicators + financial news, processes them, and
packages model-ready datasets for ML (LSTM / XGBoost / Random Forest). The goal is to mirror how large
fintech teams operate while staying understandable for someone learning Data Engineering / Data Science.

Authoritative design source: [docs/superpowers/specs/2026-06-26-gold-data-pipeline-prompt-design.md](docs/superpowers/specs/2026-06-26-gold-data-pipeline-prompt-design.md).
Read it before implementing any stage.

## Intended tech stack

Python · `yfinance` (gold prices, ticker `GC=F`) · `fredapi` (macro: `DGS10` US10Y, `DTWEXBGS` broad
dollar index, `CPIAUCSL` CPI) · news via RSS / ForexFactory (avoid scraping Reuters/Bloomberg — their
ToS forbids it) · `pandas`/`numpy` · `ta` for indicators (prefer over `pandas_ta`) · PostgreSQL via
`SQLAlchemy`/`psycopg2` · `tenacity` for retry · `plotly` for viz.

## Architecture (the big picture)

A **Medallion-style** 4-stage pipeline, with PostgreSQL schemas as the layer boundaries:

1. **Ingestion → `raw` schema** — pull from APIs/news. Store source data *immutable*. Retry with
   exponential backoff + rate limiting. Tables keyed for idempotent `UPSERT` (e.g. `(date, source)`).
2. **Preprocessing → `staging` schema** — align mixed frequencies onto the gold trading calendar,
   clean, flag outliers, mark imputed rows.
3. **Feature Engineering → `features` schema** — technical indicators, lagged features, sentiment.
4. **Packaging** — time-series split + scaling → dataset generator for ML models.

Data Quality Checks run *between* every stage (fail-fast).

## Non-negotiable invariants (the reason this project exists)

These are the cross-cutting rules every stage must honor. They are the most common ways financial
time-series pipelines silently break, so treat violations as bugs:

- **Point-in-time correctness.** At time `t`, only data *actually published* by `t` may be visible.
  Notably **CPI is released ~2 weeks after its reference month** — join macro by *release/available
  date*, never reference date (`merge_asof(..., direction="backward")`).
- **No look-ahead / data leakage.**
  - Fill gaps with **forward-fill only**. Never `bfill` or `interpolate` across the future.
  - Features stop at `t`; lagged features use `shift(+k)` (past). Targets use `shift(-h)` (future)
    and are the *only* place future values appear.
  - **Fit scalers/encoders on the TRAIN split only**, then transform all splits.
  - Split chronologically (no `shuffle`); for CV use walk-forward (`TimeSeriesSplit`), not K-Fold.
- **Idempotency.** Re-running a stage for the same date yields identical rows (`ON CONFLICT DO UPDATE`).

There are godchi skills that enforce these — `ml-data-leakage-guard`, `walk-forward-guard`,
`model-feature-versioning`. Consult them when touching feature/dataset/split/scaler code.

## Conventions

- Layers map to DB schemas (`raw` / `staging` / `features`); keep raw data immutable and reproducible.
- New specs/design docs go under `docs/superpowers/specs/` with a dated `YYYY-MM-DD-topic.md` name.
