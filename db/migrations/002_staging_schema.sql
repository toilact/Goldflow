CREATE SCHEMA IF NOT EXISTS staging;

-- Gold, cleaned. `date` is the trading-calendar backbone; gold is never imputed
-- (it defines the calendar, so there are no internal days to fill for itself).
CREATE TABLE IF NOT EXISTS staging.gold_prices (
    date         DATE        NOT NULL,
    open         NUMERIC(12,4),
    high         NUMERIC(12,4),
    low          NUMERIC(12,4),
    close        NUMERIC(12,4),
    volume       BIGINT,
    log_return   NUMERIC(12,8),
    is_outlier   BOOLEAN     NOT NULL DEFAULT false,
    source       TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, source)
);

-- Macro, reindexed point-in-time onto each gold trading day (long-form).
CREATE TABLE IF NOT EXISTS staging.macro_aligned (
    date         DATE NOT NULL,
    series_id    TEXT NOT NULL,
    value        NUMERIC(14,6),
    release_date DATE,
    is_imputed   BOOLEAN NOT NULL DEFAULT false,
    days_stale   INTEGER,
    is_anomaly   BOOLEAN NOT NULL DEFAULT false,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, series_id)
);
