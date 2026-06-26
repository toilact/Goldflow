CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.gold_prices (
    date        DATE        NOT NULL,
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4),
    volume      BIGINT,
    source      TEXT        NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, source)
);

CREATE TABLE IF NOT EXISTS raw.macro_indicators (
    date         DATE NOT NULL,
    series_id    TEXT NOT NULL,
    value        NUMERIC(14,6),
    release_date DATE,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, series_id)
);
