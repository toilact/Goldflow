"""Feature parameters + the canonical column list (single source of truth).

`feature_table_columns()` MUST stay in sync with db/migrations/003_features_schema.sql.
Changing any window/lag/horizon that adds or renames a column requires a new migration.
"""
from __future__ import annotations

from dataclasses import dataclass

MACRO_SERIES_IDS: tuple[str, ...] = ("DGS10", "DTWEXBGS", "CPIAUCSL")


@dataclass(frozen=True)
class FeatureConfig:
    sma_windows: tuple[int, ...] = (10, 20)
    ema_windows: tuple[int, ...] = (12, 26)
    rsi_window: int = 14
    macd: tuple[int, int, int] = (12, 26, 9)  # fast, slow, signal
    bb_window: int = 20
    bb_dev: float = 2.0
    ratio_sma_windows: tuple[int, ...] = (10, 20)
    lag_columns: tuple[str, ...] = ("log_return", "rsi_14")
    lags: tuple[int, ...] = (1, 2, 3, 5)
    horizons: tuple[int, ...] = (1, 5)


DEFAULT_CONFIG = FeatureConfig()

# log_return is aliased to "logret" in lagged/derived column names for brevity.
_COL_ALIAS = {"log_return": "logret"}


def _alias(col: str) -> str:
    return _COL_ALIAS.get(col, col)


def technical_columns(cfg: FeatureConfig) -> list[str]:
    cols: list[str] = []
    cols += [f"sma_{w}" for w in cfg.sma_windows]
    cols += [f"ema_{w}" for w in cfg.ema_windows]
    cols += [f"rsi_{cfg.rsi_window}"]
    cols += ["macd", "macd_signal", "macd_diff"]
    cols += ["bb_high", "bb_mid", "bb_low"]
    return cols


def ratio_columns(cfg: FeatureConfig) -> list[str]:
    return [f"close_to_sma_{w}" for w in cfg.ratio_sma_windows]


def lagged_columns(cfg: FeatureConfig) -> list[str]:
    return [f"{_alias(c)}_lag_{k}" for c in cfg.lag_columns for k in cfg.lags]


def target_columns(cfg: FeatureConfig) -> list[str]:
    return [f"target_logret_{h}" for h in cfg.horizons]


def macro_value_columns() -> list[str]:
    return [s.lower() for s in MACRO_SERIES_IDS]


def macro_flag_columns() -> list[str]:
    out: list[str] = []
    for s in MACRO_SERIES_IDS:
        out += [f"{s.lower()}_is_imputed", f"{s.lower()}_is_anomaly"]
    return out


def warmup_columns(cfg: FeatureConfig) -> list[str]:
    """Gold-derived feature columns whose NaN at the head defines has_features=False."""
    return technical_columns(cfg) + ratio_columns(cfg) + lagged_columns(cfg)


def feature_table_columns(cfg: FeatureConfig) -> list[str]:
    """Ordered columns written to features.gold_features (excludes processed_at default)."""
    return (
        ["date", "source", "close", "log_return"]
        + technical_columns(cfg)
        + ratio_columns(cfg)
        + macro_value_columns()
        + macro_flag_columns()
        + lagged_columns(cfg)
        + target_columns(cfg)
        + ["has_features"]
        + [f"has_target_{h}" for h in cfg.horizons]
    )
