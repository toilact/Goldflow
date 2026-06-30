# Stage 3 — Feature Engineering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `features` package that turns `staging` (cleaned gold + point-in-time macro) into a wide, model-ready table `features.gold_features`.

**Architecture:** Một package `src/gold_pipeline/features/` theo pattern Stage 1/2 — mỗi file một việc, hàm thuần (pure) cho tính toán + một `run.py` wiring có seam inject để test. Tái dùng `gold_pipeline.db` (writer/reader). Tất cả phép time-series (`shift`, rolling) đi qua `groupby("source")`.

**Tech Stack:** Python 3.11+, pandas, numpy, `ta` (indicators), SQLAlchemy + psycopg2 (Postgres), pytest.

**Spec:** [docs/superpowers/specs/2026-06-29-stage3-feature-engineering-design.md](../specs/2026-06-29-stage3-feature-engineering-design.md)

## Global Constraints

- Mọi `shift` (target `shift(-h)`, lag `shift(+k)`) PHẢI qua `groupby("source")[col].shift(...)` — không bao giờ shift trên DataFrame phẳng (tránh boundary bleed giữa các nguồn).
- Chỉ lag đại lượng **stationary** (`log_return`, `rsi_14`); KHÔNG lag `close`.
- `config.py` là nguồn sự thật của tập cột, nhưng PHẢI khớp đúng cột tĩnh trong `db/migrations/003_features_schema.sql`. Đổi indicator/lag/horizon ⇒ viết migration mới.
- "Flag, đừng mutate": giữ NaN → NULL, thêm cờ; không drop, không bịa giá trị (no bfill/interpolate).
- Idempotent: UPSERT composite PK `(date, source)`.
- Fail-fast: nguồn rỗng hoặc quality check fail → raise trước mọi ghi DB.
- DB là Postgres (`postgresql.insert` UPSERT); test DB-integration chạy trên `gold_test`, không SQLite.
- Unit test không cần DB; theo quy ước `pytest -q -k "not test_writer and not test_reader"`.

---

## File Structure

- Create `src/gold_pipeline/features/__init__.py` — package marker.
- Create `src/gold_pipeline/features/config.py` — `FeatureConfig` + các hàm liệt kê cột (nguồn sự thật).
- Create `src/gold_pipeline/features/technical.py` — `add_technical` (indicators + ratio stationary).
- Create `src/gold_pipeline/features/lagged.py` — `add_lagged` (lag stationary qua groupby).
- Create `src/gold_pipeline/features/macro_features.py` — `build_macro_wide` (pivot long→wide + cờ).
- Create `src/gold_pipeline/features/target.py` — `add_targets` (multi-horizon shift(-h) qua groupby).
- Create `src/gold_pipeline/features/assemble.py` — `assemble_features` (ghép + cờ + chuẩn hoá date).
- Create `src/gold_pipeline/features/quality.py` — `check_features` (gate fail-fast).
- Create `src/gold_pipeline/features/run.py` — `run_features(...)` wiring + `main()`.
- Create `src/gold_pipeline/features/CLAUDE.md` — boundary doc (như preprocessing/CLAUDE.md).
- Create `db/migrations/003_features_schema.sql` — bảng `features.gold_features` (cột tĩnh).
- Modify `pyproject.toml` — thêm `ta>=0.11` vào dependencies.
- Create `tests/features/__init__.py` + `tests/features/test_config.py`, `test_technical.py`, `test_lagged.py`, `test_macro_features.py`, `test_target.py`, `test_assemble.py`, `test_quality.py`.
- Create `tests/db/test_features_run.py` — DB integration cho `run_features`.

---

### Task 1: Dependency + package skeleton + `config.py`

**Files:**
- Modify: `pyproject.toml` (dependencies)
- Create: `src/gold_pipeline/features/__init__.py`
- Create: `src/gold_pipeline/features/config.py`
- Test: `tests/features/__init__.py`, `tests/features/test_config.py`

**Interfaces:**
- Produces:
  - `FeatureConfig` (frozen dataclass) với mặc định: `sma_windows=(10,20)`, `ema_windows=(12,26)`, `rsi_window=14`, `macd=(12,26,9)`, `bb_window=20`, `bb_dev=2.0`, `ratio_sma_windows=(10,20)`, `lag_columns=("log_return","rsi_14")`, `lags=(1,2,3,5)`, `horizons=(1,5)`.
  - `DEFAULT_CONFIG: FeatureConfig`
  - `MACRO_SERIES_IDS: tuple[str,...] = ("DGS10","DTWEXBGS","CPIAUCSL")`
  - `technical_columns(cfg) -> list[str]`, `ratio_columns(cfg) -> list[str]`, `lagged_columns(cfg) -> list[str]`, `target_columns(cfg) -> list[str]`, `macro_value_columns() -> list[str]`, `macro_flag_columns() -> list[str]`, `warmup_columns(cfg) -> list[str]`, `feature_table_columns(cfg) -> list[str]`.

- [ ] **Step 1: Thêm `ta` vào dependencies**

Trong `pyproject.toml`, mục `[project].dependencies`, thêm dòng (sau `tenacity>=8.3`):

```toml
    "ta>=0.11",
```

- [ ] **Step 2: Cài lại deps**

Run: `pip install -e ".[dev]"`
Expected: cài thành công, có `ta`.

- [ ] **Step 3: Tạo package markers**

Tạo `src/gold_pipeline/features/__init__.py` (rỗng) và `tests/features/__init__.py` (rỗng).

- [ ] **Step 4: Viết test thất bại cho `config.py`**

Tạo `tests/features/test_config.py`:

```python
from gold_pipeline.features import config as cfg


def test_default_horizons_and_lags():
    c = cfg.DEFAULT_CONFIG
    assert c.horizons == (1, 5)
    assert c.lags == (1, 2, 3, 5)
    assert c.lag_columns == ("log_return", "rsi_14")  # stationary only, no close


def test_lagged_columns_use_logret_alias_and_skip_close():
    cols = cfg.lagged_columns(cfg.DEFAULT_CONFIG)
    assert "logret_lag_1" in cols
    assert "rsi_14_lag_5" in cols
    assert not any(c.startswith("close_lag") for c in cols)


def test_target_columns_per_horizon():
    assert cfg.target_columns(cfg.DEFAULT_CONFIG) == ["target_logret_1", "target_logret_5"]


def test_macro_columns_lowercased_with_flags():
    assert cfg.macro_value_columns() == ["dgs10", "dtwexbgs", "cpiaucsl"]
    assert "dgs10_is_imputed" in cfg.macro_flag_columns()
    assert "cpiaucsl_is_anomaly" in cfg.macro_flag_columns()


def test_feature_table_columns_are_unique_and_ordered():
    cols = cfg.feature_table_columns(cfg.DEFAULT_CONFIG)
    assert cols[:2] == ["date", "source"]
    assert len(cols) == len(set(cols))  # no duplicates
    for flag in ["has_features", "has_target_1", "has_target_5"]:
        assert flag in cols
```

- [ ] **Step 5: Chạy test để xác nhận FAIL**

Run: `pytest tests/features/test_config.py -v`
Expected: FAIL (ModuleNotFoundError: `gold_pipeline.features.config`).

- [ ] **Step 6: Viết `config.py`**

Tạo `src/gold_pipeline/features/config.py`:

```python
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
```

- [ ] **Step 7: Chạy test để xác nhận PASS**

Run: `pytest tests/features/test_config.py -v`
Expected: PASS (5 tests).

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml src/gold_pipeline/features/__init__.py src/gold_pipeline/features/config.py tests/features/
git commit -m "feat: Stage 3 features config + ta dependency"
```

---

### Task 2: `technical.py` — indicators + stationary ratio features

**Files:**
- Create: `src/gold_pipeline/features/technical.py`
- Test: `tests/features/test_technical.py`

**Interfaces:**
- Consumes: `FeatureConfig`, `technical_columns`, `ratio_columns` từ `config`.
- Produces: `add_technical(gold_df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG) -> pd.DataFrame`
  — trả bản copy sắp xếp `(source, date)` với các cột trong `technical_columns(cfg)` + `ratio_columns(cfg)` thêm vào. Đầu vào phải có `date, source, close`.

- [ ] **Step 1: Viết test thất bại**

Tạo `tests/features/test_technical.py`:

```python
import numpy as np
import pandas as pd
import ta
from gold_pipeline.features.config import DEFAULT_CONFIG
from gold_pipeline.features.technical import add_technical


def _gold(closes, source="GC=F", start="2020-01-01"):
    dates = pd.bdate_range(start, periods=len(closes))
    return pd.DataFrame({"date": dates, "close": closes, "source": source})


def test_rsi_matches_ta_reference():
    closes = list(np.linspace(100, 130, 40))
    out = add_technical(_gold(closes))
    ref = ta.momentum.rsi(pd.Series(closes), window=14)
    np.testing.assert_allclose(
        out["rsi_14"].to_numpy(dtype=float), ref.to_numpy(dtype=float), equal_nan=True
    )


def test_ratio_feature_is_close_over_sma():
    closes = list(np.linspace(100, 130, 40))
    out = add_technical(_gold(closes))
    sma10 = ta.trend.sma_indicator(pd.Series(closes), window=10)
    expected = pd.Series(closes) / sma10
    np.testing.assert_allclose(
        out["close_to_sma_10"].to_numpy(dtype=float),
        expected.to_numpy(dtype=float),
        equal_nan=True,
    )


def test_per_source_isolation_no_bleed():
    # Two sources; the SMA of source B must not pull rows from source A.
    a = _gold(list(np.linspace(100, 130, 40)), source="GC=F")
    b = _gold(list(np.linspace(2000, 2050, 40)), source="XAU/USD")
    out = add_technical(pd.concat([a, b], ignore_index=True))
    b_out = out[out["source"] == "XAU/USD"].reset_index(drop=True)
    ref_b = ta.trend.sma_indicator(pd.Series(list(np.linspace(2000, 2050, 40))), window=10)
    np.testing.assert_allclose(
        b_out["sma_10"].to_numpy(dtype=float), ref_b.to_numpy(dtype=float), equal_nan=True
    )


def test_causality_future_change_does_not_affect_today():
    # Perturbation: changing row t+1 must not change indicators at row t.
    closes = list(np.linspace(100, 130, 40))
    base = add_technical(_gold(closes))
    perturbed_closes = closes.copy()
    perturbed_closes[30] = 999.0  # change the future relative to row 25
    pert = add_technical(_gold(perturbed_closes))
    cols = ["sma_10", "ema_12", "rsi_14", "macd"]
    pd.testing.assert_frame_equal(base.loc[:25, cols], pert.loc[:25, cols])
```

- [ ] **Step 2: Chạy test để xác nhận FAIL**

Run: `pytest tests/features/test_technical.py -v`
Expected: FAIL (ModuleNotFoundError: `technical`).

- [ ] **Step 3: Viết `technical.py`**

Tạo `src/gold_pipeline/features/technical.py`:

```python
"""Per-source technical indicators + stationary ratio features (via `ta`).

`ta` operates on a Series, not a groupby object, so we compute per source group
and assign back by index — no cross-source bleed.
"""
from __future__ import annotations

import pandas as pd
import ta

from .config import DEFAULT_CONFIG, FeatureConfig


def add_technical(gold_df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG) -> pd.DataFrame:
    df = gold_df.sort_values(["source", "date"]).reset_index(drop=True)
    for _src, idx in df.groupby("source").groups.items():
        close = df.loc[idx, "close"].astype(float)
        for w in cfg.sma_windows:
            df.loc[idx, f"sma_{w}"] = ta.trend.sma_indicator(close, window=w)
        for w in cfg.ema_windows:
            df.loc[idx, f"ema_{w}"] = ta.trend.ema_indicator(close, window=w)
        df.loc[idx, f"rsi_{cfg.rsi_window}"] = ta.momentum.rsi(close, window=cfg.rsi_window)

        fast, slow, sign = cfg.macd
        macd = ta.trend.MACD(close, window_slow=slow, window_fast=fast, window_sign=sign)
        df.loc[idx, "macd"] = macd.macd()
        df.loc[idx, "macd_signal"] = macd.macd_signal()
        df.loc[idx, "macd_diff"] = macd.macd_diff()

        bb = ta.volatility.BollingerBands(close, window=cfg.bb_window, window_dev=cfg.bb_dev)
        df.loc[idx, "bb_high"] = bb.bollinger_hband()
        df.loc[idx, "bb_mid"] = bb.bollinger_mavg()
        df.loc[idx, "bb_low"] = bb.bollinger_lband()

        for w in cfg.ratio_sma_windows:
            df.loc[idx, f"close_to_sma_{w}"] = close / ta.trend.sma_indicator(close, window=w)
    return df
```

- [ ] **Step 4: Chạy test để xác nhận PASS**

Run: `pytest tests/features/test_technical.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/features/technical.py tests/features/test_technical.py
git commit -m "feat: Stage 3 technical indicators + stationary ratio features"
```

---

### Task 3: `lagged.py` — stationary lags via groupby

**Files:**
- Create: `src/gold_pipeline/features/lagged.py`
- Test: `tests/features/test_lagged.py`

**Interfaces:**
- Consumes: `FeatureConfig`, `_alias` logic (qua `config`).
- Produces: `add_lagged(df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG) -> pd.DataFrame`
  — thêm các cột trong `lagged_columns(cfg)`. Đầu vào phải có `source` + các cột trong `cfg.lag_columns` (vd `log_return`, `rsi_14`). Lag qua `groupby("source")`.

- [ ] **Step 1: Viết test thất bại**

Tạo `tests/features/test_lagged.py`:

```python
import numpy as np
import pandas as pd
from gold_pipeline.features.config import DEFAULT_CONFIG
from gold_pipeline.features.lagged import add_lagged


def _df(n, source="GC=F", start="2020-01-01", base=0.0):
    dates = pd.bdate_range(start, periods=n)
    return pd.DataFrame({
        "date": dates,
        "source": source,
        "log_return": [base + i for i in range(n)],
        "rsi_14": [50 + i for i in range(n)],
    })


def test_lag_is_past_value():
    out = add_lagged(_df(5)).reset_index(drop=True)
    # logret_lag_1 at row k equals log_return at row k-1
    assert out.loc[2, "logret_lag_1"] == out.loc[1, "log_return"]
    assert np.isnan(out.loc[0, "logret_lag_1"])


def test_no_close_lag_columns():
    out = add_lagged(_df(5))
    assert not any(c.startswith("close_lag") for c in out.columns)


def test_lag_does_not_bleed_across_sources():
    a = _df(4, source="GC=F", base=0.0)
    b = _df(4, source="XAU/USD", base=100.0)
    out = add_lagged(pd.concat([a, b], ignore_index=True))
    b_out = out[out["source"] == "XAU/USD"].reset_index(drop=True)
    # First row of source B must be NaN, NOT the last value of source A.
    assert np.isnan(b_out.loc[0, "logret_lag_1"])
    assert b_out.loc[1, "logret_lag_1"] == b_out.loc[0, "log_return"]
```

- [ ] **Step 2: Chạy test để xác nhận FAIL**

Run: `pytest tests/features/test_lagged.py -v`
Expected: FAIL (ModuleNotFoundError: `lagged`).

- [ ] **Step 3: Viết `lagged.py`**

Tạo `src/gold_pipeline/features/lagged.py`:

```python
"""Lagged features — stationary inputs only, shifted per source (no cross-source bleed)."""
from __future__ import annotations

import pandas as pd

from .config import DEFAULT_CONFIG, FeatureConfig, _alias


def add_lagged(df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG) -> pd.DataFrame:
    out = df.sort_values(["source", "date"]).reset_index(drop=True)
    grouped = out.groupby("source")
    for col in cfg.lag_columns:
        for k in cfg.lags:
            out[f"{_alias(col)}_lag_{k}"] = grouped[col].shift(k)
    return out
```

- [ ] **Step 4: Chạy test để xác nhận PASS**

Run: `pytest tests/features/test_lagged.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/features/lagged.py tests/features/test_lagged.py
git commit -m "feat: Stage 3 stationary lagged features (per-source shift)"
```

---

### Task 4: `macro_features.py` — pivot long→wide + confidence flags

**Files:**
- Create: `src/gold_pipeline/features/macro_features.py`
- Test: `tests/features/test_macro_features.py`

**Interfaces:**
- Consumes: `MACRO_SERIES_IDS` từ `config`.
- Produces: `build_macro_wide(macro_df: pd.DataFrame) -> pd.DataFrame`
  — đầu vào là `staging.macro_aligned` long-form (cột `date, series_id, value, release_date, is_imputed, days_stale, is_anomaly`). Trả wide keyed by `date` với cột `dgs10`, `dtwexbgs`, `cpiaucsl`, `{sid}_is_imputed`, `{sid}_is_anomaly`. `date` ép `datetime64[ns]`.

- [ ] **Step 1: Viết test thất bại**

Tạo `tests/features/test_macro_features.py`:

```python
import pandas as pd
from gold_pipeline.features.macro_features import build_macro_wide


def _long():
    # Two trading days, two series.
    rows = []
    for d in ["2020-01-01", "2020-01-02"]:
        rows.append({"date": d, "series_id": "DGS10", "value": 1.5,
                     "release_date": "2019-12-31", "is_imputed": True,
                     "days_stale": 2, "is_anomaly": False})
        rows.append({"date": d, "series_id": "DTWEXBGS", "value": 120.0,
                     "release_date": "2020-01-01", "is_imputed": False,
                     "days_stale": 0, "is_anomaly": False})
        rows.append({"date": d, "series_id": "CPIAUCSL", "value": 260.0,
                     "release_date": "2019-12-15", "is_imputed": True,
                     "days_stale": 18, "is_anomaly": False})
    return pd.DataFrame(rows)


def test_pivots_values_to_lowercased_columns():
    wide = build_macro_wide(_long())
    assert wide.loc[wide["date"] == pd.Timestamp("2020-01-01"), "dgs10"].iloc[0] == 1.5
    assert "dtwexbgs" in wide.columns and "cpiaucsl" in wide.columns
    assert len(wide) == 2  # one row per date


def test_carries_confidence_flags():
    wide = build_macro_wide(_long())
    row = wide[wide["date"] == pd.Timestamp("2020-01-01")].iloc[0]
    assert bool(row["dgs10_is_imputed"]) is True
    assert bool(row["dtwexbgs_is_imputed"]) is False
    assert bool(row["cpiaucsl_is_anomaly"]) is False
```

- [ ] **Step 2: Chạy test để xác nhận FAIL**

Run: `pytest tests/features/test_macro_features.py -v`
Expected: FAIL (ModuleNotFoundError: `macro_features`).

- [ ] **Step 3: Viết `macro_features.py`**

Tạo `src/gold_pipeline/features/macro_features.py`:

```python
"""Pivot the long staging.macro_aligned into a wide, per-date frame.

Stage 2 already point-in-time aligned macro onto trading days, so here we only
reshape long→wide (one column per series) and carry the confidence flags. We do
NOT shift or re-align — that would risk re-introducing look-ahead.
"""
from __future__ import annotations

import pandas as pd

from .config import MACRO_SERIES_IDS


def build_macro_wide(macro_df: pd.DataFrame) -> pd.DataFrame:
    df = macro_df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

    wide = pd.DataFrame({"date": pd.Series(sorted(df["date"].unique()))})
    for sid in MACRO_SERIES_IDS:
        col = sid.lower()
        sub = df[df["series_id"] == sid][["date", "value", "is_imputed", "is_anomaly"]]
        sub = sub.rename(columns={
            "value": col,
            "is_imputed": f"{col}_is_imputed",
            "is_anomaly": f"{col}_is_anomaly",
        })
        wide = wide.merge(sub, on="date", how="left")
    return wide
```

- [ ] **Step 4: Chạy test để xác nhận PASS**

Run: `pytest tests/features/test_macro_features.py -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/features/macro_features.py tests/features/test_macro_features.py
git commit -m "feat: Stage 3 macro pivot long->wide with confidence flags"
```

---

### Task 5: `target.py` — multi-horizon future returns via groupby

**Files:**
- Create: `src/gold_pipeline/features/target.py`
- Test: `tests/features/test_target.py`

**Interfaces:**
- Consumes: `FeatureConfig` từ `config`.
- Produces: `add_targets(df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG) -> pd.DataFrame`
  — thêm cột `target_logret_{h}` cho mỗi `h` trong `cfg.horizons`. Đầu vào phải có `source, log_return`. Shift qua `groupby("source")`.

- [ ] **Step 1: Viết test thất bại**

Tạo `tests/features/test_target.py`:

```python
import numpy as np
import pandas as pd
from gold_pipeline.features.config import DEFAULT_CONFIG
from gold_pipeline.features.target import add_targets


def _df(n, source="GC=F", start="2020-01-01", base=0.0):
    dates = pd.bdate_range(start, periods=n)
    return pd.DataFrame({
        "date": dates, "source": source,
        "log_return": [base + i for i in range(n)],
    })


def test_target_is_future_return():
    out = add_targets(_df(6)).reset_index(drop=True)
    # target_logret_1 at row k equals log_return at row k+1
    assert out.loc[2, "target_logret_1"] == out.loc[3, "log_return"]
    # tail rows lack a future value
    assert np.isnan(out.loc[5, "target_logret_1"])
    assert np.isnan(out.loc[5, "target_logret_5"])


def test_all_horizons_present():
    out = add_targets(_df(8))
    assert "target_logret_1" in out.columns
    assert "target_logret_5" in out.columns


def test_target_does_not_bleed_across_sources():
    a = _df(4, source="GC=F", base=0.0)
    b = _df(4, source="XAU/USD", base=100.0)
    out = add_targets(pd.concat([a, b], ignore_index=True))
    a_out = out[out["source"] == "GC=F"].reset_index(drop=True)
    # Last row of source A must be NaN, NOT the first value of source B.
    assert np.isnan(a_out.loc[3, "target_logret_1"])
```

- [ ] **Step 2: Chạy test để xác nhận FAIL**

Run: `pytest tests/features/test_target.py -v`
Expected: FAIL (ModuleNotFoundError: `target`).

- [ ] **Step 3: Viết `target.py`**

Tạo `src/gold_pipeline/features/target.py`:

```python
"""Targets — the ONLY place future values appear. Future return per source.

groupby("source") shift(-h) so the tail of one source never borrows the head of
the next source's history (look-ahead at the join boundary).
"""
from __future__ import annotations

import pandas as pd

from .config import DEFAULT_CONFIG, FeatureConfig


def add_targets(df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG) -> pd.DataFrame:
    out = df.sort_values(["source", "date"]).reset_index(drop=True)
    grouped = out.groupby("source")
    for h in cfg.horizons:
        out[f"target_logret_{h}"] = grouped["log_return"].shift(-h)
    return out
```

- [ ] **Step 4: Chạy test để xác nhận PASS**

Run: `pytest tests/features/test_target.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/features/target.py tests/features/test_target.py
git commit -m "feat: Stage 3 multi-horizon targets (per-source shift)"
```

---

### Task 6: `assemble.py` — combine + readiness flags

**Files:**
- Create: `src/gold_pipeline/features/assemble.py`
- Test: `tests/features/test_assemble.py`

**Interfaces:**
- Consumes: `add_technical`, `add_lagged`, `add_targets`, `build_macro_wide`, và các hàm cột của `config` (`feature_table_columns`, `warmup_columns`).
- Produces: `assemble_features(gold_df, macro_df, cfg=DEFAULT_CONFIG) -> pd.DataFrame`
  — trả DataFrame đúng thứ tự cột `feature_table_columns(cfg)`, có `has_features` (mọi cột warmup non-NaN) và `has_target_{h}` (target horizon đó non-NaN). Không drop dòng. `date` là `datetime64[ns]`.

- [ ] **Step 1: Viết test thất bại**

Tạo `tests/features/test_assemble.py`:

```python
import numpy as np
import pandas as pd
from gold_pipeline.features.assemble import assemble_features
from gold_pipeline.features.config import DEFAULT_CONFIG, feature_table_columns


def _gold(n, source="GC=F", start="2018-01-01"):
    dates = pd.bdate_range(start, periods=n)
    closes = list(100 + np.arange(n) * 0.5)
    logret = [np.nan] + list(np.diff(np.log(closes)))
    return pd.DataFrame({"date": dates, "close": closes, "log_return": logret, "source": source})


def _macro(dates):
    rows = []
    for d in dates:
        for sid, val in [("DGS10", 1.5), ("DTWEXBGS", 120.0), ("CPIAUCSL", 260.0)]:
            rows.append({"date": d, "series_id": sid, "value": val,
                         "release_date": d, "is_imputed": False,
                         "days_stale": 0, "is_anomaly": False})
    return pd.DataFrame(rows)


def test_output_columns_match_schema_exactly():
    g = _gold(60)
    out = assemble_features(g, _macro(g["date"]))
    assert list(out.columns) == feature_table_columns(DEFAULT_CONFIG)


def test_has_features_false_during_warmup_true_after():
    g = _gold(60)
    out = assemble_features(g, _macro(g["date"])).reset_index(drop=True)
    assert out.loc[0, "has_features"] == False  # noqa: E712 (head warmup)
    assert out.loc[59, "has_features"] == True   # noqa: E712 (enough history)


def test_has_target_false_at_tail():
    g = _gold(60)
    out = assemble_features(g, _macro(g["date"])).reset_index(drop=True)
    assert out.loc[59, "has_target_1"] == False  # noqa: E712 (no future row)
    assert out.loc[59, "has_target_5"] == False  # noqa: E712


def test_no_rows_dropped():
    g = _gold(60)
    out = assemble_features(g, _macro(g["date"]))
    assert len(out) == 60
```

- [ ] **Step 2: Chạy test để xác nhận FAIL**

Run: `pytest tests/features/test_assemble.py -v`
Expected: FAIL (ModuleNotFoundError: `assemble`).

- [ ] **Step 3: Viết `assemble.py`**

Tạo `src/gold_pipeline/features/assemble.py`:

```python
"""Assemble the wide feature table: gold features + macro + targets + readiness flags.

Order of ops matters: technical (adds rsi_14) BEFORE lagged (lags rsi_14).
All date columns are normalized to tz-naive datetime64 before the macro merge so
a DATE-vs-Timestamp dtype mismatch can't silently empty the join.
"""
from __future__ import annotations

import pandas as pd

from .config import DEFAULT_CONFIG, FeatureConfig, feature_table_columns, warmup_columns
from .lagged import add_lagged
from .macro_features import build_macro_wide
from .target import add_targets
from .technical import add_technical


def assemble_features(
    gold_df: pd.DataFrame, macro_df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG
) -> pd.DataFrame:
    gold = gold_df.copy()
    gold["date"] = pd.to_datetime(gold["date"]).dt.tz_localize(None)

    df = add_technical(gold, cfg)   # adds rsi_14 etc.
    df = add_lagged(df, cfg)        # lags rsi_14/log_return
    df = add_targets(df, cfg)

    macro_wide = build_macro_wide(macro_df)  # date already tz-naive datetime64
    df = df.merge(macro_wide, on="date", how="left")

    df["has_features"] = df[warmup_columns(cfg)].notna().all(axis=1)
    for h in cfg.horizons:
        df[f"has_target_{h}"] = df[f"target_logret_{h}"].notna()

    return df[feature_table_columns(cfg)].sort_values(["source", "date"]).reset_index(drop=True)
```

- [ ] **Step 4: Chạy test để xác nhận PASS**

Run: `pytest tests/features/test_assemble.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/features/assemble.py tests/features/test_assemble.py
git commit -m "feat: Stage 3 assemble wide feature table + readiness flags"
```

---

### Task 7: `quality.py` — fail-fast runtime gate

**Files:**
- Create: `src/gold_pipeline/features/quality.py`
- Test: `tests/features/test_quality.py`

**Interfaces:**
- Consumes: `FeatureConfig`, `feature_table_columns`, `warmup_columns` từ `config`.
- Produces: `FeatureQualityError(Exception)` và `check_features(df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG) -> None` — raise nếu vi phạm; trả `None` nếu ổn (flags không phải lỗi).

- [ ] **Step 1: Viết test thất bại**

Tạo `tests/features/test_quality.py`:

```python
import numpy as np
import pandas as pd
import pytest
from gold_pipeline.features.assemble import assemble_features
from gold_pipeline.features.config import DEFAULT_CONFIG
from gold_pipeline.features.quality import FeatureQualityError, check_features
from tests.features.test_assemble import _gold, _macro


def _good():
    g = _gold(60)
    return assemble_features(g, _macro(g["date"]))


def test_valid_frame_passes():
    check_features(_good())  # no raise


def test_duplicate_key_raises():
    df = _good()
    df = pd.concat([df, df.iloc[[59]]], ignore_index=True)
    with pytest.raises(FeatureQualityError, match="duplicate"):
        check_features(df)


def test_null_key_raises():
    df = _good()
    df.loc[0, "source"] = None
    with pytest.raises(FeatureQualityError, match="NULL"):
        check_features(df)


def test_missing_column_raises():
    df = _good().drop(columns=["rsi_14"])
    with pytest.raises(FeatureQualityError, match="column"):
        check_features(df)


def test_target_not_matching_future_return_raises():
    df = _good()
    # Corrupt a target so it no longer equals the next-row log_return.
    df.loc[10, "target_logret_1"] = 999.0
    with pytest.raises(FeatureQualityError, match="target"):
        check_features(df)


def test_unexpected_nan_outside_warmup_raises():
    df = _good()
    df.loc[59, "rsi_14"] = np.nan  # row 59 is past warmup -> illegal NaN
    with pytest.raises(FeatureQualityError, match="NaN"):
        check_features(df)
```

- [ ] **Step 2: Chạy test để xác nhận FAIL**

Run: `pytest tests/features/test_quality.py -v`
Expected: FAIL (ModuleNotFoundError: `quality`).

- [ ] **Step 3: Viết `quality.py`**

Tạo `src/gold_pipeline/features/quality.py`:

```python
"""Fail-fast runtime gate for the features layer.

Causality (no-look-ahead) is proven by unit-test perturbation, NOT here — on a
static snapshot we can only assert observable invariants: keys, column set,
target == future return, and that NaN appears only where a readiness flag is False.
"""
from __future__ import annotations

import pandas as pd

from .config import DEFAULT_CONFIG, FeatureConfig, feature_table_columns, warmup_columns


class FeatureQualityError(Exception):
    pass


def check_features(df: pd.DataFrame, cfg: FeatureConfig = DEFAULT_CONFIG) -> None:
    # 1. Keys.
    nulls = df[["date", "source"]].isna().sum()
    bad = nulls[nulls > 0]
    if not bad.empty:
        raise FeatureQualityError(f"NULL in key columns: {bad.to_dict()}")
    n_dup = int(df.duplicated(subset=["date", "source"]).sum())
    if n_dup:
        raise FeatureQualityError(f"{n_dup} duplicate (date, source) rows")

    # 2. Column set == schema (catches config<->migration drift).
    expected = feature_table_columns(cfg)
    if list(df.columns) != expected:
        missing = set(expected) - set(df.columns)
        extra = set(df.columns) - set(expected)
        raise FeatureQualityError(f"column mismatch: missing={missing}, extra={extra}")

    # 3. Monotonic date per source.
    for source, g in df.groupby("source"):
        if not pd.to_datetime(g["date"]).is_monotonic_increasing:
            raise FeatureQualityError(f"date not monotonic for source {source}")

    # 4. Target == future return, per horizon, per source.
    for source, g in df.groupby("source"):
        g = g.sort_values("date")
        for h in cfg.horizons:
            expected_t = g["log_return"].shift(-h)
            got = g[f"target_logret_{h}"]
            both = expected_t.notna() & got.notna()
            if not (got[both].to_numpy() == expected_t[both].to_numpy()).all():
                raise FeatureQualityError(f"target_logret_{h} != future log_return ({source})")

    # 5. NaN only where a readiness flag is False.
    wcols = warmup_columns(cfg)
    feat_ready = df["has_features"].astype(bool)
    if df.loc[feat_ready, wcols].isna().any().any():
        raise FeatureQualityError("NaN in warmup columns where has_features is True")
    for h in cfg.horizons:
        ready = df[f"has_target_{h}"].astype(bool)
        if df.loc[ready, f"target_logret_{h}"].isna().any():
            raise FeatureQualityError(f"NaN target_logret_{h} where has_target_{h} is True")
```

- [ ] **Step 4: Chạy test để xác nhận PASS**

Run: `pytest tests/features/test_quality.py -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add src/gold_pipeline/features/quality.py tests/features/test_quality.py
git commit -m "feat: Stage 3 features quality gate (keys, schema, target, NaN)"
```

---

### Task 8: Migration + `run.py` + DB integration

**Files:**
- Create: `db/migrations/003_features_schema.sql`
- Create: `src/gold_pipeline/features/run.py`
- Create: `src/gold_pipeline/features/CLAUDE.md`
- Test: `tests/db/test_features_run.py`

**Interfaces:**
- Consumes: `read_table`, `run_migrations`, `upsert_dataframe` (`gold_pipeline.db`); `Settings` (`gold_pipeline.ingestion.config`); `assemble_features`, `check_features`.
- Produces: `run_features(engine, gold_reader, macro_reader, cfg=DEFAULT_CONFIG) -> dict[str, int]`
  — đọc staging qua seam, assemble, check, UPSERT vào `features.gold_features`, trả `{"gold_features": n}`.

- [ ] **Step 1: Viết migration**

Tạo `db/migrations/003_features_schema.sql` (cột phải khớp `feature_table_columns(DEFAULT_CONFIG)`):

```sql
CREATE SCHEMA IF NOT EXISTS features;

-- Wide, model-ready feature table. Columns MUST match
-- gold_pipeline.features.config.feature_table_columns(DEFAULT_CONFIG).
-- Changing windows/lags/horizons that add or rename columns requires a new migration.
CREATE TABLE IF NOT EXISTS features.gold_features (
    date            DATE NOT NULL,
    source          TEXT NOT NULL,
    close           NUMERIC(12,4),
    log_return      NUMERIC(12,8),
    sma_10          NUMERIC(14,6),
    sma_20          NUMERIC(14,6),
    ema_12          NUMERIC(14,6),
    ema_26          NUMERIC(14,6),
    rsi_14          NUMERIC(14,6),
    macd            NUMERIC(14,6),
    macd_signal     NUMERIC(14,6),
    macd_diff       NUMERIC(14,6),
    bb_high         NUMERIC(14,6),
    bb_mid          NUMERIC(14,6),
    bb_low          NUMERIC(14,6),
    close_to_sma_10 NUMERIC(14,6),
    close_to_sma_20 NUMERIC(14,6),
    dgs10           NUMERIC(14,6),
    dtwexbgs        NUMERIC(14,6),
    cpiaucsl        NUMERIC(14,6),
    dgs10_is_imputed     BOOLEAN,
    dgs10_is_anomaly     BOOLEAN,
    dtwexbgs_is_imputed  BOOLEAN,
    dtwexbgs_is_anomaly  BOOLEAN,
    cpiaucsl_is_imputed  BOOLEAN,
    cpiaucsl_is_anomaly  BOOLEAN,
    logret_lag_1    NUMERIC(12,8),
    logret_lag_2    NUMERIC(12,8),
    logret_lag_3    NUMERIC(12,8),
    logret_lag_5    NUMERIC(12,8),
    rsi_14_lag_1    NUMERIC(14,6),
    rsi_14_lag_2    NUMERIC(14,6),
    rsi_14_lag_3    NUMERIC(14,6),
    rsi_14_lag_5    NUMERIC(14,6),
    target_logret_1 NUMERIC(12,8),
    target_logret_5 NUMERIC(12,8),
    has_features    BOOLEAN NOT NULL DEFAULT false,
    has_target_1    BOOLEAN NOT NULL DEFAULT false,
    has_target_5    BOOLEAN NOT NULL DEFAULT false,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, source)
);
```

- [ ] **Step 2: Viết `run.py`**

Tạo `src/gold_pipeline/features/run.py`:

```python
"""Stage 3 orchestrator: read staging -> assemble -> quality-check -> UPSERT features.

Run with:  python -m gold_pipeline.features.run
"""
from __future__ import annotations

import logging
from pathlib import Path

from ..db.reader import read_table
from ..db.writer import run_migrations, upsert_dataframe
from ..ingestion.config import Settings
from .assemble import assemble_features
from .config import DEFAULT_CONFIG, FeatureConfig
from .quality import check_features

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("features.run")

_MIGRATIONS = Path(__file__).resolve().parents[3] / "db" / "migrations"


def run_features(engine, gold_reader, macro_reader, cfg: FeatureConfig = DEFAULT_CONFIG) -> dict[str, int]:
    """Transform staging -> features. gold_reader/macro_reader are zero-arg seams."""
    gold = gold_reader()
    macro = macro_reader()
    if gold.empty:
        raise ValueError("staging.gold_prices is empty; run Stage 2 first")

    feats = assemble_features(gold, macro, cfg)
    check_features(feats, cfg)

    n = upsert_dataframe(engine, feats, "gold_features", "features", ["date", "source"])
    counts = {"gold_features": n}
    log.info("featurized %s", counts)
    return counts


def main() -> None:
    from sqlalchemy import create_engine

    settings = Settings.from_env()
    engine = create_engine(settings.database_url)
    run_migrations(engine, _MIGRATIONS)

    def gold_reader():
        return read_table(engine, "staging", "gold_prices")

    def macro_reader():
        return read_table(engine, "staging", "macro_aligned")

    run_features(engine, gold_reader=gold_reader, macro_reader=macro_reader)


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Viết DB integration test (thất bại trước)**

Tạo `tests/db/test_features_run.py`:

```python
import os

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from gold_pipeline.db.writer import run_migrations, upsert_dataframe
from gold_pipeline.features.run import _MIGRATIONS, run_features

pytestmark = pytest.mark.skipif(
    not os.environ.get("TEST_DATABASE_URL"), reason="needs TEST_DATABASE_URL"
)


def _seed_staging(engine):
    dates = pd.bdate_range("2018-01-01", periods=60)
    closes = list(100 + np.arange(60) * 0.5)
    logret = [np.nan] + list(np.diff(np.log(closes)))
    gold = pd.DataFrame({
        "date": dates, "open": closes, "high": [c * 1.01 for c in closes],
        "low": [c * 0.99 for c in closes], "close": closes, "volume": [100] * 60,
        "log_return": logret, "is_outlier": [False] * 60, "source": "GC=F",
    })
    macro_rows = []
    for d in dates:
        for sid, val in [("DGS10", 1.5), ("DTWEXBGS", 120.0), ("CPIAUCSL", 260.0)]:
            macro_rows.append({"date": d, "series_id": sid, "value": val,
                               "release_date": d, "is_imputed": False,
                               "days_stale": 0, "is_anomaly": False})
    macro = pd.DataFrame(macro_rows)
    upsert_dataframe(engine, gold, "gold_prices", "staging", ["date", "source"])
    upsert_dataframe(engine, macro, "macro_aligned", "staging", ["date", "series_id"])


def test_run_features_idempotent_upsert():
    engine = create_engine(os.environ["TEST_DATABASE_URL"])
    run_migrations(engine, _MIGRATIONS)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE staging.gold_prices, staging.macro_aligned, features.gold_features"))
    _seed_staging(engine)

    def gold_reader():
        return pd.read_sql_table("gold_prices", engine, schema="staging")

    def macro_reader():
        return pd.read_sql_table("macro_aligned", engine, schema="staging")

    first = run_features(engine, gold_reader, macro_reader)
    second = run_features(engine, gold_reader, macro_reader)  # re-run
    assert first["gold_features"] == 60
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT count(*) FROM features.gold_features")).scalar()
    assert rows == 60  # idempotent: no duplication
```

- [ ] **Step 4: Chạy DB test để xác nhận FAIL rồi PASS**

Run: `docker compose up -d` (nếu chưa chạy), rồi
`TEST_DATABASE_URL="postgresql+psycopg2://gold:gold@localhost:5432/gold_test" pytest tests/db/test_features_run.py -v`
Expected: PASS (1 test). Nếu FAIL vì lệch cột giữa migration và `feature_table_columns`, sửa cho khớp (test `check_features` ở Task 7 đã chặn lệch này) rồi chạy lại.

- [ ] **Step 5: Viết `features/CLAUDE.md`**

Tạo `src/gold_pipeline/features/CLAUDE.md`:

```markdown
# features/ — Stage 3 of the gold pipeline

Reads the `staging` layer and writes the wide, model-ready `features.gold_features`.
See the design spec: docs/superpowers/specs/2026-06-29-stage3-feature-engineering-design.md

## Run
- `pip install -e ".[dev]"`, `docker compose up -d`, then
  `python -m gold_pipeline.features.run` (NEVER `python src/.../run.py` — src layout).
  Requires Stage 1 + 2 to have populated `staging` first.

## Boundaries (one job each)
- `config.py` — FeatureConfig + the canonical column list (source of truth; must match migration 003).
- `technical.py` — per-source `ta` indicators + stationary `close_to_sma_*` ratios.
- `lagged.py` — stationary lags (`log_return`, `rsi_14`) via groupby shift(+k).
- `macro_features.py` — pivot staging.macro_aligned long->wide + carry is_imputed/is_anomaly.
- `target.py` — multi-horizon `target_logret_{h}` = groupby shift(-h) (only future-looking column).
- `assemble.py` — combine + readiness flags (has_features / has_target_{h}); normalizes date dtype.
- `quality.py` — fail-fast gate before any write.
- `run.py` — wires the above; `run_features(...)` takes injectable reader seams for tests.

## Invariants
- No look-ahead: every shift goes through groupby("source"); only targets see the future.
- Stationary inputs: never lag absolute `close`; use returns/ratios/oscillators.
- Flag, don't mutate: keep NaN -> NULL, add flags; never bfill/interpolate; never drop rows.
- config <-> schema: feature_table_columns must equal migration 003 columns (quality gate enforces).
- Idempotent: composite-PK UPSERT. Fail-fast: empty staging or failed check raises before any write.
```

- [ ] **Step 6: Cập nhật CLAUDE.md gốc (Commands Stage 3)**

Trong `CLAUDE.md` (root), sau mục "## Commands (Stage 2)", thêm:

```markdown
## Commands (Stage 3)

- Run feature engineering: `python -m gold_pipeline.features.run` (needs `staging` populated by Stage 2)
- Unit tests (no DB): `pytest -q tests/features`
- DB integration: `TEST_DATABASE_URL="postgresql+psycopg2://gold:gold@localhost:5432/gold_test" pytest -q tests/db/test_features_run.py`
```

- [ ] **Step 7: Chạy toàn bộ unit test Stage 3**

Run: `pytest -q tests/features`
Expected: PASS toàn bộ.

- [ ] **Step 8: Commit**

```bash
git add db/migrations/003_features_schema.sql src/gold_pipeline/features/run.py src/gold_pipeline/features/CLAUDE.md tests/db/test_features_run.py CLAUDE.md
git commit -m "feat: Stage 3 run orchestrator + features schema migration + DB test"
```

---

## Self-Review

**Spec coverage:**
- Phạm vi (kỹ thuật + lagged + macro, no sentiment) → Task 2,3,4. ✓
- Target multi-horizon → Task 5 + config. ✓
- Output wide `(date, source)` → Task 6 + migration Task 8. ✓
- NaN giữ + cờ `has_features`/`has_target_{h}` → Task 6. ✓
- Cờ tin cậy macro → Task 4. ✓
- Ràng buộc config↔schema → config (Task 1) + quality column check (Task 7) + migration (Task 8). ✓
- Boundary bleed (groupby shift) → Task 3,5 + tests. ✓
- Stationarity (no close lag, ratio features) → Task 1,2,3. ✓
- date dtype normalize → Task 4,6. ✓
- Causality qua unit perturbation; runtime gate logic → Task 2 (perturbation) + Task 7. ✓
- Idempotent UPSERT + fail-fast → Task 8 + run.py. ✓
- `ta` library, loop per source → Task 1 dep + Task 2. ✓

**Placeholder scan:** không có TODO/TBD; mọi step có code/command cụ thể.

**Type consistency:** `add_technical`/`add_lagged`/`add_targets`/`build_macro_wide`/`assemble_features`/`check_features`/`run_features` tên & chữ ký nhất quán giữa các task; cột do `config.feature_table_columns` định nghĩa được dùng đồng nhất ở assemble, quality, migration.
