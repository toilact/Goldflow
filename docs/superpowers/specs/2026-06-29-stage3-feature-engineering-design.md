# Stage 3 — Feature Engineering (design)

Date: 2026-06-29 · Status: approved, pre-implementation

## Mục tiêu

Biến lớp `staging` (gold đã sạch + macro đã point-in-time) thành một bảng **feature dày, wide,
model-ready** trong schema `features`. Đây là nguyên liệu trực tiếp cho Stage 4 (packaging) và các
mô hình ML (LSTM / XGBoost / Random Forest).

Stage 3 **không** train mô hình, **không** scale/split (đó là Stage 4). Nó chỉ tạo feature + target
và ghi vào `features.gold_features`.

## Phạm vi (đã chốt)

- **Trong phạm vi:** chỉ báo kỹ thuật, lagged features, macro features (pivot từ staging).
- **Ngoài phạm vi lần này:** sentiment/tin tức — Stage 1 chưa ingest tin tức nên hoãn lại; thêm sau
  như một nhóm cột mới mà không phá schema.
- Thư viện chỉ báo: **`ta`** (đúng tech stack; ưu tiên hơn `pandas_ta`). Các indicator của `ta` là
  *causal* (rolling lùi về quá khứ) nên an toàn leakage; test sẽ khẳng định điều này.

## Bất biến (kế thừa cross-cutting của dự án)

- **Point-in-time:** macro đã được Stage 2 reindex theo `release_date`; Stage 3 chỉ pivot + join theo
  `date`, không shift thêm. Không tái tạo rủi ro look-ahead ở macro.
- **No look-ahead / leakage:**
  - Chỉ báo & lag dùng **quá khứ**: rolling causal, `shift(+k)`.
  - Target là **chỗ DUY NHẤT** nhìn tương lai: `log_return.shift(-h)`.
  - Forward-fill only — không bfill/interpolate. NaN warmup giữ nguyên (xem dưới).
- **Flag, đừng mutate:** không sửa giá; chỉ thêm cột feature + cờ. Staging vẫn bất biến.
- **Idempotent:** UPSERT theo composite PK `(date, source)`; chạy lại cùng ngày không nhân đôi dòng.
- **Fail-fast:** nguồn rỗng hoặc quality check fail → raise trước mọi ghi DB.

## Quyết định thiết kế (đã chốt khi brainstorm)

1. **Phạm vi:** kỹ thuật + lagged + macro, không sentiment.
2. **Target:** future log-return, horizon `h` cấu hình được (mặc định `h=1`) → cột `target_logret_{h}`.
3. **Output shape:** một bảng **wide** `features.gold_features`, PK `(date, source)`. Giữ `source` để
   sau thêm nguồn XAU/USD mà không phá schema.
4. **NaN ở biên:** giữ NaN → NULL trong DB + cột cờ (`is_warmup` đầu chuỗi, `has_target` cuối chuỗi);
   **không drop** ở Stage 3. Stage 4 mới quyết định cắt khi split. Đúng triết lý "flag don't mutate".
5. **Thư viện:** `ta` (Hướng A).

## Kiến trúc & ranh giới module

Package mới `src/gold_pipeline/features/`, theo pattern Stage 1/2 (mỗi file một việc, có seam inject
để test). Tái dùng `gold_pipeline.db` (writer/reader) — không đụng lại tầng DB.

| File | Một việc duy nhất |
|------|-------------------|
| `config.py` | Tham số feature: window indicator, danh sách lag, horizon `h`. |
| `technical.py` | Gold OHLCV (per `source`) → cột chỉ báo kỹ thuật qua `ta`, rolling past-only. |
| `lagged.py` | Lagged features `shift(+k)` cho `close`, `log_return`, các chỉ báo (chỉ quá khứ). |
| `macro_features.py` | Pivot `staging.macro_aligned` long→wide (`dgs10/dtwexbgs/cpiaucsl`), join theo `date`. |
| `target.py` | `target_logret_{h}` = `log_return.shift(-h)` (chỗ duy nhất nhìn tương lai). |
| `assemble.py` | Ghép gold-features + macro + lagged + target → 1 bảng wide; thêm cờ `is_warmup`/`has_target`. Không drop NaN. |
| `quality.py` | Gate fail-fast trước khi ghi (key, monotonic, anti-leakage, tính nhất quán cờ). |
| `run.py` | Wiring: đọc `staging` → assemble → quality → UPSERT `features.gold_features`. Seam reader cho test. |

Migration mới: `db/migrations/003_features_schema.sql` (idempotent `CREATE ... IF NOT EXISTS`,
theo pattern hiện có).

## Luồng dữ liệu

```
staging.gold_prices ─┐
                     ├─→ technical.py ──┐
                     └─→ lagged.py ─────┤
staging.macro_aligned ─→ macro_features ┤→ assemble.py → quality.py → UPSERT features.gold_features
        (date backbone từ gold) ────────┘        ↑ target.py
```

- Date backbone = các `date` của gold (như Stage 2). Macro join theo `date` (đã point-in-time sẵn).
- Mọi thao tác time-series group theo `source`.

## Schema bảng — `features.gold_features` (PK `(date, source)`)

| Nhóm cột | Ví dụ |
|----------|-------|
| Khóa | `date`, `source` |
| Giá gốc mang theo | `close`, `log_return` |
| Chỉ báo kỹ thuật | `sma_10`, `sma_20`, `ema_12`, `ema_26`, `rsi_14`, `macd`, `macd_signal`, `macd_diff`, `bb_high`, `bb_mid`, `bb_low` |
| Macro — giá trị (pivot) | `dgs10`, `dtwexbgs`, `cpiaucsl` |
| Macro — cờ tin cậy (pivot) | `dgs10_is_imputed`, `dgs10_is_anomaly`, `dtwexbgs_is_imputed`, `dtwexbgs_is_anomaly`, `cpiaucsl_is_imputed`, `cpiaucsl_is_anomaly` |
| Lagged | `close_lag_1`, `logret_lag_1`, `rsi_14_lag_1`, … (theo list lag) |
| Target | `target_logret_1` (tương lai) |
| Cờ / meta | `is_warmup` BOOLEAN, `has_target` BOOLEAN, `processed_at` TIMESTAMPTZ |

NaN → NULL trong DB (writer đã coerce NaN/NaT → None). Idempotent qua composite-PK UPSERT.

**Cờ tin cậy macro:** Stage 2 đã tính `is_imputed` (giá trị bị carry-forward vì chưa có release mới)
và `is_anomaly` (ngoài bound / quá hạn). Pivot kèm chúng để mô hình biết dữ liệu macro tại ngày đó có
đáng tin không. `days_stale` (numeric) tạm hoãn — có thể thêm sau như feature riêng nếu cần, tránh phình
cột ngay.

**Ràng buộc config ↔ schema (BẮT BUỘC):** PostgreSQL có schema tĩnh, còn `config.py` cho phép đổi
danh sách indicator/lag. Vì vậy `config.py` là **nguồn sự thật** nhưng PHẢI khớp đúng các cột tĩnh
trong `003_features_schema.sql`. Muốn thêm/bớt indicator hoặc đổi list lag ⇒ **bắt buộc viết migration
SQL mới** nâng cấp bảng; không được để code sinh cột mà DB không có (sẽ sập lúc UPSERT). Một test khẳng
định tập cột do pipeline sinh ra == tập cột schema.

## Indicator cụ thể & tham số (mặc định, cấu hình trong `config.py`)

| Indicator | Hàm `ta` | Cột |
|-----------|----------|-----|
| SMA 10, 20 | `trend.sma_indicator` | `sma_10`, `sma_20` |
| EMA 12, 26 | `trend.ema_indicator` | `ema_12`, `ema_26` |
| RSI 14 | `momentum.rsi` | `rsi_14` |
| MACD (12/26/9) | `trend.MACD` | `macd`, `macd_signal`, `macd_diff` |
| Bollinger (20, 2σ) | `volatility.BollingerBands` | `bb_high`, `bb_mid`, `bb_low` |

Tất cả tính trên `close`, **group theo `source`**, rolling lùi về quá khứ.

**Lưu ý implementation (`ta` + groupby):** `ta` nhận `Series`/`DataFrame`, **không** nhận object
`groupby`. `technical.py` phải **loop qua từng `source`**, tính chỉ báo trên chuỗi của nhóm đó rồi gộp
lại (hoặc dùng `groupby(...).transform(...)` cẩn thận) — tuyệt đối không trộn dữ liệu giữa các nguồn.

**`is_warmup` tính ĐỘNG, không hardcode:** mỗi indicator có độ warmup khác nhau, và `ta` dựng EMA/MACD
bằng `pandas.ewm().mean()` (phát giá trị ngay từ dòng 0, **không** NaN) trong khi RSI/SMA mới tạo NaN
warmup; thêm lag còn cộng dồn (vd `rsi_14_lag_5` cần 14+5 dòng). Vì vậy `is_warmup` của một dòng =
"dòng đó còn NaN ở bất kỳ cột feature nào do thiếu lịch sử" — tính theo số NaN thực tế lớn nhất per
`source`, **không** gán theo một con số cố định.

**Lag mặc định:** `[1, 2, 3, 5]` ngày, áp cho `close`, `log_return`, `rsi_14`.

## Anti-leakage: hai tầng kiểm tra khác nhau (làm rõ trách nhiệm)

Causality ("feature không nhìn quá `t`") **không** kiểm được lúc runtime trên một bảng tĩnh — chỉ có
một snapshot dữ liệu, không thể biết một hàm có lén dùng tương lai hay không. Vì vậy tách rõ:

- **Unit test (perturbation) — nơi DUY NHẤT kiểm causality:** đổi giá trị ngày `t+1`, khẳđịnh feature
  tại ngày `t` **không đổi**. Đây là cách chứng minh chỉ báo/lag chỉ dùng quá khứ.
- **Runtime gate (`quality.py`) — kiểm logic trên dữ liệu tĩnh:** không "chứng minh" causality, chỉ
  assert các bất biến quan sát được.

## Data Quality — `features/quality.py` (fail-fast, runtime)

- Key `(date, source)` không NULL, không trùng; `date` monotonic tăng theo từng `source`.
- Tập cột pipeline sinh ra == tập cột schema (bắt lệch config ↔ migration sớm).
- **Target logic:** cột `target_logret_{h}` của mỗi dòng khớp đúng `log_return.shift(-h)` theo vị trí
  per `source` (xác nhận target nhìn đúng `h` ngày tương lai).
- **Tính nhất quán cờ/NaN:** `is_warmup` đúng = các dòng còn NaN feature do thiếu lịch sử;
  `has_target` đúng = các dòng cuối thiếu target. NaN chỉ được phép ở vùng đã cờ hoá; NaN ngoài vùng
  đó → raise.
- Flag (`is_warmup`/`has_target`/macro flags) **không** phải là lỗi — dòng cờ vẫn pass và được ghi.

## Chiến lược test (TDD, tách DB như Stage 2)

- **Unit (no DB):** mỗi module một file —
  - `test_technical`: giá trị khớp `ta`; **perturbation** — đổi giá trị `t+1`, feature tại `t` không
    đổi (causality); tính độc lập per `source`.
  - `test_lagged`: lag là quá khứ, không lệch tương lai (perturbation tương tự).
  - `test_macro_features`: pivot giá trị đúng; cờ `is_imputed`/`is_anomaly` đi kèm đúng series;
    point-in-time giữ nguyên.
  - `test_target`: `shift(-h)` đúng; đuôi NaN.
  - `test_assemble`: ghép đúng cột; `is_warmup` tính động đúng theo NaN; `has_target` đúng; không drop;
    tập cột == schema.
  - `test_quality`: mỗi nhánh raise được cover.
- **DB integration:** UPSERT idempotent vào `features.gold_features` (chạy lại không nhân đôi).
- Tham chiếu guard `ml-data-leakage-guard` khi code `target.py`/`lagged.py`.

## Run

- `pip install -e ".[dev]"` (thêm `ta` vào dependencies), `docker compose up -d`, đảm bảo Stage 1 + 2
  đã chạy (`raw` + `staging` có dữ liệu), rồi `python -m gold_pipeline.features.run`.
- Unit tests (no DB): `pytest -q -k "not test_writer and not test_reader"`.
- DB integration: `TEST_DATABASE_URL=... pytest -q tests/db`.

## Ngoài phạm vi (để sau)

- Sentiment/news features (cần mở rộng Stage 1 để ingest tin tức trước).
- Multi-source thực sự (XAU/USD): schema đã giữ `source` để mở rộng, nhưng dữ liệu chưa có.
- Scaling, train/val/test split, dataset generator → Stage 4.
