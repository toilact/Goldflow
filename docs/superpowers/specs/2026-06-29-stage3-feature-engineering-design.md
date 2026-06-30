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
2. **Target:** future log-return, **multi-horizon** — `horizons` cấu hình được (mặc định `[1, 5]`) →
   sinh đồng thời các cột `target_logret_{h}` (vd `target_logret_1`, `target_logret_5`). Mỗi horizon có
   cờ `has_target_{h}` riêng (đuôi NaN khác nhau).
3. **Output shape:** một bảng **wide** `features.gold_features`, PK `(date, source)`. Giữ `source` để
   sau thêm nguồn XAU/USD mà không phá schema.
4. **NaN ở biên:** giữ NaN → NULL trong DB + cột cờ hướng "sẵn sàng" (`has_features` đầu chuỗi,
   `has_target_{h}` cuối chuỗi); **không drop** ở Stage 3. Stage 4 mới quyết định cắt khi split. Đúng
   triết lý "flag don't mutate". Lọc downstream gọn: `df[df.has_features & df.has_target_1]`.
5. **Thư viện:** `ta` (Hướng A).

## Kiến trúc & ranh giới module

Package mới `src/gold_pipeline/features/`, theo pattern Stage 1/2 (mỗi file một việc, có seam inject
để test). Tái dùng `gold_pipeline.db` (writer/reader) — không đụng lại tầng DB.

| File | Một việc duy nhất |
|------|-------------------|
| `config.py` | Tham số feature: window indicator, danh sách lag (stationary), `horizons` list. |
| `technical.py` | Gold OHLCV (per `source`) → chỉ báo kỹ thuật + feature tỷ lệ stationary qua `ta`, rolling past-only. |
| `lagged.py` | Lagged features `groupby("source")[col].shift(+k)` cho `log_return`, `rsi_14` (chỉ quá khứ, stationary). |
| `macro_features.py` | Pivot `staging.macro_aligned` long→wide (`dgs10/dtwexbgs/cpiaucsl` + cờ tin cậy), join theo `date`. |
| `target.py` | `target_logret_{h}` = `groupby("source")["log_return"].shift(-h)` cho mỗi `h` trong `horizons` (chỗ duy nhất nhìn tương lai). |
| `assemble.py` | Ghép gold-features + macro + lagged + target → 1 bảng wide; chuẩn hoá `date` đồng kiểu trước merge; thêm cờ `has_features`/`has_target_{h}`. Không drop NaN. |
| `quality.py` | Gate fail-fast trước khi ghi (key, monotonic, target logic per-horizon, tính nhất quán cờ, cột==schema). |
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

**⚠️ Boundary bleed — bắt buộc shift qua groupby:** bảng gộp nhiều nguồn (GC=F, sau này XAU/USD) xếp
nối tiếp. Gọi `.shift()` trực tiếp trên cột toàn cục sẽ cho dòng cuối nguồn này lấy nhầm giá trị dòng
đầu nguồn kế tiếp → **look-ahead leakage** ở điểm giao. Vì vậy MỌI phép dịch (target `shift(-h)` và
lag `shift(+k)`) PHẢI đi qua `groupby("source")[col].shift(...)`, không bao giờ shift trên DataFrame
phẳng. `assemble.py` cũng phải chuẩn hoá mọi cột `date` về `pd.to_datetime()` (tz-naive) trước khi
merge — gold đọc từ DB có thể là `datetime64[ns]` còn `merge_asof`/`date` có thể là `datetime.date`;
lệch kiểu khiến merge trả rỗng hoặc lỗi.

## Schema bảng — `features.gold_features` (PK `(date, source)`)

| Nhóm cột | Ví dụ |
|----------|-------|
| Khóa | `date`, `source` |
| Giá gốc tham chiếu (không làm model input) | `close`, `log_return` |
| Chỉ báo kỹ thuật | `sma_10`, `sma_20`, `ema_12`, `ema_26`, `rsi_14`, `macd`, `macd_signal`, `macd_diff`, `bb_high`, `bb_mid`, `bb_low` |
| Feature stationary (tỷ lệ giá) | `close_to_sma_10`, `close_to_sma_20` |
| Macro — giá trị (pivot) | `dgs10`, `dtwexbgs`, `cpiaucsl` |
| Macro — cờ tin cậy (pivot) | `dgs10_is_imputed`, `dgs10_is_anomaly`, `dtwexbgs_is_imputed`, `dtwexbgs_is_anomaly`, `cpiaucsl_is_imputed`, `cpiaucsl_is_anomaly` |
| Lagged (chỉ stationary) | `logret_lag_1`, `rsi_14_lag_1`, … (theo list lag; KHÔNG lag `close`) |
| Target (multi-horizon) | `target_logret_1`, `target_logret_5` (tương lai, theo `horizons`) |
| Cờ / meta | `has_features` BOOLEAN, `has_target_1` / `has_target_5` BOOLEAN, `processed_at` TIMESTAMPTZ |

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
| Tỷ lệ giá (stationary) | `close / sma_10`, `close / sma_20` | `close_to_sma_10`, `close_to_sma_20` |

Tất cả tính trên `close`, **group theo `source`**, rolling lùi về quá khứ.

**Stationarity — vì sao không lag `close`:** giá đóng cửa tuyệt đối là **non-stationary** (có xu hướng,
drift theo thời gian). Đưa `close_lag_k` thẳng làm input thì XGBoost/RF không ngoại suy được khi giá
tương lai vượt khoảng đã học trong tập train, còn thang đo trôi theo thời gian. Vì vậy: **chỉ lag các
đại lượng stationary** (`log_return`, `rsi_14`), và thêm feature **tỷ lệ** `close_to_sma_{w}` (giá so
với trung bình động) thay cho mức giá tuyệt đối. `close`/`log_return` vẫn được giữ làm cột tham chiếu
(để Stage 4 dựng lại / kiểm tra), không khuyến nghị làm model input.

**Lưu ý implementation (`ta` + groupby):** `ta` nhận `Series`/`DataFrame`, **không** nhận object
`groupby`. `technical.py` phải **loop qua từng `source`**, tính chỉ báo trên chuỗi của nhóm đó rồi gộp
lại (hoặc dùng `groupby(...).transform(...)` cẩn thận) — tuyệt đối không trộn dữ liệu giữa các nguồn.

**`has_features` tính ĐỘNG, không hardcode:** mỗi indicator có độ warmup khác nhau, và `ta` dựng
EMA/MACD bằng `pandas.ewm().mean()` (phát giá trị ngay từ dòng 0, **không** NaN) trong khi RSI/SMA mới
tạo NaN warmup; thêm lag còn cộng dồn (vd `rsi_14_lag_5` cần 14+5 dòng). Vì vậy `has_features` của một
dòng = "mọi cột feature đều non-NaN" (đủ lịch sử) — tính theo NaN thực tế per `source`, **không** gán
theo một con số cố định.

**Lag mặc định:** `[1, 2, 3, 5]` ngày, áp cho `log_return`, `rsi_14` (KHÔNG `close` — xem stationarity).

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
- **Target logic (per-horizon):** với mỗi `h` trong `horizons`, cột `target_logret_{h}` khớp đúng
  `groupby("source")["log_return"].shift(-h)` theo vị trí (xác nhận target nhìn đúng `h` ngày tương lai,
  không tràn nguồn).
- **Tính nhất quán cờ/NaN:** `has_features` đúng = các dòng mọi feature non-NaN; `has_target_{h}` đúng =
  các dòng còn target ở horizon đó (đuôi thiếu = False). NaN chỉ được phép ở vùng cờ = False; NaN ngoài
  vùng đó → raise.
- Flag (`has_features`/`has_target_{h}`/macro flags) **không** phải là lỗi — dòng cờ vẫn pass và được ghi.

## Chiến lược test (TDD, tách DB như Stage 2)

- **Unit (no DB):** mỗi module một file —
  - `test_technical`: giá trị khớp `ta`; **perturbation** — đổi giá trị `t+1`, feature tại `t` không
    đổi (causality); tính độc lập per `source`.
  - `test_lagged`: lag chỉ stationary (`log_return`/`rsi_14`, không `close`); past-only; **boundary
    bleed** — bảng 2 nguồn, lag KHÔNG tràn từ nguồn này sang nguồn kia ở điểm giao.
  - `test_macro_features`: pivot giá trị đúng; cờ `is_imputed`/`is_anomaly` đi kèm đúng series;
    point-in-time giữ nguyên.
  - `test_target`: **multi-horizon** — mỗi `h` cho cột `target_logret_{h}` đúng `shift(-h)`; đuôi NaN
    đúng độ dài; **boundary bleed** — không tràn nguồn ở điểm giao.
  - `test_assemble`: ghép đúng cột; `date` đồng kiểu trước merge; `has_features` tính động đúng theo
    NaN; `has_target_{h}` đúng; không drop; tập cột == schema.
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
