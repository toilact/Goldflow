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
