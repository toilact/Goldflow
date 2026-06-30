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
    last_row = df.tail(1)  # same semantics as iloc[[59]]; avoids Bus error on this platform
    df = pd.concat([df, last_row], ignore_index=True)
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


def test_corrupted_has_features_flag_raises():
    df = _good()
    # Flag claims ready but row 0 is warmup (features are NaN) -> contradiction.
    df.loc[0, "has_features"] = True
    with pytest.raises(FeatureQualityError, match="has_features"):
        check_features(df)


def test_corrupted_has_target_flag_raises():
    df = _good()
    # Tail row has no future return (target is NaN) but the flag claims it does.
    df.loc[df.index[-1], "has_target_1"] = True
    with pytest.raises(FeatureQualityError, match="has_target_1"):
        check_features(df)


def test_flag_false_but_features_present_raises():
    df = _good()
    # Reverse direction: a fully-populated row flagged not-ready would silently
    # drop a usable training row. The old NaN-only check missed this.
    df.loc[59, "has_features"] = False
    with pytest.raises(FeatureQualityError, match="has_features"):
        check_features(df)
