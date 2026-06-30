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
