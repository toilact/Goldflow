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

    # 5. Readiness flags match their contract, both directions. We recompute the
    #    flags independently from the data columns and compare to the stored flags:
    #    a True flag over NaN data would leak NaN into the model; a False flag over
    #    complete data would silently drop a usable row. The one-directional NaN
    #    check this replaces only caught the former.
    wcols = warmup_columns(cfg)
    expected_feat = df[wcols].notna().all(axis=1).to_numpy()
    if not (df["has_features"].astype(bool).to_numpy() == expected_feat).all():
        raise FeatureQualityError("has_features disagrees with warmup-column NaN pattern")
    for h in cfg.horizons:
        expected_t = df[f"target_logret_{h}"].notna().to_numpy()
        if not (df[f"has_target_{h}"].astype(bool).to_numpy() == expected_t).all():
            raise FeatureQualityError(
                f"has_target_{h} disagrees with target_logret_{h} NaN pattern"
            )
