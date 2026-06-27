import pandas as pd
from gold_pipeline.preprocessing.calendar import trading_days


def test_trading_days_dedups_and_sorts():
    gold = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-03", "2020-01-02", "2020-01-03"]),
        "source": ["GC=F", "GC=F", "XAU/USD"],
    })
    days = trading_days(gold)
    assert list(days) == [pd.Timestamp("2020-01-02"), pd.Timestamp("2020-01-03")]
    assert list(days.index) == [0, 1]
