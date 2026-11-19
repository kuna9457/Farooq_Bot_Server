import numpy as np
import pandas as pd
import pytest

from app.indicators import chandelier_exit, heikin_ashi, ma_ribbon, volume_profile
from app.strategy.filters import (decide_regime, filter1, filter2, filter2_setup, filter3, month_extreme_flags,
                                  pick_candidates, prev_day_poc, resample)
from app.strategy.params import (Filter1Params, Filter2Params, Filter3Params, RegimeParams, ScanParams,
                                 StrategyParams)


def make_df(closes, freq="D", start="2020-01-01", vol=1000.0):
    c = np.asarray(closes, float)
    o = np.concatenate([[c[0]], c[:-1]])
    h = np.maximum(o, c) + 1
    l = np.minimum(o, c) - 1
    idx = pd.date_range(start, periods=len(c), freq=freq, tz="Asia/Kolkata")
    return pd.DataFrame({"open": o, "high": h, "low": l, "close": c, "volume": vol}, index=idx)


def test_heikin_ashi_basic():
    df = make_df([10, 11, 12, 13])
    ha = heikin_ashi(df)
    assert ha["close"].iloc[0] == pytest.approx((df.iloc[0][["open", "high", "low", "close"]].sum()) / 4)
    assert ha["open"].iloc[1] == pytest.approx((ha["open"].iloc[0] + ha["close"].iloc[0]) / 2)
    assert (ha["high"] >= ha[["open", "close"]].max(axis=1)).all()
    assert (ha["low"] <= ha[["open", "close"]].min(axis=1)).all()


def test_chandelier_flips():
    up = list(np.linspace(100, 200, 60))
    down = list(np.linspace(200, 100, 60))
    df = make_df(down + up + down)
    ce = chandelier_exit(df, 22, 3.0)
    assert ce["buy"].sum() >= 1 and ce["sell"].sum() >= 1
    # in the middle of the up-leg we are long, at the end of the final down-leg we are short
    assert ce["dir"].iloc[110] == 1
    assert ce["dir"].iloc[-1] == -1
    first_buy = int(np.flatnonzero(ce["buy"])[0])
    assert 60 <= first_buy < 100


def test_volume_profile_poc_and_value_area():
    # most volume traded around 105
    idx = pd.date_range("2024-01-01 09:15", periods=5, freq="5min", tz="Asia/Kolkata")
    df = pd.DataFrame({"open": [100, 104, 105, 105, 110], "high": [101, 106, 106, 106, 111],
                       "low": [99, 104, 104, 104, 109], "close": [100, 105, 105, 105, 110],
                       "volume": [10, 1000, 1000, 1000, 10]}, index=idx)
    vp = volume_profile(df, rows=12, value_area_pct=70)
    assert 104 <= vp.poc <= 106
    assert vp.val <= vp.poc <= vp.vah
    assert vp.vah <= 107 and vp.val >= 103
    assert sum(r["volume"] for r in vp.rows) == pytest.approx(3020)


def test_ma_ribbon_bands():
    df = make_df(np.arange(1, 260))
    r = ma_ribbon(df)
    last = r.iloc[-1]
    assert last["upper"] == pytest.approx(last["ma_20"])
    assert last["lower"] == pytest.approx(last["ma_200"])
    assert np.isnan(r["upper"].iloc[10])


def test_regime_rules():
    lit = RegimeParams(rule="literal", threshold_points=50)
    sym = RegimeParams(rule="symmetric", threshold_points=50)
    assert decide_regime(60, lit) == "BUY"
    assert decide_regime(30, lit) == "SELL"
    assert decide_regime(-80, lit) == "SELL"
    assert decide_regime(30, sym) == "NO_TRADE"
    assert decide_regime(-50, sym) == "SELL"
    assert decide_regime(50, sym) == "BUY"


def test_scanner_month_high_and_rank():
    df = make_df(list(range(100, 130)))
    f = month_extreme_flags(df, ScanParams())
    assert f["month_high"] and not f["month_low"]
    rows = [{"symbol": "A", "month_high": True, "month_low": False, "market_cap": 5},
            {"symbol": "B", "month_high": True, "month_low": False, "market_cap": 9},
            {"symbol": "C", "month_high": False, "month_low": True, "market_cap": 1},
            {"symbol": "D", "month_high": True, "month_low": False, "market_cap": 7},
            {"symbol": "E", "month_high": True, "month_low": False, "market_cap": 1}]
    b, s = pick_candidates(rows, 3)
    assert [x["symbol"] for x in b] == ["B", "D", "A"]
    assert [x["symbol"] for x in s] == ["C"]


def test_filters_on_strong_uptrend():
    # 6 years of a down-then-strong-up daily trend -> BUY side should pass F1 and have an F2 setup
    closes = list(np.linspace(300, 150, 600)) + list(np.linspace(150, 900, 900))
    daily = make_df(closes, freq="B", start="2019-01-01", vol=1e6)
    p = StrategyParams()
    f1 = filter1(daily, "BUY", p.filter1)
    assert f1.status == "PASS", f1.reason
    setup = filter2_setup(daily, "BUY", p.filter2)
    assert setup is not None
    assert filter2(setup, "BUY", setup["trigger_level"] + 1, p.filter2).status == "PASS"
    assert filter2(setup, "BUY", setup["trigger_level"] - 1, p.filter2).status == "FAIL"
    # SELL side must fail on an uptrend
    assert filter1(daily, "SELL", p.filter1).status == "FAIL"


def test_filter3_poc():
    prev = make_df(list(np.linspace(100, 101, 75)), freq="5min", start="2024-01-01 09:15")
    today = make_df(list(np.linspace(101, 110, 75)), freq="5min", start="2024-01-02 09:15")
    bars = pd.concat([prev, today])
    p = Filter3Params()
    poc = prev_day_poc(prev, p, ha_context=prev)
    assert 99 <= poc["poc"] <= 102
    r = filter3(bars, poc, "BUY", p)
    assert r.status == "PASS", r.reason
    assert filter3(bars, poc, "SELL", p).status == "FAIL"


def test_resample_weekly():
    daily = make_df(list(range(1, 31)), freq="B", start="2024-01-01")
    w = resample(daily, "1W")
    assert len(w) == 6
    assert w["volume"].iloc[0] == 5000
