"""Chart payloads for the UI (TradingView Lightweight Charts). Times are IST wall-clock encoded as UTC seconds."""
import numpy as np
import pandas as pd

from ..data.market_data import completed_5m, market_data, now_ist
from ..indicators import chandelier_exit, heikin_ashi, ma_ribbon, volume_profile
from ..strategy.evaluator import prepare_day
from ..strategy.filters import resample, with_partial_bar
from ..strategy.params import StrategyParams


def _ts(idx: pd.DatetimeIndex) -> list[int]:
    local = idx.tz_convert("Asia/Kolkata").tz_localize(None)
    return [int(t.value // 10**9) for t in local]


def _num(x):
    return None if x is None or (isinstance(x, float) and np.isnan(x)) else round(float(x), 2)


def _series(times, values):
    return [{"time": t, "value": _num(v)} for t, v in zip(times, values) if _num(v) is not None]


def _candles(df, times):
    return [{"time": t, "open": round(o, 2), "high": round(h, 2), "low": round(l, 2), "close": round(c, 2)}
            for t, o, h, l, c in zip(times, df["open"], df["high"], df["low"], df["close"])]


def _ce_payload(chart, times, ce):
    long_s = [v if d == 1 else np.nan for v, d in zip(ce["long_stop"], ce["dir"])]
    short_s = [v if d == -1 else np.nan for v, d in zip(ce["short_stop"], ce["dir"])]
    markers = []
    for i, t in enumerate(times):
        if ce["buy"].iloc[i]:
            markers.append({"time": t, "position": "belowBar", "color": "#16a34a", "shape": "arrowUp", "text": "CE Buy"})
        elif ce["sell"].iloc[i]:
            markers.append({"time": t, "position": "aboveBar", "color": "#dc2626", "shape": "arrowDown", "text": "CE Sell"})
    gap = lambda vals: [{"time": t, "value": _num(v)} for t, v in zip(times, vals)]  # noqa: E731  (None = gap)
    return {"ce_long": gap(long_s), "ce_short": gap(short_s), "markers": markers}


async def build_chart(symbol: str, tf: str, side: str, p: StrategyParams) -> dict:
    out: dict = {"symbol": symbol, "tf": tf, "side": side, "levels": []}
    if tf == "5m":
        bars = completed_5m(await market_data.intraday_5m(symbol, days=10))
        today = now_ist().date()
        days = sorted(set(bars.index.date))
        before = bars[bars.index.date < (today if today in days else days[-1] if days else today)]
        prep = prepare_day(await market_data.completed_daily(symbol), before, side, p)
        chart = heikin_ashi(bars) if p.filter3.use_heikin_ashi else bars
        ce = chandelier_exit(chart, p.filter3.ce.length, p.filter3.ce.mult, p.filter3.ce.use_close)
        show = slice(-75 * 3, None)  # last ~3 sessions
        chart, ce, real = chart.iloc[show], ce.iloc[show], bars.iloc[show]
        times = _ts(chart.index)
        out.update(_ce_payload(chart, times, ce))
        if prep.get("poc"):
            pc = prep["poc"]
            out["levels"] += [{"price": pc["poc"], "title": f"POC ({pc['date']})", "color": "#f59e0b"},
                              {"price": pc["vah"], "title": "VAH", "color": "#3b82f6"},
                              {"price": pc["val"], "title": "VAL", "color": "#3b82f6"}]
    elif tf == "1D":
        daily = await market_data.daily(symbol)
        daily = daily.iloc[-400:]
        chart = heikin_ashi(daily) if p.filter2.use_heikin_ashi else daily
        ce = chandelier_exit(chart, p.filter2.ce.length, p.filter2.ce.mult, p.filter2.ce.use_close)
        rib = ma_ribbon(chart, p.filter2.ribbon.lengths, p.filter2.ribbon.kind, p.filter2.ribbon.source)
        show = slice(-250, None)
        chart, ce, rib, real = chart.iloc[show], ce.iloc[show], rib.iloc[show], daily.iloc[show]
        times = _ts(chart.index)
        out.update(_ce_payload(chart, times, ce))
        out["ribbon"] = [{"name": c, "data": _series(times, rib[c])} for c in rib.columns if c.startswith("ma_")]
        prep = prepare_day(await market_data.completed_daily(symbol), None, side, p)
        if prep.get("setup"):
            su = prep["setup"]
            out["setup"] = su
            out["levels"].append({"price": su["trigger_level"], "title": f"Setup {su['setup_date']} trigger",
                                  "color": "#a855f7"})
    else:  # F1 timeframe
        f1 = p.filter1
        daily = await market_data.completed_daily(symbol)
        live = with_partial_bar(daily, completed_5m(await market_data.intraday_5m(symbol, days=2)).pipe(
            lambda b: b[b.index.date == now_ist().date()]))
        start = live.index[-1] - pd.DateOffset(years=f1.years)
        bars = resample(live[live.index >= start], f1.timeframe)
        chart = heikin_ashi(bars) if f1.use_heikin_ashi else bars
        ce = chandelier_exit(chart, f1.ce.length, f1.ce.mult, f1.ce.use_close)
        times = _ts(chart.index)
        real = bars
        out.update(_ce_payload(chart, times, ce))
        col = "buy" if side == "BUY" else "sell"
        idx = np.flatnonzero(ce[col].to_numpy())
        if len(idx):
            a = int(idx[-1])
            vp = volume_profile(chart.iloc[a:], f1.vp.rows, f1.vp.value_area_pct)
            out["anchor_time"] = times[a]
            out["profile"] = vp.to_dict(include_rows=True)
            out["levels"] += [{"price": round(vp.vah, 2), "title": "VAH (anchored)", "color": "#3b82f6"},
                              {"price": round(vp.val, 2), "title": "VAL (anchored)", "color": "#3b82f6"},
                              {"price": round(vp.poc, 2), "title": "POC (anchored)", "color": "#f59e0b"}]
    out["candles"] = _candles(chart, times)
    out["real_candles"] = _candles(real, times)
    out["volume"] = [{"time": t, "value": float(v)} for t, v in zip(times, real["volume"])]
    return out
