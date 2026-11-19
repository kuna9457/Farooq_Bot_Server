"""One place that turns data-known-at-time-T into F1/F2/F3 results. Shared by live engine and backtester."""
from __future__ import annotations

from datetime import date

import pandas as pd

from .filters import filter1, filter2, filter2_setup, filter3, prev_day_poc, with_partial_bar
from .params import StrategyParams


def split_days(bars_5m: pd.DataFrame) -> dict[date, pd.DataFrame]:
    return {d: g for d, g in bars_5m.groupby(bars_5m.index.date)}


def prepare_day(daily_completed: pd.DataFrame, bars_5m_before_today: pd.DataFrame, side: str,
                p: StrategyParams) -> dict:
    """Things that are fixed for the whole day: F2 setup candle and F3 previous-day POC."""
    setup = filter2_setup(daily_completed, side, p.filter2)
    poc = None
    if bars_5m_before_today is not None and not bars_5m_before_today.empty:
        last_day = bars_5m_before_today.index[-1].date()
        prev = bars_5m_before_today[bars_5m_before_today.index.date == last_day]
        poc = prev_day_poc(prev, p.filter3, ha_context=bars_5m_before_today)
    return {"setup": setup, "poc": poc}


def evaluate(daily_completed: pd.DataFrame, bars_5m: pd.DataFrame, today: date, side: str,
             p: StrategyParams, prep: dict) -> dict:
    """bars_5m: COMPLETED 5m bars for several days up to now (incl. today's so far)."""
    today_bars = bars_5m[bars_5m.index.date == today]
    daily_live = with_partial_bar(daily_completed, today_bars)
    f1 = filter1(daily_live, side, p.filter1)
    price = float(today_bars["close"].iloc[-1]) if not today_bars.empty else None
    f2 = filter2(prep.get("setup"), side, price, p.filter2)
    f3 = filter3(bars_5m.iloc[-300:], prep.get("poc"), side, p.filter3) if not today_bars.empty else \
        filter3(None, prep.get("poc"), side, p.filter3)
    all_pass = f1.passed and f2.passed and f3.passed
    return {"F1": f1.to_dict(), "F2": f2.to_dict(), "F3": f3.to_dict(), "all_passed": all_pass, "price": price,
            "candle_time": today_bars.index[-1].isoformat() if not today_bars.empty else None}
