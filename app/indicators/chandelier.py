"""Chandelier Exit — port of the popular TradingView script by everget (default 22 / 3.0 / useClose=true).

Pine reference:
    atr = mult * ta.atr(length)
    longStop  = (useClose ? ta.highest(close, length) : ta.highest(length)) - atr
    longStopPrev = nz(longStop[1], longStop)
    longStop := close[1] > longStopPrev ? math.max(longStop, longStopPrev) : longStop
    shortStop = (useClose ? ta.lowest(close, length) : ta.lowest(length)) + atr
    shortStopPrev = nz(shortStop[1], shortStop)
    shortStop := close[1] < shortStopPrev ? math.min(shortStop, shortStopPrev) : shortStop
    dir := close > shortStopPrev ? 1 : close < longStopPrev ? -1 : dir
    buySignal  = dir == 1 and dir[1] == -1
    sellSignal = dir == -1 and dir[1] == 1
"""
import numpy as np
import pandas as pd


def _rma(x: np.ndarray, length: int) -> np.ndarray:
    """Wilder's RMA as used by ta.atr (seeded with SMA of first `length` values)."""
    out = np.full(len(x), np.nan)
    if len(x) < length:
        return out
    out[length - 1] = np.nanmean(x[:length])
    alpha = 1.0 / length
    for i in range(length, len(x)):
        out[i] = alpha * x[i] + (1 - alpha) * out[i - 1]
    return out


def atr(df: pd.DataFrame, length: int) -> np.ndarray:
    h = df["high"].to_numpy(float)
    l = df["low"].to_numpy(float)
    c = df["close"].to_numpy(float)
    prev_c = np.concatenate([[np.nan], c[:-1]])
    tr = np.where(np.isnan(prev_c), h - l,
                  np.maximum.reduce([h - l, np.abs(h - prev_c), np.abs(l - prev_c)]))
    return _rma(tr, length)


def chandelier_exit(df: pd.DataFrame, length: int = 22, mult: float = 3.0, use_close: bool = True) -> pd.DataFrame:
    """Returns DataFrame with columns: long_stop, short_stop, dir (1/-1), buy (bool), sell (bool)."""
    n = len(df)
    c = df["close"].to_numpy(float)
    src_hi = pd.Series(c if use_close else df["high"].to_numpy(float))
    src_lo = pd.Series(c if use_close else df["low"].to_numpy(float))
    a = mult * atr(df, length)
    hh = src_hi.rolling(length, min_periods=length).max().to_numpy()
    ll = src_lo.rolling(length, min_periods=length).min().to_numpy()
    long_raw = hh - a
    short_raw = ll + a

    long_stop = np.full(n, np.nan)
    short_stop = np.full(n, np.nan)
    direction = np.ones(n, dtype=int)
    for i in range(n):
        ls, ss = long_raw[i], short_raw[i]
        if np.isnan(ls) or np.isnan(ss):
            direction[i] = direction[i - 1] if i else 1
            continue
        ls_prev = long_stop[i - 1] if i and not np.isnan(long_stop[i - 1]) else ls
        ss_prev = short_stop[i - 1] if i and not np.isnan(short_stop[i - 1]) else ss
        if i and c[i - 1] > ls_prev:
            ls = max(ls, ls_prev)
        if i and c[i - 1] < ss_prev:
            ss = min(ss, ss_prev)
        long_stop[i], short_stop[i] = ls, ss
        prev_dir = direction[i - 1] if i else 1
        if c[i] > ss_prev:
            direction[i] = 1
        elif c[i] < ls_prev:
            direction[i] = -1
        else:
            direction[i] = prev_dir
    prev = np.concatenate([[direction[0]], direction[:-1]])
    valid = ~np.isnan(long_stop)
    buy = (direction == 1) & (prev == -1) & valid
    sell = (direction == -1) & (prev == 1) & valid
    return pd.DataFrame({"long_stop": long_stop, "short_stop": short_stop, "dir": direction,
                         "buy": buy, "sell": sell, "valid": valid}, index=df.index)
