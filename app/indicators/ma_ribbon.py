"""Moving Average Ribbon (TradingView built-in default: SMA 20/50/100/200)."""
import pandas as pd


def _ma(s: pd.Series, kind: str, length: int) -> pd.Series:
    kind = kind.upper()
    if kind == "EMA":
        return s.ewm(span=length, adjust=False).mean()
    if kind == "WMA":
        w = pd.Series(range(1, length + 1), dtype=float)
        return s.rolling(length).apply(lambda x: (x * w.values).sum() / w.sum(), raw=True)
    if kind in ("RMA", "SMMA"):
        return s.ewm(alpha=1 / length, adjust=False).mean()
    return s.rolling(length, min_periods=length).mean()  # SMA


def ma_ribbon(df: pd.DataFrame, lengths=(20, 50, 100, 200), kind: str = "SMA", source: str = "close") -> pd.DataFrame:
    """Returns columns ma_<len> + upper (max of MAs) + lower (min of MAs). Rows where any MA is NaN have NaN bands."""
    src = (source or "close").lower()
    derived = {"hl2": ("high", "low"), "hlc3": ("high", "low", "close"), "ohlc4": ("open", "high", "low", "close")}
    s = df[list(derived[src])].astype(float).mean(axis=1) if src in derived else df[src].astype(float)
    out = pd.DataFrame(index=df.index)
    for L in lengths:
        out[f"ma_{L}"] = _ma(s, kind, int(L))
    mas = out[[f"ma_{L}" for L in lengths]]
    out["upper"] = mas.max(axis=1, skipna=False)
    out["lower"] = mas.min(axis=1, skipna=False)
    return out
