"""Moving Average Ribbon (TradingView "MA ribbon": up to N lines, each with its own type / source / length;
default SMA 20/50/100/200 on close)."""
import pandas as pd

_DERIVED = {"hl2": ("high", "low"), "hlc3": ("high", "low", "close"), "ohlc4": ("open", "high", "low", "close"),
            "hlcc4": ("high", "low", "close", "close")}


def price_source(df: pd.DataFrame, source: str) -> pd.Series:
    src = (source or "close").lower()
    if src in _DERIVED:
        return df[list(_DERIVED[src])].astype(float).mean(axis=1)
    return df[src].astype(float)


def _ma(s: pd.Series, kind: str, length: int, volume: pd.Series | None = None) -> pd.Series:
    kind = kind.upper()
    if kind == "EMA":
        return s.ewm(span=length, adjust=False).mean()
    if kind == "WMA":
        w = pd.Series(range(1, length + 1), dtype=float)
        return s.rolling(length).apply(lambda x: (x * w.values).sum() / w.sum(), raw=True)
    if kind in ("RMA", "SMMA"):
        return s.ewm(alpha=1 / length, adjust=False).mean()
    if kind == "VWMA":
        v = volume if volume is not None else pd.Series(1.0, index=s.index)
        return (s * v).rolling(length, min_periods=length).sum() / v.rolling(length, min_periods=length).sum()
    return s.rolling(length, min_periods=length).mean()  # SMA


def _get(spec, key, default):
    return spec.get(key, default) if isinstance(spec, dict) else getattr(spec, key, default)


def ma_ribbon(df: pd.DataFrame, lengths=(20, 50, 100, 200), kind: str = "SMA", source: str = "close",
              specs=None) -> pd.DataFrame:
    """Returns columns ma_<len> (one per enabled MA) + upper (max of MAs) + lower (min of MAs).
    Rows where any MA is NaN have NaN bands. `specs` = list of {enabled, kind, source, length} overrides the simple args."""
    if specs is None:
        specs = [{"enabled": True, "kind": kind, "source": source, "length": int(L)} for L in lengths]
    vol = df["volume"].astype(float) if "volume" in df.columns else None
    out = pd.DataFrame(index=df.index)
    names: list[str] = []
    for i, sp in enumerate(specs):
        if not _get(sp, "enabled", True):
            continue
        L = int(_get(sp, "length", 20))
        name = f"ma_{L}" if f"ma_{L}" not in out.columns else f"ma_{L}_{i + 1}"
        out[name] = _ma(price_source(df, _get(sp, "source", "close")), _get(sp, "kind", "SMA"), L, vol)
        names.append(name)
    if names:
        mas = out[names]
        out["upper"] = mas.max(axis=1, skipna=False)
        out["lower"] = mas.min(axis=1, skipna=False)
    else:
        out["upper"] = float("nan")
        out["lower"] = float("nan")
    return out
