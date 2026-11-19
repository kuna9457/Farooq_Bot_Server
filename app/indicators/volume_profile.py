"""Volume profile (used for both Anchored VP and Fixed Range VP).

Algorithm (TradingView-like):
  1. Price range [min low, max high] of the window split into `rows` equal rows.
  2. Each bar's volume is spread over the rows its [low, high] overlaps, proportional to overlap.
  3. POC = row with max volume (mid price).
  4. Value area: start at POC, repeatedly add the neighbouring row (above/below) with more volume
     until >= value_area_pct of total volume. VAH = top of highest row in VA, VAL = bottom of lowest.
"""
from dataclasses import dataclass, asdict
import numpy as np
import pandas as pd


@dataclass
class VolumeProfile:
    poc: float
    vah: float
    val: float
    low: float
    high: float
    total_volume: float
    rows: list  # [{"price_low", "price_high", "volume"}]

    def to_dict(self, include_rows: bool = False) -> dict:
        d = asdict(self)
        if not include_rows:
            d.pop("rows")
        return d


def volume_profile(df: pd.DataFrame, rows: int = 24, value_area_pct: float = 70.0) -> VolumeProfile | None:
    if df is None or df.empty:
        return None
    lo = float(df["low"].min())
    hi = float(df["high"].max())
    if not np.isfinite(lo) or not np.isfinite(hi):
        return None
    if hi <= lo:
        v = float(df["volume"].sum())
        return VolumeProfile(lo, hi, lo, lo, hi, v, [{"price_low": lo, "price_high": hi, "volume": v}])

    edges = np.linspace(lo, hi, rows + 1)
    vols = np.zeros(rows)
    bl = df["low"].to_numpy(float)
    bh = df["high"].to_numpy(float)
    bv = df["volume"].to_numpy(float)
    for l, h, v in zip(bl, bh, bv):
        if not v or np.isnan(v):
            continue
        if h <= l:  # doji/flat bar: all volume into its row
            idx = min(int((l - lo) / (hi - lo) * rows), rows - 1)
            vols[idx] += v
            continue
        overlap = np.clip(np.minimum(edges[1:], h) - np.maximum(edges[:-1], l), 0, None)
        tot = overlap.sum()
        if tot > 0:
            vols += v * overlap / tot

    total = vols.sum()
    poc_i = int(np.argmax(vols))
    poc = float((edges[poc_i] + edges[poc_i + 1]) / 2)

    target = total * value_area_pct / 100.0
    up, dn = poc_i, poc_i
    acc = vols[poc_i]
    while acc < target and (up < rows - 1 or dn > 0):
        v_up = vols[up + 1] if up < rows - 1 else -1
        v_dn = vols[dn - 1] if dn > 0 else -1
        if v_up >= v_dn:
            up += 1
            acc += vols[up]
        else:
            dn -= 1
            acc += vols[dn]
    vah = float(edges[up + 1])
    val = float(edges[dn])
    row_list = [{"price_low": float(edges[i]), "price_high": float(edges[i + 1]), "volume": float(vols[i])}
                for i in range(rows)]
    return VolumeProfile(poc=poc, vah=vah, val=val, low=lo, high=hi, total_volume=float(total), rows=row_list)
