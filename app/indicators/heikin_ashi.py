import numpy as np
import pandas as pd


def heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    """Return a new DataFrame with Heikin-Ashi OHLC (volume copied). Matches TradingView's HA candles."""
    if df.empty:
        return df.copy()
    o = df["open"].to_numpy(float)
    h = df["high"].to_numpy(float)
    l = df["low"].to_numpy(float)
    c = df["close"].to_numpy(float)
    n = len(df)
    ha_c = (o + h + l + c) / 4.0
    ha_o = np.empty(n)
    ha_o[0] = (o[0] + c[0]) / 2.0
    for i in range(1, n):
        ha_o[i] = (ha_o[i - 1] + ha_c[i - 1]) / 2.0
    ha_h = np.maximum.reduce([h, ha_o, ha_c])
    ha_l = np.minimum.reduce([l, ha_o, ha_c])
    out = pd.DataFrame(
        {"open": ha_o, "high": ha_h, "low": ha_l, "close": ha_c,
         "volume": df["volume"].to_numpy(float) if "volume" in df else 0.0},
        index=df.index,
    )
    return out
