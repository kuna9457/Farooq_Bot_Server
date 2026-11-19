"""Market data providers. All return DataFrames indexed by tz-aware Asia/Kolkata timestamps with columns
open, high, low, close, volume. Intraday bars are labelled by their START time (TradingView convention)."""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from urllib.parse import quote

import httpx
import pandas as pd

log = logging.getLogger("providers")
IST = "Asia/Kolkata"
COLS = ["open", "high", "low", "close", "volume"]


def empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=COLS, index=pd.DatetimeIndex([], tz=IST), dtype=float)


def yf_symbol(sym: str) -> str:
    return "^NSEI" if sym == "NIFTY50" else f"{sym}.NS"


class DataProvider(ABC):
    name = "base"

    @abstractmethod
    async def daily(self, symbol: str, start: date, end: date) -> pd.DataFrame: ...

    @abstractmethod
    async def intraday_5m(self, symbol: str, start: date, end: date) -> pd.DataFrame: ...

    @abstractmethod
    async def ltp(self, symbols: list[str]) -> dict[str, float]: ...


# ============================================================================ yfinance (free, delayed)
class YFinanceProvider(DataProvider):
    """Free data via Yahoo Finance. 5m history limited to ~60 days. Good for paper trading / backtests."""
    name = "yfinance"

    @staticmethod
    def _norm(df: pd.DataFrame, daily: bool) -> pd.DataFrame:
        if df is None or df.empty:
            return empty_df()
        df = df.rename(columns=str.lower)[COLS].astype(float)
        idx = df.index
        idx = idx.tz_localize(IST) if idx.tz is None else idx.tz_convert(IST)
        if daily:
            idx = idx.normalize()
        df.index = idx
        df = df[~df.index.duplicated(keep="last")].sort_index()
        return df.dropna(subset=["open", "close"])

    @staticmethod
    def _hist(symbol, **kw) -> pd.DataFrame:
        import yfinance as yf
        return yf.Ticker(yf_symbol(symbol)).history(auto_adjust=False, actions=False, **kw)

    async def daily(self, symbol, start, end):
        df = await asyncio.to_thread(self._hist, symbol, start=start.isoformat(),
                                     end=(end + timedelta(days=1)).isoformat(), interval="1d")
        return self._norm(df, True)

    async def intraday_5m(self, symbol, start, end):
        start = max(start, date.today() - timedelta(days=58))
        df = await asyncio.to_thread(self._hist, symbol, start=start.isoformat(),
                                     end=(end + timedelta(days=1)).isoformat(), interval="5m")
        df = self._norm(df, False)
        t = df.index.time  # regular session only
        return df[(t >= datetime.strptime("09:15", "%H:%M").time()) & (t < datetime.strptime("15:30", "%H:%M").time())]

    async def ltp(self, symbols):
        import yfinance as yf

        def _one(s):
            try:
                return s, float(yf.Ticker(yf_symbol(s)).fast_info["last_price"])
            except Exception:  # noqa: BLE001
                return s, None
        res = await asyncio.gather(*[asyncio.to_thread(_one, s) for s in symbols])
        return {s: p for s, p in res if p is not None}


# ============================================================================ Upstox (official, live)
class UpstoxProvider(DataProvider):
    """Upstox API v3 historical + intraday candles and LTP. Requires a daily access token."""
    name = "upstox"
    BASE = "https://api.upstox.com"

    def __init__(self, token_getter, key_getter):
        self._token = token_getter          # async () -> str | None
        self._key = key_getter              # async (symbol) -> instrument_key

    async def _get(self, path: str, params: dict | None = None) -> dict:
        token = await self._token()
        if not token:
            raise RuntimeError("Upstox not connected (no access token). Login from the Broker page.")
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(self.BASE + path, params=params,
                            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
            if r.status_code >= 400:
                raise RuntimeError(f"Upstox {r.status_code}: {r.text[:300]}")
            return r.json()

    @staticmethod
    def _candles(js: dict, daily: bool) -> pd.DataFrame:
        rows = js.get("data", {}).get("candles", [])
        if not rows:
            return empty_df()
        df = pd.DataFrame([r[:6] for r in rows], columns=["ts"] + COLS)
        idx = pd.DatetimeIndex(pd.to_datetime(df.pop("ts"))).tz_convert(IST)
        df.index = idx.normalize() if daily else idx
        return df.astype(float).sort_index()

    async def daily(self, symbol, start, end):
        key = quote(await self._key(symbol), safe="")
        frames, s = [], start
        while s <= end:  # chunked requests
            e = min(end, s + timedelta(days=3600))
            js = await self._get(f"/v3/historical-candle/{key}/days/1/{e.isoformat()}/{s.isoformat()}")
            frames.append(self._candles(js, True))
            s = e + timedelta(days=1)
        df = pd.concat(frames) if frames else empty_df()
        return df[~df.index.duplicated()].sort_index()

    async def intraday_5m(self, symbol, start, end):
        key = quote(await self._key(symbol), safe="")
        frames, s = [], start
        today = date.today()
        hist_end = min(end, today - timedelta(days=1))
        while s <= hist_end:  # 5-min history: max 1 month per request
            e = min(hist_end, s + timedelta(days=28))
            js = await self._get(f"/v3/historical-candle/{key}/minutes/5/{e.isoformat()}/{s.isoformat()}")
            frames.append(self._candles(js, False))
            s = e + timedelta(days=1)
        if end >= today:
            js = await self._get(f"/v3/historical-candle/intraday/{key}/minutes/5")
            frames.append(self._candles(js, False))
        df = pd.concat(frames) if frames else empty_df()
        return df[~df.index.duplicated(keep="last")].sort_index()

    async def ltp(self, symbols):
        keys = {await self._key(s): s for s in symbols}
        js = await self._get("/v3/market-quote/ltp", {"instrument_key": ",".join(keys)})
        out = {}
        for _, v in (js.get("data") or {}).items():
            tok = v.get("instrument_token")
            if tok in keys:
                out[keys[tok]] = float(v.get("last_price"))
        return out
