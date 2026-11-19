"""Market data service: picks the provider, caches candles in Mongo (`candle_cache`) and in memory."""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from ..config import get_settings
from ..db import db
from .providers import COLS, IST, DataProvider, UpstoxProvider, YFinanceProvider, empty_df
from .universe import NIFTY_INDEX, get_universe

log = logging.getLogger("market_data")


def now_ist() -> pd.Timestamp:
    return pd.Timestamp.now(tz=IST)


def _to_records(df: pd.DataFrame) -> list:
    return [[int(ts.value // 10**6), *map(float, row)] for ts, row in zip(df.index, df[COLS].to_numpy())]


def _from_records(recs: list) -> pd.DataFrame:
    if not recs:
        return empty_df()
    df = pd.DataFrame(recs, columns=["ts"] + COLS)
    df.index = pd.DatetimeIndex(pd.to_datetime(df.pop("ts"), unit="ms", utc=True)).tz_convert(IST)
    return df.astype(float)


class MarketData:
    def __init__(self):
        self._provider: DataProvider | None = None
        self._mem: dict[tuple, tuple[float, pd.DataFrame]] = {}
        self._locks: dict[tuple, asyncio.Lock] = {}
        self._isin: dict[str, str] = {}

    # ---------------------------------------------------------------- provider
    @property
    def provider(self) -> DataProvider:
        if self._provider is None:
            self.set_provider(get_settings().data_provider)
        return self._provider

    def set_provider(self, name: str) -> None:
        if name == "upstox":
            from ..execution.tokens import get_token
            self._provider = UpstoxProvider(lambda: get_token("upstox"), self.upstox_key)
        else:
            self._provider = YFinanceProvider()
        self._mem.clear()
        log.info("Data provider: %s", self._provider.name)

    async def upstox_key(self, symbol: str) -> str:
        if symbol == NIFTY_INDEX:
            return "NSE_INDEX|Nifty 50"
        if not self._isin:
            self._isin = {u["symbol"]: u["isin"] for u in await get_universe()}
        if symbol not in self._isin:
            raise KeyError(f"No ISIN for {symbol}")
        return f"NSE_EQ|{self._isin[symbol]}"

    def _lock(self, key) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    # ---------------------------------------------------------------- daily
    async def daily(self, symbol: str, years: int = 6, force: bool = False) -> pd.DataFrame:
        """Daily candles (cached). May include today's partial bar from the provider — callers that need only
        completed sessions should use `completed_daily`."""
        key = (symbol, "1D")
        async with self._lock(key):
            m = self._mem.get(key)
            if m and not force and time.time() - m[0] < 600:
                return m[1]
            doc = await db().candle_cache.find_one({"_id": f"{symbol}|1D|{self.provider.name}"})
            df = _from_records(doc["data"]) if doc else empty_df()
            updated = doc["updated_at"] if doc else None
            if updated is not None and updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            stale = (df.empty or updated is None
                     or datetime.now(timezone.utc) - updated > timedelta(hours=4) or force)
            if stale:
                start = (df.index[-1].date() - timedelta(days=7)) if not df.empty \
                    else date.today() - timedelta(days=365 * years + 30)
                try:
                    new = await self.provider.daily(symbol, start, date.today())
                    df = pd.concat([df[df.index < new.index[0]] if not new.empty else df, new])
                    df = df[~df.index.duplicated(keep="last")].sort_index()
                    await db().candle_cache.replace_one(
                        {"_id": f"{symbol}|1D|{self.provider.name}"},
                        {"symbol": symbol, "tf": "1D", "data": _to_records(df),
                         "updated_at": datetime.now(timezone.utc)}, upsert=True)
                except Exception as e:  # noqa: BLE001
                    log.warning("daily fetch failed for %s: %s", symbol, e)
                    if df.empty:
                        raise
            self._mem[key] = (time.time(), df)
            return df

    async def completed_daily(self, symbol: str, before: date | None = None, **kw) -> pd.DataFrame:
        df = await self.daily(symbol, **kw)
        cut = pd.Timestamp(before or now_ist().date(), tz=IST)
        return df[df.index < cut]

    # ---------------------------------------------------------------- 5m
    async def intraday_5m(self, symbol: str, days: int = 10, ttl: float = 20) -> pd.DataFrame:
        """Recent 5m bars (incl. today's, possibly with the still-forming last bar). Memory cached for `ttl` s."""
        key = (symbol, "5m", days)
        async with self._lock(key):
            m = self._mem.get(key)
            if m and time.time() - m[0] < ttl:
                return m[1]
            df = await self.provider.intraday_5m(symbol, date.today() - timedelta(days=days), date.today())
            self._mem[key] = (time.time(), df)
            return df

    async def history_5m(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        """5m history for backtests, cached in Mongo and extended incrementally."""
        cid = f"{symbol}|5m|{self.provider.name}"
        doc = await db().candle_cache.find_one({"_id": cid})
        df = _from_records(doc["data"]) if doc else empty_df()
        need_fetch = df.empty or df.index[0].date() > start + timedelta(days=4) or df.index[-1].date() < end - timedelta(days=1)
        if need_fetch:
            try:
                new = await self.provider.intraday_5m(symbol, start, end)
                df = pd.concat([df, new])
                df = df[~df.index.duplicated(keep="last")].sort_index()
                # never cache a still-forming bar
                df = df[df.index + pd.Timedelta(minutes=5) <= now_ist()]
                await db().candle_cache.replace_one({"_id": cid}, {"symbol": symbol, "tf": "5m", "data": _to_records(df),
                                                                   "updated_at": datetime.now(timezone.utc)}, upsert=True)
            except Exception as e:  # noqa: BLE001
                log.warning("5m history fetch failed for %s: %s", symbol, e)
        lo, hi = pd.Timestamp(start, tz=IST), pd.Timestamp(end + timedelta(days=1), tz=IST)
        return df[(df.index >= lo) & (df.index < hi)]

    async def ltp(self, symbols: list[str]) -> dict[str, float]:
        try:
            return await self.provider.ltp(symbols)
        except Exception as e:  # noqa: BLE001
            log.warning("LTP fetch failed: %s", e)
            return {}


def completed_5m(df: pd.DataFrame, at: pd.Timestamp | None = None) -> pd.DataFrame:
    """Drop the still-forming candle (bar start + 5m > now)."""
    at = at or now_ist()
    return df[df.index + pd.Timedelta(minutes=5) <= at]


market_data = MarketData()
