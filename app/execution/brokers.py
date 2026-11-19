"""Broker adapters: Paper, Upstox, Zerodha Kite — one interface."""
from __future__ import annotations

import asyncio
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import httpx

from ..config import get_settings
from ..db import db
from .tokens import get_token

log = logging.getLogger("brokers")


class BrokerError(Exception):
    pass


class BrokerAdapter(ABC):
    name = "base"

    @abstractmethod
    async def place_order(self, symbol: str, side: str, qty: int, *, ltp: float | None = None, tag: str = "") -> dict:
        """Market order, intraday product. Returns {order_id, status, avg_price}."""

    @abstractmethod
    async def funds(self) -> dict:
        """{available, used, total, raw}"""

    async def positions(self) -> list[dict]:
        return []

    async def orders(self) -> list[dict]:
        return []

    async def profile(self) -> dict:
        return {}


# ================================================================== PAPER
class PaperBroker(BrokerAdapter):
    name = "paper"

    async def place_order(self, symbol, side, qty, *, ltp=None, tag=""):
        if ltp is None:
            raise BrokerError("Paper fill needs a price")
        return {"order_id": f"PAPER-{uuid.uuid4().hex[:10]}", "status": "COMPLETE", "avg_price": float(ltp)}

    async def funds(self):
        state = await db().app_state.find_one({"_id": "runtime"}) or {}
        capital = float(state.get("paper_capital", 1_000_000))
        realized = 0.0
        async for t in db().trades.find({"mode": "paper"}, {"net_pnl": 1}):
            realized += t.get("net_pnl", 0)
        used = 0.0
        async for p in db().positions.find({"mode": "paper", "status": "OPEN"}):
            used += p["entry_price"] * p["qty"] / 5  # MIS ~5x leverage
        total = capital + realized
        return {"available": round(total - used, 2), "used": round(used, 2), "total": round(total, 2),
                "starting_capital": capital, "realized_pnl_all_time": round(realized, 2)}


# ================================================================== UPSTOX
class UpstoxBroker(BrokerAdapter):
    name = "upstox"
    BASE = "https://api.upstox.com"
    HFT = "https://api-hft.upstox.com"

    def __init__(self, key_getter):
        self._key = key_getter

    async def _req(self, method, url, **kw):
        token = await get_token("upstox")
        if not token:
            raise BrokerError("Upstox not connected")
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.request(method, url, headers={"Authorization": f"Bearer {token}",
                                                      "Accept": "application/json",
                                                      "Content-Type": "application/json"}, **kw)
        js = r.json() if r.content else {}
        if r.status_code >= 400 or js.get("status") == "error":
            raise BrokerError(f"Upstox {r.status_code}: {js.get('errors') or r.text[:300]}")
        return js

    async def place_order(self, symbol, side, qty, *, ltp=None, tag=""):
        body = {"quantity": int(qty), "product": "I", "validity": "DAY", "price": 0, "tag": tag[:20] or "farooqbot",
                "instrument_token": await self._key(symbol), "order_type": "MARKET",
                "transaction_type": side, "disclosed_quantity": 0, "trigger_price": 0, "is_amo": False}
        js = await self._req("POST", f"{self.HFT}/v3/order/place", json=body)
        data = js.get("data") or {}
        oid = (data.get("order_ids") or [data.get("order_id")])[0]
        avg = await self._avg_price(oid, ltp)
        return {"order_id": oid, "status": "COMPLETE" if avg else "OPEN", "avg_price": avg or ltp}

    async def _avg_price(self, oid, fallback):
        for _ in range(5):
            await asyncio.sleep(0.6)
            try:
                js = await self._req("GET", f"{self.BASE}/v2/order/details", params={"order_id": oid})
                d = js.get("data") or {}
                if d.get("status") == "complete":
                    return float(d.get("average_price") or fallback)
                if d.get("status") in ("rejected", "cancelled"):
                    raise BrokerError(f"Order {oid} {d.get('status')}: {d.get('status_message')}")
            except BrokerError:
                raise
            except Exception:  # noqa: BLE001
                pass
        return None

    async def funds(self):
        js = await self._req("GET", f"{self.BASE}/v2/user/get-funds-and-margin", params={"segment": "SEC"})
        eq = (js.get("data") or {}).get("equity", {})
        avail = float(eq.get("available_margin", 0))
        used = float(eq.get("used_margin", 0))
        return {"available": avail, "used": used, "total": avail + used, "raw": eq}

    async def positions(self):
        js = await self._req("GET", f"{self.BASE}/v2/portfolio/short-term-positions")
        return js.get("data") or []

    async def orders(self):
        js = await self._req("GET", f"{self.BASE}/v2/order/retrieve-all")
        return js.get("data") or []

    async def profile(self):
        js = await self._req("GET", f"{self.BASE}/v2/user/profile")
        return js.get("data") or {}


# ================================================================== KITE
class KiteBroker(BrokerAdapter):
    name = "kite"

    async def _kite(self):
        from kiteconnect import KiteConnect
        token = await get_token("kite")
        if not token:
            raise BrokerError("Kite not connected")
        k = KiteConnect(api_key=get_settings().kite_api_key)
        k.set_access_token(token)
        return k

    async def place_order(self, symbol, side, qty, *, ltp=None, tag=""):
        k = await self._kite()
        try:
            oid = await asyncio.to_thread(
                k.place_order, variety=k.VARIETY_REGULAR, exchange=k.EXCHANGE_NSE, tradingsymbol=symbol,
                transaction_type=side, quantity=int(qty), product=k.PRODUCT_MIS, order_type=k.ORDER_TYPE_MARKET,
                tag=(tag or "farooqbot")[:20])
        except Exception as e:  # noqa: BLE001
            raise BrokerError(f"Kite order failed: {e}") from e
        avg = None
        for _ in range(5):
            await asyncio.sleep(0.6)
            try:
                hist = await asyncio.to_thread(k.order_history, oid)
                last = hist[-1] if hist else {}
                if last.get("status") == "COMPLETE":
                    avg = float(last.get("average_price") or ltp)
                    break
                if last.get("status") in ("REJECTED", "CANCELLED"):
                    raise BrokerError(f"Kite order {oid} {last.get('status')}: {last.get('status_message')}")
            except BrokerError:
                raise
            except Exception:  # noqa: BLE001
                pass
        return {"order_id": oid, "status": "COMPLETE" if avg else "OPEN", "avg_price": avg or ltp}

    async def funds(self):
        k = await self._kite()
        m = await asyncio.to_thread(k.margins, "equity")
        avail = float(m.get("available", {}).get("live_balance", m.get("net", 0)))
        used = float(m.get("utilised", {}).get("debits", 0))
        return {"available": avail, "used": used, "total": float(m.get("net", avail)), "raw": m}

    async def positions(self):
        k = await self._kite()
        return (await asyncio.to_thread(k.positions)).get("day", [])

    async def orders(self):
        k = await self._kite()
        return await asyncio.to_thread(k.orders)

    async def profile(self):
        k = await self._kite()
        return await asyncio.to_thread(k.profile)


# ================================================================== ANGEL ONE
class AngelBroker(BrokerAdapter):
    name = "angel"

    async def place_order(self, symbol, side, qty, *, ltp=None, tag=""):
        from . import angel
        try:
            token, tsym = await angel.token_for(symbol)
            data = await angel.call("POST", "/rest/secure/angelbroking/order/v1/placeOrder", json_body={
                "variety": "NORMAL", "tradingsymbol": tsym, "symboltoken": token, "transactiontype": side,
                "exchange": "NSE", "ordertype": "MARKET", "producttype": "INTRADAY", "duration": "DAY",
                "price": "0", "squareoff": "0", "stoploss": "0", "quantity": str(int(qty)),
                "ordertag": (tag or "wwalgo")[:20]})
        except angel.AngelError as e:
            raise BrokerError(str(e)) from e
        oid = (data or {}).get("orderid")
        if not oid:
            raise BrokerError(f"Angel One returned no order id: {data}")
        avg = None
        for _ in range(6):
            await asyncio.sleep(0.7)
            try:
                book = await angel.call("GET", "/rest/secure/angelbroking/order/v1/getOrderBook") or []
            except angel.AngelError:
                continue
            o = next((x for x in book if str(x.get("orderid")) == str(oid)), None)
            if not o:
                continue
            st = str(o.get("orderstatus") or o.get("status") or "").lower()
            if st == "complete":
                avg = float(o.get("averageprice") or ltp or 0) or ltp
                break
            if st in ("rejected", "cancelled"):
                raise BrokerError(f"Angel order {oid} {st}: {o.get('text')}")
        return {"order_id": oid, "status": "COMPLETE" if avg else "OPEN", "avg_price": avg or ltp}

    async def funds(self):
        from . import angel
        try:
            d = await angel.call("GET", "/rest/secure/angelbroking/user/v1/getRMS") or {}
        except angel.AngelError as e:
            raise BrokerError(str(e)) from e
        avail = float(d.get("availablecash") or 0)
        used = float(d.get("utilisedamount") or d.get("utiliseddebits") or 0)
        return {"available": avail, "used": used, "total": float(d.get("net") or avail + used), "raw": d}

    async def positions(self):
        from . import angel
        return await angel.call("GET", "/rest/secure/angelbroking/order/v1/getPosition") or []

    async def orders(self):
        from . import angel
        return await angel.call("GET", "/rest/secure/angelbroking/order/v1/getOrderBook") or []

    async def profile(self):
        from . import angel
        try:
            return await angel.call("GET", "/rest/secure/angelbroking/user/v1/getProfile") or {}
        except angel.AngelError as e:
            raise BrokerError(str(e)) from e


def get_broker(name: str) -> BrokerAdapter:
    from ..data.market_data import market_data
    if name == "angel":
        return AngelBroker()
    if name == "upstox":
        return UpstoxBroker(market_data.upstox_key)
    if name == "kite":
        return KiteBroker()
    return PaperBroker()


def utcnow():
    return datetime.now(timezone.utc)
