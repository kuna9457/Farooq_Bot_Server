"""Nifty 50 constituents (from niftyindices.com, cached in Mongo) + market cap (shares outstanding x price)."""
import asyncio
import io
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

from ..db import db

log = logging.getLogger("universe")
NIFTY_CSV = "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv"
NIFTY_INDEX = "NIFTY50"

# Fallback if the NSE list cannot be downloaded (symbol, ISIN)
FALLBACK = [
    ("ADANIENT", "INE423A01024"), ("ADANIPORTS", "INE742F01042"), ("APOLLOHOSP", "INE437A01024"),
    ("ASIANPAINT", "INE021A01026"), ("AXISBANK", "INE238A01034"), ("BAJAJ-AUTO", "INE917I01010"),
    ("BAJFINANCE", "INE296A01032"), ("BAJAJFINSV", "INE918I01026"), ("BEL", "INE263A01024"),
    ("BHARTIARTL", "INE397D01024"), ("CIPLA", "INE059A01026"), ("COALINDIA", "INE522F01014"),
    ("DRREDDY", "INE089A01031"), ("EICHERMOT", "INE066A01021"), ("ETERNAL", "INE758T01015"),
    ("GRASIM", "INE047A01021"), ("HCLTECH", "INE860A01027"), ("HDFCBANK", "INE040A01034"),
    ("HDFCLIFE", "INE795G01014"), ("HINDALCO", "INE038A01020"), ("HINDUNILVR", "INE030A01027"),
    ("ICICIBANK", "INE090A01021"), ("INDIGO", "INE646L01027"), ("INFY", "INE009A01021"),
    ("ITC", "INE154A01025"), ("JIOFIN", "INE758E01017"), ("JSWSTEEL", "INE019A01038"),
    ("KOTAKBANK", "INE237A01036"), ("LT", "INE018A01030"), ("M&M", "INE101A01026"),
    ("MARUTI", "INE585B01010"), ("MAXHEALTH", "INE027H01010"), ("NESTLEIND", "INE239A01024"),
    ("NTPC", "INE733E01010"), ("ONGC", "INE213A01029"), ("POWERGRID", "INE752E01010"),
    ("RELIANCE", "INE002A01018"), ("SBILIFE", "INE123W01016"), ("SBIN", "INE062A01020"),
    ("SHRIRAMFIN", "INE721A01047"), ("SUNPHARMA", "INE044A01036"), ("TATACONSUM", "INE192A01025"),
    ("TMPV", "INE155A01022"), ("TATASTEEL", "INE081A01020"), ("TCS", "INE467B01029"),
    ("TECHM", "INE669C01036"), ("TITAN", "INE280A01028"), ("TRENT", "INE849A01020"),
    ("ULTRACEMCO", "INE481G01011"), ("WIPRO", "INE075A01022"),
]


def _download_list() -> list[dict]:
    r = requests.get(NIFTY_CSV, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    return [{"symbol": s.strip(), "isin": i.strip(), "name": n.strip(), "industry": ind.strip()}
            for s, i, n, ind in zip(df["Symbol"], df["ISIN Code"], df["Company Name"], df["Industry"])]


async def refresh_universe(force: bool = False) -> list[dict]:
    doc = await db().universe.find_one({"_id": "nifty50"})
    fresh = doc and doc.get("updated_at") and \
        datetime.now(timezone.utc) - _aware(doc["updated_at"]) < timedelta(days=7)
    if doc and fresh and not force:
        return doc["constituents"]
    try:
        items = await asyncio.to_thread(_download_list)
        if len(items) < 45:
            raise ValueError(f"only {len(items)} rows")
    except Exception as e:  # noqa: BLE001
        log.warning("Nifty50 list download failed (%s); using cached/fallback", e)
        if doc:
            return doc["constituents"]
        items = [{"symbol": s, "isin": i, "name": s, "industry": ""} for s, i in FALLBACK]
    await db().universe.update_one({"_id": "nifty50"},
                                   {"$set": {"constituents": items, "updated_at": datetime.now(timezone.utc)}},
                                   upsert=True)
    return items


async def get_universe() -> list[dict]:
    doc = await db().universe.find_one({"_id": "nifty50"})
    return doc["constituents"] if doc else await refresh_universe()


def _aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# ------------------------------------------------------------------ market cap
def _fetch_shares(symbols: list[str]) -> dict[str, float]:
    import yfinance as yf
    from .providers import yf_symbol
    out = {}
    for s in symbols:
        try:
            out[s] = float(yf.Ticker(yf_symbol(s)).fast_info["shares"])
        except Exception as e:  # noqa: BLE001
            log.warning("shares lookup failed for %s: %s", s, e)
    return out


async def refresh_fundamentals(force: bool = False) -> dict[str, float]:
    universe = await get_universe()
    symbols = [u["symbol"] for u in universe]
    existing = {d["symbol"]: d async for d in db().fundamentals.find({})}
    now = datetime.now(timezone.utc)
    stale = [s for s in symbols if force or s not in existing
             or now - _aware(existing[s]["updated_at"]) > timedelta(days=7)]
    if stale:
        log.info("Refreshing shares outstanding for %d symbols", len(stale))
        fetched = await asyncio.to_thread(_fetch_shares, stale)
        for s, sh in fetched.items():
            await db().fundamentals.update_one({"symbol": s}, {"$set": {"shares": sh, "updated_at": now}}, upsert=True)
            existing[s] = {"symbol": s, "shares": sh, "updated_at": now}
    return {s: existing[s]["shares"] for s in symbols if s in existing}
