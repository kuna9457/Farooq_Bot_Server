"""Encrypted broker token storage (Mongo `broker_accounts`). Tokens expire daily (Upstox ~03:30 IST, Kite ~06:00 IST)."""
import base64
import hashlib
from datetime import datetime, timedelta, timezone

from cryptography.fernet import Fernet, InvalidToken

from ..config import get_settings
from ..db import db


def _fernet() -> Fernet:
    s = get_settings()
    key = s.encryption_key or base64.urlsafe_b64encode(hashlib.sha256(s.jwt_secret.encode()).digest()).decode()
    return Fernet(key.encode())


def enc(text: str) -> str:
    return _fernet().encrypt(text.encode()).decode()


def dec(text: str) -> str:
    try:
        return _fernet().decrypt(text.encode()).decode()
    except InvalidToken:
        return ""


def _next_expiry(broker: str) -> datetime:
    """Next expiry in UTC: Upstox 03:30 IST (22:00 UTC), Kite 06:00 IST (00:30 UTC), Angel One 05:00 IST (23:30 UTC)."""
    now = datetime.now(timezone.utc)
    h, m = {"upstox": (22, 0), "angel": (23, 30)}.get(broker, (0, 30))
    exp = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if exp <= now:
        exp += timedelta(days=1)
    return exp


async def save_token(broker: str, token: str, extra: dict | None = None) -> None:
    await db().broker_accounts.update_one(
        {"broker": broker},
        {"$set": {"broker": broker, "access_token_enc": _fernet().encrypt(token.encode()).decode(),
                  "token_expiry": _next_expiry(broker), "connected_at": datetime.now(timezone.utc),
                  **(extra or {})}},
        upsert=True)


async def get_token(broker: str) -> str | None:
    s = get_settings()
    doc = await db().broker_accounts.find_one({"broker": broker})
    if doc and doc.get("access_token_enc"):
        exp = doc.get("token_expiry")
        if exp and exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if exp is None or exp > datetime.now(timezone.utc):
            try:
                return _fernet().decrypt(doc["access_token_enc"].encode()).decode()
            except InvalidToken:
                pass
    fallback = {"upstox": s.upstox_access_token, "kite": s.kite_access_token}.get(broker, "")
    return fallback or None


async def token_status(broker: str) -> dict:
    doc = await db().broker_accounts.find_one({"broker": broker}) or {}
    tok = await get_token(broker)
    return {"broker": broker, "connected": bool(tok), "token_expiry": doc.get("token_expiry"),
            "connected_at": doc.get("connected_at"), "user_name": doc.get("user_name")}


async def clear_token(broker: str) -> None:
    await db().broker_accounts.update_one({"broker": broker}, {"$unset": {"access_token_enc": "", "feed_token_enc": "", "refresh_token_enc": ""}})
