"""Angel One SmartAPI plumbing: session (TOTP auto-login or Publisher-login callback), REST helper,
instrument-token map and the live WebSocket tick feed. Used by the Angel data provider and broker."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import struct
import time
from datetime import datetime, timezone

import httpx

from ..config import get_settings
from ..db import db
from .tokens import dec, enc, get_token, save_token

log = logging.getLogger("angel")

NIFTY_TOKEN = ("99926000", "Nifty 50")       # NSE index token / name
INSTRUMENTS_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
WS_URL = "wss://smartapisocket.angelone.in/smart-stream"


class AngelError(Exception):
    pass


# ---------------------------------------------------------------- TOTP (RFC 6238, no extra dependency)
def totp_now(secret: str, step: int = 30, digits: int = 6) -> str:
    sec = secret.replace(" ", "").upper()
    key = base64.b32decode(sec + "=" * (-len(sec) % 8))
    h = hmac.new(key, struct.pack(">Q", int(time.time()) // step), hashlib.sha1).digest()
    o = h[-1] & 0x0F
    code = (struct.unpack(">I", h[o:o + 4])[0] & 0x7FFFFFFF) % (10 ** digits)
    return str(code).zfill(digits)


def configured() -> bool:
    return bool(get_settings().angel_api_key)


async def creds() -> dict | None:
    """Login credentials for automatic daily login: entered by the user on the Broker page (stored encrypted in
    Mongo), falling back to ANGEL_CLIENT_CODE / ANGEL_PIN / ANGEL_TOTP_SECRET from the environment."""
    doc = await db().broker_accounts.find_one({"broker": "angel_creds"})
    if doc and doc.get("client_code") and doc.get("pin_enc") and doc.get("totp_secret_enc"):
        pin, sec = dec(doc["pin_enc"]), dec(doc["totp_secret_enc"])
        if pin and sec:
            return {"client_code": doc["client_code"], "pin": pin, "totp_secret": sec, "source": "user"}
    s = get_settings()
    if s.angel_client_code and s.angel_pin and s.angel_totp_secret:
        return {"client_code": s.angel_client_code, "pin": s.angel_pin, "totp_secret": s.angel_totp_secret, "source": "env"}
    return None


async def can_auto_login() -> bool:
    return bool(get_settings().angel_api_key and await creds())


async def save_creds(client_code: str, pin: str, totp_secret: str) -> None:
    await db().broker_accounts.update_one(
        {"broker": "angel_creds"},
        {"$set": {"broker": "angel_creds", "client_code": client_code, "pin_enc": enc(pin),
                  "totp_secret_enc": enc(totp_secret), "saved_at": datetime.now(timezone.utc)}}, upsert=True)


async def forget_creds() -> None:
    await db().broker_accounts.delete_one({"broker": "angel_creds"})


def _headers(jwt: str | None = None) -> dict:
    s = get_settings()
    h = {"Content-Type": "application/json", "Accept": "application/json", "X-UserType": "USER",
         "X-SourceID": "WEB", "X-ClientLocalIP": s.angel_local_ip, "X-ClientPublicIP": s.angel_public_ip,
         "X-MACAddress": s.angel_mac, "X-PrivateKey": s.angel_api_key}
    if jwt:
        h["Authorization"] = f"Bearer {jwt}"
    return h


def _raw(jwt: str) -> str:
    return jwt[7:] if jwt.lower().startswith("bearer ") else jwt


# ---------------------------------------------------------------- session
_session_lock = asyncio.Lock()


async def store_session(jwt: str, feed: str, refresh: str, client_code: str) -> None:
    await save_token("angel", _raw(jwt), {"feed_token_enc": enc(feed) if feed else "",
                                          "refresh_token_enc": enc(refresh) if refresh else "",
                                          "client_code": client_code, "user_name": client_code})
    feed_ws.reset()


async def _login_totp(client_code: str = "", pin: str = "", totp: str = "") -> None:
    """Log in with the given credentials, or with the saved ones (generating the TOTP from the saved secret)."""
    s = get_settings()
    if not s.angel_api_key:
        raise AngelError("ANGEL_API_KEY is not set on the server")
    if not (client_code and pin and totp):
        c0 = await creds()
        if not c0:
            raise AngelError("No saved Angel One credentials. Enter your client code, PIN and TOTP on the Broker page.")
        client_code, pin, totp = c0["client_code"], c0["pin"], totp_now(c0["totp_secret"])
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(s.angel_base_url + "/rest/auth/angelbroking/user/v1/loginByPassword", headers=_headers(),
                         json={"clientcode": client_code, "password": pin, "totp": totp})
    try:
        js = r.json()
    except Exception:  # noqa: BLE001
        raise AngelError(f"Angel login failed ({r.status_code}): {r.text[:200]}") from None
    if not js.get("status") or not js.get("data"):
        raise AngelError(f"Angel login failed: {js.get('message') or js}")
    d = js["data"]
    await store_session(d["jwtToken"], d.get("feedToken", ""), d.get("refreshToken", ""), client_code)
    log.info("Angel One session created (TOTP login)")


async def login_totp(client_code: str = "", pin: str = "", totp: str = "") -> None:
    async with _session_lock:
        await _login_totp(client_code, pin, totp)


async def _current() -> dict | None:
    s = get_settings()
    tok = await get_token("angel")
    if not tok:
        return None
    doc = await db().broker_accounts.find_one({"broker": "angel"}) or {}
    return {"jwt": tok, "feed": dec(doc["feed_token_enc"]) if doc.get("feed_token_enc") else "",
            "client": doc.get("client_code") or s.angel_client_code, "api_key": s.angel_api_key}


async def session(force: bool = False) -> dict:
    """Valid session {jwt, feed, client, api_key}; logs in with TOTP when missing/expired (if credentials are set)."""
    if not get_settings().angel_api_key:
        raise AngelError("ANGEL_API_KEY is not set")
    async with _session_lock:
        if not force:
            cur = await _current()
            if cur:
                return cur
        if await can_auto_login():
            await _login_totp()
            cur = await _current()
            if cur:
                return cur
    raise AngelError("Angel One not connected. Login from the Broker page.")


# ---------------------------------------------------------------- REST
_rate_lock = asyncio.Lock()
_next_ok = 0.0
MIN_GAP = 0.4   # SmartAPI allows roughly 3 requests/second on historical + quote endpoints


async def _gate() -> None:
    global _next_ok
    async with _rate_lock:
        wait = _next_ok - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)
        _next_ok = time.monotonic() + MIN_GAP


def _token_error(js: dict) -> bool:
    msg = f"{js.get('message', '')} {js.get('errorcode', '')}".lower()
    return any(x in msg for x in ("invalid token", "token expired", "ag8001", "ag8002", "ag8003", "session expired"))


async def call(method: str, path: str, *, json_body: dict | None = None, params: dict | None = None,
               _tries: int = 0, _relogin: bool = True):
    """Authenticated SmartAPI request. Returns the `data` payload. Retries rate limits; re-logs in on token expiry."""
    s = get_settings()
    sess = await session()
    await _gate()
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.request(method, s.angel_base_url + path, headers=_headers(sess["jwt"]), json=json_body, params=params)
    try:
        js = r.json()
    except Exception:  # noqa: BLE001
        js = {}
    text = r.text[:200]
    if r.status_code == 429 or "exceeding access rate" in text.lower() or "exceeding access rate" in str(js.get("message", "")).lower():
        if _tries < 4:
            await asyncio.sleep(1.5 * (_tries + 1))
            return await call(method, path, json_body=json_body, params=params, _tries=_tries + 1, _relogin=_relogin)
        raise AngelError("Angel rate limit exceeded")
    if (r.status_code in (401, 403) or _token_error(js)) and _relogin and await can_auto_login():
        await session(force=True)
        return await call(method, path, json_body=json_body, params=params, _tries=_tries, _relogin=False)
    if r.status_code >= 400 or not js.get("status"):
        raise AngelError(f"Angel {r.status_code}: {js.get('message') or text} {js.get('errorcode') or ''}".strip())
    return js.get("data")


# ---------------------------------------------------------------- instrument tokens
_tokens: dict[str, tuple[str, str]] = {}      # symbol -> (token, trading symbol)
_tokens_loaded = 0.0


async def _download_tokens(wanted: set[str]) -> dict[str, tuple[str, str]]:
    """Stream the (very large) scrip master and keep only the Nifty 50 cash symbols (low memory)."""
    decoder, buf, out = json.JSONDecoder(), "", {}
    async with httpx.AsyncClient(timeout=180) as c, c.stream("GET", INSTRUMENTS_URL) as r:
        r.raise_for_status()
        async for chunk in r.aiter_text():
            buf += chunk
            pos = 0
            while True:
                while pos < len(buf) and buf[pos] in " \r\n\t,[":
                    pos += 1
                if pos >= len(buf) or buf[pos] == "]":
                    break
                try:
                    obj, end = decoder.raw_decode(buf, pos)
                except ValueError:
                    break  # incomplete object, wait for the next chunk
                pos = end
                if obj.get("exch_seg") == "NSE":
                    sym = obj.get("symbol", "")
                    if sym.endswith("-EQ") and sym[:-3] in wanted:
                        out[sym[:-3]] = (str(obj["token"]), sym)
            buf = buf[pos:]
            await asyncio.sleep(0)
    return out


async def symbol_tokens() -> dict[str, tuple[str, str]]:
    global _tokens, _tokens_loaded
    if _tokens and time.time() - _tokens_loaded < 86400:
        return _tokens
    from ..data.universe import NIFTY_INDEX, get_universe
    doc = await db().app_state.find_one({"_id": "angel_instruments"})
    uni = {u["symbol"] for u in await get_universe()}
    fresh = False
    if doc:
        upd = doc["updated_at"]
        upd = upd.replace(tzinfo=timezone.utc) if upd.tzinfo is None else upd
        fresh = (datetime.now(timezone.utc) - upd).days < 7 and uni <= {r[0] for r in doc["rows"]}
    if fresh:
        m = {r[0]: (r[1], r[2]) for r in doc["rows"]}
    else:
        log.info("Downloading Angel One instrument master…")
        m = await _download_tokens(uni)
        missing = sorted(uni - set(m))
        if missing:
            log.warning("No Angel token for: %s", ", ".join(missing))
        await db().app_state.update_one(
            {"_id": "angel_instruments"},
            {"$set": {"rows": [[k, v[0], v[1]] for k, v in m.items()], "updated_at": datetime.now(timezone.utc)}},
            upsert=True)
    m[NIFTY_INDEX] = NIFTY_TOKEN
    _tokens, _tokens_loaded = m, time.time()
    return m


async def token_for(symbol: str) -> tuple[str, str]:
    m = await symbol_tokens()
    if symbol not in m:
        raise AngelError(f"No Angel One instrument token for {symbol}")
    return m[symbol]


# ---------------------------------------------------------------- WebSocket tick feed (LTP, mode 1)
class AngelFeed:
    """Persistent SmartAPI WebSocket. Keeps the latest LTP per symbol; reconnects (with re-login) on failure."""

    def __init__(self):
        self.ltp: dict[str, float] = {}
        self.ts: dict[str, float] = {}
        self.connected = False
        self._by_token: dict[str, str] = {}      # token -> symbol
        self._task: asyncio.Task | None = None
        self._ws = None
        self._restart = False

    def reset(self) -> None:
        """New session tokens -> drop the socket so it reconnects with them."""
        self._restart = True
        ws = self._ws
        if ws is not None:
            try:
                asyncio.get_running_loop().create_task(ws.close())
            except RuntimeError:
                pass

    def fresh(self, symbol: str, max_age: float = 15.0) -> float | None:
        t = self.ts.get(symbol)
        return self.ltp.get(symbol) if t and time.time() - t <= max_age else None

    async def ensure(self, symbols: list[str]) -> None:
        new = []
        for s in symbols:
            tok, _ = await token_for(s)
            if tok not in self._by_token:
                self._by_token[tok] = s
                new.append((s, tok))
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())
        elif new and self._ws is not None and self.connected:
            await self._send_subscribe(self._ws, new)

    @staticmethod
    async def _send_subscribe(ws, items: list[tuple[str, str]]) -> None:
        if items:
            await ws.send(json.dumps({"correlationID": "wwalgo", "action": 1, "params": {
                "mode": 1, "tokenList": [{"exchangeType": 1, "tokens": [t for _, t in items]}]}}))

    async def _run(self) -> None:
        from websockets.asyncio.client import connect
        backoff = 2
        while True:
            try:
                sess = await session()
                if not sess["feed"]:
                    raise AngelError("No feed token (login again to enable the WebSocket)")
                headers = {"Authorization": _raw(sess["jwt"]), "x-api-key": sess["api_key"],
                           "x-client-code": sess["client"], "x-feed-token": sess["feed"]}
                async with connect(WS_URL, additional_headers=headers, ping_interval=None, max_size=2 ** 22) as ws:
                    self._ws, self.connected, self._restart, backoff = ws, True, False, 2
                    log.info("Angel WebSocket connected (%d symbols)", len(self._by_token))
                    await self._send_subscribe(ws, [(s, t) for t, s in self._by_token.items()])
                    hb = asyncio.create_task(self._heartbeat(ws))
                    try:
                        async for msg in ws:
                            if isinstance(msg, (bytes, bytearray)):
                                self._on_binary(bytes(msg))
                    finally:
                        hb.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.warning("Angel WebSocket error: %s", e)
            finally:
                self.connected, self._ws = False, None
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    @staticmethod
    async def _heartbeat(ws) -> None:
        while True:
            await asyncio.sleep(10)
            await ws.send("ping")

    def _on_binary(self, data: bytes) -> None:
        if len(data) < 51:
            return
        token = data[2:27].split(b"\x00")[0].decode(errors="ignore")
        ltp_paise = struct.unpack("<q", data[43:51])[0]
        sym = self._by_token.get(token)
        if sym and ltp_paise > 0:
            self.ltp[sym] = ltp_paise / 100.0
            self.ts[sym] = time.time()


feed_ws = AngelFeed()
