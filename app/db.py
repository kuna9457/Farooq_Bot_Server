"""MongoDB connection (motor). Falls back to in-memory mongomock when Mongo is unreachable (dev only)."""
import logging
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from .config import get_settings

log = logging.getLogger("db")

_client = None
_db: AsyncIOMotorDatabase | None = None
using_mock = False


async def connect() -> AsyncIOMotorDatabase:
    global _client, _db, using_mock
    s = get_settings()
    try:
        _client = AsyncIOMotorClient(s.mongo_uri, serverSelectionTimeoutMS=3000, tz_aware=True)
        await _client.admin.command("ping")
        _db = _client[s.mongo_db]
        log.info("Connected to MongoDB %s/%s", s.mongo_uri, s.mongo_db)
    except Exception as e:  # noqa: BLE001
        if not s.allow_mock_db:
            raise
        from mongomock_motor import AsyncMongoMockClient
        log.warning("MongoDB unreachable (%s) -> using IN-MEMORY mock DB. Data will NOT persist!", e)
        _client = AsyncMongoMockClient(tz_aware=True)
        _db = _client[s.mongo_db]
        using_mock = True
    await _ensure_indexes(_db)
    return _db


async def _ensure_indexes(db: AsyncIOMotorDatabase) -> None:
    try:
        await db.candles.create_index([("symbol", 1), ("tf", 1), ("ts", 1)], unique=True)
        await db.daily_sessions.create_index("date", unique=True)
        await db.filter_states.create_index([("date", 1), ("symbol", 1)])
        await db.filter_history.create_index([("date", 1), ("symbol", 1), ("ts", 1)])
        await db.signals.create_index([("date", 1)])
        await db.orders.create_index([("date", 1)])
        await db.trades.create_index([("date", -1)])
        await db.positions.create_index([("mode", 1), ("status", 1)])
        await db.pnl_snapshots.create_index([("date", 1), ("ts", 1)])
        await db.backtest_trades.create_index("run_id")
        await db.event_logs.create_index([("ts", -1)])
    except Exception as e:  # noqa: BLE001
        log.warning("index creation skipped: %s", e)


def db() -> AsyncIOMotorDatabase:
    assert _db is not None, "DB not connected"
    return _db


def clean(doc):
    """Make a mongo doc JSON friendly."""
    if doc is None:
        return None
    if isinstance(doc, list):
        return [clean(d) for d in doc]
    d = dict(doc)
    if "_id" in d:
        d["_id"] = str(d["_id"])
    return d
