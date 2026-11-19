"""Runtime state (mode, broker, engine on/off) + versioned strategy params, both in Mongo."""
from datetime import datetime, timezone

from ..db import db
from ..strategy.params import StrategyParams

RUNTIME_DEFAULTS = {
    "mode": "paper",              # paper | live
    "broker": "angel",            # execution broker when mode == live: angel | upstox | kite
    "engine_enabled": True,       # auto-trading on/off
    "paper_capital": 1_000_000.0,
    "halted_date": None,          # kill switch / daily loss limit -> no more entries today
}


async def get_runtime() -> dict:
    doc = await db().app_state.find_one({"_id": "runtime"}) or {}
    return {**RUNTIME_DEFAULTS, **{k: v for k, v in doc.items() if k != "_id"}}


async def update_runtime(**changes) -> dict:
    await db().app_state.update_one({"_id": "runtime"}, {"$set": changes}, upsert=True)
    return await get_runtime()


async def get_params() -> StrategyParams:
    doc = await db().strategy_config.find_one({"active": True}, sort=[("version", -1)])
    return StrategyParams(**doc["params"]) if doc else StrategyParams()


async def save_params(params: StrategyParams, note: str = "") -> int:
    last = await db().strategy_config.find_one({}, sort=[("version", -1)])
    version = (last["version"] + 1) if last else 1
    await db().strategy_config.update_many({"active": True}, {"$set": {"active": False}})
    await db().strategy_config.insert_one({"version": version, "active": True, "params": params.model_dump(),
                                           "note": note, "created_at": datetime.now(timezone.utc)})
    return version
