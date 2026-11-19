"""Daily job schedule (IST, Mon-Fri). Holidays are detected automatically (no Nifty bars today -> skip)."""
import logging
from datetime import time

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from ..core.events import log_event
from ..core.state import get_params
from ..data.market_data import completed_5m, market_data, now_ist
from ..data.universe import NIFTY_INDEX
from ..strategy.engine import engine

log = logging.getLogger("scheduler")
TZ = "Asia/Kolkata"
scheduler = AsyncIOScheduler(timezone=TZ)


async def _is_trading_day() -> bool:
    try:
        bars = await market_data.intraday_5m(NIFTY_INDEX, days=2, ttl=60)
        return bool((bars.index.date == now_ist().date()).any())
    except Exception:  # noqa: BLE001
        return True  # don't block on data errors


async def job_scan():
    await engine.pre_market_scan()


async def job_regime():
    if not await _is_trading_day():
        await log_event("info", "scheduler", "No market data today (holiday?) — skipping regime")
        return
    await engine.decide_regime()


async def job_evaluate():
    s = await engine.session()
    if not s or not s.get("regime"):
        return
    p = await get_params()
    sq = time(*map(int, p.risk.square_off_time.split(":")))
    if now_ist().time() >= sq:
        return
    await engine.evaluate_all(allow_orders=True)


async def job_square_off():
    await engine.square_off("Time square-off")


async def job_eod():
    await engine.end_of_day()


async def _square_off_time_job():
    p = await get_params()
    h, m = map(int, p.risk.square_off_time.split(":"))
    scheduler.add_job(job_square_off, CronTrigger(day_of_week="mon-fri", hour=h, minute=m, timezone=TZ),
                      id="square_off", replace_existing=True)


async def start_scheduler():
    p = await get_params()
    rh, rm = map(int, p.regime.decision_time.split(":"))
    scheduler.add_job(job_scan, CronTrigger(day_of_week="mon-fri", hour=8, minute=30, timezone=TZ), id="scan",
                      replace_existing=True, misfire_grace_time=3600)
    scheduler.add_job(job_regime, CronTrigger(day_of_week="mon-fri", hour=rh, minute=rm, second=10, timezone=TZ),
                      id="regime", replace_existing=True, misfire_grace_time=1800)
    # every 5 minutes, 20s after the candle closes (gives the data provider time to publish the bar)
    scheduler.add_job(job_evaluate, CronTrigger(day_of_week="mon-fri", hour="9-15", minute="*/5", second=20, timezone=TZ),
                      id="evaluate", replace_existing=True, max_instances=1, coalesce=True)
    await _square_off_time_job()
    scheduler.add_job(job_eod, CronTrigger(day_of_week="mon-fri", hour=15, minute=40, timezone=TZ), id="eod",
                      replace_existing=True)
    scheduler.start()
    await catch_up()


async def catch_up():
    """If the server starts mid-day, run the jobs that were missed."""
    n = now_ist()
    if n.weekday() >= 5:
        return
    s = await engine.session()
    if n.time() >= time(8, 30) and not (s and s.get("scanned_at")):
        await log_event("info", "scheduler", "Catch-up: running missed pre-market scan")
        await engine.pre_market_scan()
        s = await engine.session()
    p = await get_params()
    rh, rm = map(int, p.regime.decision_time.split(":"))
    if time(rh, rm) <= n.time() and not (s and s.get("regime")):
        await log_event("info", "scheduler", "Catch-up: deciding missed regime")
        await job_regime()


async def reschedule_from_params():
    if scheduler.running:
        p = await get_params()
        rh, rm = map(int, p.regime.decision_time.split(":"))
        scheduler.reschedule_job("regime", trigger=CronTrigger(day_of_week="mon-fri", hour=rh, minute=rm, second=10, timezone=TZ))
        await _square_off_time_job()


def jobs_info() -> list[dict]:
    return [{"id": j.id, "next_run": j.next_run_time.isoformat() if j.next_run_time else None}
            for j in scheduler.get_jobs()]
