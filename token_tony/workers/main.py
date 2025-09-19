# -*- coding: utf-8 -*-
"""Main worker for Token Tony."""
from __future__ import annotations

import asyncio
import logging

from telegram.ext import Application

from config import BIRDEYE_API_KEY, HELIUS_API_KEY, PUBLIC_CHAT_ID, VIP_CHAT_ID, compute_config_sanity
from token_tony.db_core import setup_database
from token_tony.reports import load_advanced_quips
from token_tony.utils.telegram import _can_post_to_chat, _notify_owner
from token_tony.workers.discovery import (
    aggregator_poll_worker,
    logs_firehose_worker,
    pumpportal_worker,
)
from token_tony.workers.scheduler import _schedule_pushes, weekly_maintenance_job

log = logging.getLogger(__name__)

FIREHOSE_STATUS: Dict[str, str] = {}

def _start_optional_worker(app: Application, worker_name: str, task_name: str) -> None: 
    worker = globals().get(worker_name)
    if not callable(worker):
        log.warning(f"Skipping {task_name}: {worker_name} is not available.")
        return
    try:
        app.create_task(worker(), name=task_name)
    except Exception as exc:
        log.error(f"Failed to start {task_name}: {exc}")

async def post_init(app: Application) -> None: 
    """Runs async setup and starts background workers after the bot is initialized."""
    await setup_database()
    load_advanced_quips()
    # Config sanity summary at startup
    try:
        global CONFIG_SANITY
        CONFIG_SANITY = compute_config_sanity()
        log.info(
            "Config Sanity: "
            + f"DB={'OK' if CONFIG_SANITY.get('db_writable') else 'FAIL'}, "
            + f"LOG={'OK' if CONFIG_SANITY.get('log_writable') else 'FAIL'}, "
            + f"HELIUS={'OK' if CONFIG_SANITY.get('helius_api') else 'MISSING'}, "
            + f"BIRDEYE={'OK' if CONFIG_SANITY.get('birdeye_api') else 'MISSING'}, "
            + f"DEGRADED={'YES' if CONFIG_SANITY.get('degraded_mode') else 'no'}"
        )
    except Exception as e:
        log.warning(f"Config sanity check failed: {e}")
    
    log.info("✅ Blueprint Engine: Firing up background workers...")
    # Using PumpPortal WS (single socket). Skip client-api.* pump.fun endpoints entirely.
    # Single-socket streams (keep counts low to avoid upstream limits)
    app.create_task(pumpportal_worker(), name="PumpPortalWS") # Tony's discovery worker
    # Use logsSubscribe-based firehose across providers (if configured)
    app.create_task(logs_firehose_worker(), name="LogsFirehoseWorker")
    app.create_task(aggregator_poll_worker(), name="AggregatorPollWorker")
    optional_workers = [
        ("process_discovery_queue", "EnhancedProcessingWorker"),
        ("re_analyzer_worker", "ReAnalyzerWorker"),
        ("maintenance_worker", "MaintenanceWorker"),
        ("circuit_breaker_reset_worker", "CircuitBreakerResetWorker"),
    ]
    for func_name, task_name in optional_workers:
        _start_optional_worker(app, func_name, task_name)

    if not all([BIRDEYE_API_KEY, HELIUS_API_KEY]):
        log.warning("One or more critical API keys (Helius, Birdeye) are missing. Analysis quality will be degraded.")
        FIREHOSE_STATUS.clear()
        FIREHOSE_STATUS["System"] = "🔴 Missing API Key(s)"

    # Schedule Public/VIP push cadences if chat IDs provided

    def _sched_repeating(name: str, secs: int, chat_id: int, segment: str, delay: float = 5.0):
        """Enhanced scheduling with validation and conflict prevention."""
        if not chat_id:
            log.warning(f"Skipping {name}: no chat_id provided")
            return
        # Remove existing job if present to prevent duplicates
        existing_jobs = [job for job in jq.jobs() if job.name == name]
        for job in existing_jobs:
            job.schedule_removal()
            log.info(f"Removed existing job: {name}")
        jq.run_repeating(
            scheduled_push_job,
            interval=secs,
            first=delay + random.uniform(0, 5.0),
            name=name,
            data={"chat_id": chat_id, "segment": segment},
        )
        log.info(f"Scheduled {name} every {secs}s for chat {chat_id} (segment: {segment})")

    jq = app.job_queue

    # Public cadence - only if bot has rights to post
    if PUBLIC_CHAT_ID:
        ok, reason = await _can_post_to_chat(app.bot, PUBLIC_CHAT_ID)
        if ok:
            _sched_repeating("public_hatching", 5 * 60, PUBLIC_CHAT_ID, "hatching")
            _sched_repeating("public_cooking", 60, PUBLIC_CHAT_ID, "cooking") # User request: 60s
            _sched_repeating("public_top", 60 * 60, PUBLIC_CHAT_ID, "top")
            # Continuous fresh cadence every 60 seconds
            _sched_repeating("public_fresh", 60, PUBLIC_CHAT_ID, "fresh")
        else:
            log.error(f"PUBLIC_CHAT_ID={PUBLIC_CHAT_ID} is not writable: {reason}. Auto-pushes not scheduled.")
            await _notify_owner(app.bot, f"<b>Setup required:</b> Bot lacks post rights for PUBLIC chat <code>{PUBLIC_CHAT_ID}</code> ({reason}).\nAdd the bot as <b>Admin</b> in the channel and re-run /setpublic here or restart.")

    # VIP cadence - only if bot has rights to post
    if VIP_CHAT_ID:
        ok, reason = await _can_post_to_chat(app.bot, VIP_CHAT_ID)
        if ok:
            _sched_repeating("vip_hatching", 2 * 60, VIP_CHAT_ID, "hatching")
            _sched_repeating("vip_cooking", 60, VIP_CHAT_ID, "cooking") # User request: 60s
            _sched_repeating("vip_top", 20 * 60, VIP_CHAT_ID, "top")
            # Continuous fresh cadence every 60 seconds
            _sched_repeating("vip_fresh", 60, VIP_CHAT_ID, "fresh")
        else:
            log.error(f"VIP_CHAT_ID={VIP_CHAT_ID} is not writable: {reason}. Auto-pushes not scheduled.")
            await _notify_owner(app.bot, f"<b>Setup required:</b> Bot lacks post rights for VIP chat <code>{VIP_CHAT_ID}</code> ({reason}).\nAdd the bot as <b>Admin</b> in the channel and re-run /setvip here or restart.")

    # Weekly maintenance: Sunday 03:30 UTC — VACUUM + WAL truncate + log cleanup
    try:
        jq.run_daily(weekly_maintenance_job, time=dtime(3, 30, tzinfo=timezone.utc), days=(6,), name="WeeklyMaintenance")
        log.info("Scheduled weekly maintenance job (Sun 03:30 UTC).")
    except Exception as e:
        log.warning(f"Failed to schedule weekly maintenance: {e}")

async def pre_shutdown(app: Application) -> None: 
    """Gracefully cancel all running background tasks before shutdown."""
    log.info("Initiating graceful shutdown. Canceling background tasks...")
    tasks = [t for t in asyncio.all_tasks() if not t.done()]
    if not tasks:
        return
    log.info(f"Canceling {len(tasks)} background tasks...")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("All background tasks canceled. Shutdown complete.")
    # Close shared HTTP client
    try:
        global _HTTP_CLIENT, _HTTP_CLIENT_DS
        if _HTTP_CLIENT is not None:
            await _HTTP_CLIENT.aclose()
            _HTTP_CLIENT = None
        if _HTTP_CLIENT_DS is not None:
            await _HTTP_CLIENT_DS.aclose()
            _HTTP_CLIENT_DS = None
    except Exception as e:
        log.debug(f"HTTP client close error: {e}")

async def shutdown_handler(app: Application):
    """Enhanced shutdown handler with proper cleanup."""
    log.info("🛑 Token Tony shutting down...")
    try:
        # Cancel all background tasks
        tasks = [t for t in asyncio.all_tasks() if not t.done()]
        if tasks:
            log.info(f"Canceling {len(tasks)} background tasks...")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Close HTTP clients
        if hasattr(app, '_http_clients'):
            for client in app._http_clients.values():
                try:
                    await client.aclose()
                except Exception as e:
                    log.debug(f"Error closing HTTP client: {e}")
        
        log.info("✅ Shutdown complete")
    except Exception as e:
        log.error(f"Error during shutdown: {e}")