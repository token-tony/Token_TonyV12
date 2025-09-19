# -*- coding: utf-8 -*-
"""Scheduler for Token Tony."""
from __future__ import annotations

import logging
import random
import time
from datetime import time as dtime, timezone

from telegram.ext import ContextTypes, Application

from config import CONFIG, PUBLIC_CHAT_ID, VIP_CHAT_ID
from token_tony.db_core import get_push_message_id, set_push_message_id, get_recently_served_mints, _execute_db
from token_tony.reports import build_segment_message, pick_header_label
from token_tony.utils.telegram import _can_post_to_chat, _notify_owner, OUTBOX
from token_tony.analysis import LITE_MODE_UNTIL, load_latest_snapshot

log = logging.getLogger(__name__)


async def _prepare_segment_text_from_cache(segment: str) -> Tuple[Optional[str], List[str]]:
    """Builds the segment text without triggering live HTTP calls.
    Returns (text, minted_ids_served). Adds 'Lite Mode' when cache is stale or circuit breaker is active.
    """
    cooldown_hours = int(CONFIG.get("PUSH_COOLDOWN_HOURS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours) 
    items = await _select_items_for_segment(segment, cooldown)
    if not items:
        # Provide a compact nothing-found message per segment
        empty_lines = {
            'fresh': "– Reservoir’s dry, Tony. No top-tier fresh signals right now. ⏱️",
            'hatching': "🦉 Token's nest is empty. No brand-new, structurally sound tokens right now.",
            'cooking': "🍳 Stove's cold. Nothing showing significant momentum right now.",
            'top': "– Nothin' but crickets. The pot's a bit thin right now, check back later. 🦗",
        }
        return empty_lines.get(segment, "Nothing to show right now."), []

    # Determine Lite Mode: if circuit breaker tripped OR snapshots stale
    lite_mode = False
    try:
        if LITE_MODE_UNTIL and LITE_MODE_UNTIL > time.time():
            lite_mode = True
        else:
            snaps = await asyncio.gather(*[load_latest_snapshot(i.get('mint')) for i in items], return_exceptions=True)
            staleness = int(CONFIG.get("SNAPSHOT_STALENESS_SECONDS", 600) or 600)
            for s in snaps:
                if isinstance(s, dict):
                    if (s.get('snapshot_age_sec') or 1e9) > staleness:
                        lite_mode = True
                        break
                else:
                    # No snapshot available => treat as lite
                    lite_mode = True
                    break
    except Exception:
        pass

    header = pick_header_label(f"/{segment}")
    if lite_mode:
        header = f"{header} — ⚡ Lite Mode"
    limit = int(CONFIG.get(f"{segment.upper()}_COMMAND_LIMIT", 2) or 2)
    final = build_segment_message(segment, items[:limit], lite_mode=lite_mode)
    served = [i.get('mint') for i in items[:limit] if i.get('mint')]
    return final, served

async def push_segment_to_chat(app: Application, chat_id: int, segment: str) -> None:
    """Edit the existing segment message in a chat or send a new one if missing."""
    try:
        text, served = await _prepare_segment_text_from_cache(segment)
        if not text:
            return
        mid = await get_push_message_id(chat_id, segment)
        # Try to edit first
        if mid:
            try:
                # Apply basic gating to avoid pool bursts before editing
                try:
                    await OUTBOX.global_bucket.acquire(1)
                    if int(chat_id) < 0:
                        await (await OUTBOX._group_bucket(int(chat_id))).acquire(1)
                    await (await OUTBOX._chat_bucket(int(chat_id))).acquire(1)
                except Exception:
                    pass
                await app.bot.edit_message_text(chat_id=chat_id, message_id=mid, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e:
                msg = str(e)
                if "message to edit not found" in msg.lower() or "message_id" in msg.lower():
                    mid = None  # fall through to send new
                elif "message is not modified" in msg.lower():
                    pass
                else:
                    # Unexpected edit error — try sending a fresh message
                    mid = None
        if not mid:
            sent = await app.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            try:
                await set_push_message_id(chat_id, segment, sent.message_id)
            except Exception:
                log.debug("Failed to persist push message id for chat %s segment %s", chat_id, segment)
    except Exception as e:
        log.error(f"Error pushing segment {segment} to chat {chat_id}: {e}")




# Global push tracking to prevent duplicates
ACTIVE_PUSHES = set()
PUSH_FAILURES = {}

async def scheduled_push_job(context: ContextTypes.DEFAULT_TYPE):
    """Rock-solid push job with Tony's reliability standards."""
    data = context.job.data or {}
    seg = data.get('segment')
    chat_id = data.get('chat_id')
    
    if not seg or not chat_id:
        log.warning(f"🚨 Push job missing critical data: segment={seg}, chat_id={chat_id}")
        return
    
    job_key = f"push_{chat_id}_{seg}"
    
    # Prevent duplicate pushes - Tony doesn't repeat himself
    if job_key in ACTIVE_PUSHES:
        log.info(f"🛡️ Tony's already pushing {seg} to {chat_id} - skipping duplicate")
        return
    
    try:
        ACTIVE_PUSHES.add(job_key)
        
        # Check for recent failures and implement backoff
        failure_key = f"{chat_id}_{seg}"
        if failure_key in PUSH_FAILURES:
            last_failure, count = PUSH_FAILURES[failure_key]
            backoff_time = min(300, 30 * (2 ** count))  # Max 5min backoff
            if time.time() - last_failure < backoff_time:
                log.info(f"⏳ Tony's backing off {seg} push to {chat_id} for {backoff_time}s")
                return
        
        await push_segment_to_chat(context.application, int(chat_id), str(seg))
        
        # Clear failure tracking on success
        if failure_key in PUSH_FAILURES:
            del PUSH_FAILURES[failure_key]
            log.info(f"✅ Tony's back online for {seg} pushes to {chat_id}")
        
    except Exception as e:
        log.error(f"💥 Push job failed for {chat_id}/{seg}: {e}")
        
        # Track failures for intelligent backoff
        failure_key = f"{chat_id}_{seg}"
        if failure_key in PUSH_FAILURES:
            PUSH_FAILURES[failure_key] = (time.time(), PUSH_FAILURES[failure_key][1] + 1)
        else:
            PUSH_FAILURES[failure_key] = (time.time(), 1)
            
    finally:
        ACTIVE_PUSHES.discard(job_key)


async def _schedule_pushes(c: ContextTypes.DEFAULT_TYPE, chat_id: int, chat_type: str):
    """Schedules the push notifications for a given chat."""
    jq = c.application.job_queue
    # Cancel prior jobs
    try:
        for name in (f"{chat_type}_hatching", f"{chat_type}_cooking", f"{chat_type}_top", f"{chat_type}_fresh"):
            for j in jq.get_jobs_by_name(name):
                j.schedule_removal()
    except Exception:
        pass

    # Recreate schedules
    if chat_id:
        prefix = chat_type
        # Standardize both public and vip per your 60s spec
        jq.run_repeating(scheduled_push_job, interval=5 * 60, first=5.0, name=f"{prefix}_hatching", data={"chat_id": chat_id, "segment": "hatching"})
        jq.run_repeating(scheduled_push_job, interval=60, first=7.0, name=f"{prefix}_cooking", data={"chat_id": chat_id, "segment": "cooking"})
        jq.run_repeating(scheduled_push_job, interval=60 * 60, first=9.0, name=f"{prefix}_top", data={"chat_id": chat_id, "segment": "top"})
        jq.run_repeating(scheduled_push_job, interval=60, first=11.0, name=f"{prefix}_fresh", data={"chat_id": chat_id, "segment": "fresh"})

async def weekly_maintenance_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        await _execute_db("PRAGMA wal_checkpoint(TRUNCATE)", commit=True)
    except Exception:
        pass
    try:
        await _execute_db("VACUUM", commit=True)
    except Exception:
        pass
    try:
        removed, kept = _cleanup_logs()
        log.info(f"Weekly maintenance: removed {removed} logs, kept {kept} latest.")
    except Exception:
        pass
