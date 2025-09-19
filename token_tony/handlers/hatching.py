# -*- coding: utf-8 -*-
"""/hatching command for Token Tony."""
from __future__ import annotations

import logging
import random

from telegram import Update, ReplyKeyboardRemove
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from config import CONFIG
from token_tony.analysis import _refresh_reports_with_latest, _filter_items_for_command
from token_tony.db_core import get_recently_served_mints, get_reports_by_tag, mark_as_served, _execute_db
from token_tony.reports import build_segment_message, pick_header_label
from token_tony.utils.telegram import safe_reply_text, _maybe_send_typing

log = logging.getLogger(__name__)


async def hatching(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    reports = await get_reports_by_tag(
        "is_hatching_candidate",
        CONFIG["HATCHING_COMMAND_LIMIT"],
        cooldown,
        min_score=CONFIG.get("HATCHING_MIN_SCORE_TO_SHOW", 0)
    )
    if not reports:
        # Last resort: query very young analyzed tokens directly (even if tags weren't set due to earlier failures)
        log.warning("/hatching: Tag search found nothing. Activating Last Resort (age-based scan).")
        exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
        age_limit = int(CONFIG.get('HATCHING_MAX_AGE_MINUTES', 30))
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND (age_minutes IS NULL OR age_minutes <= {age_limit})
            AND final_score >= {CONFIG.get('HATCHING_MIN_SCORE_TO_SHOW', 0)}
            AND mint_address NOT IN ({exclude_placeholders})
            ORDER BY last_analyzed_time DESC
            LIMIT ?
        """
        params = (*cooldown, CONFIG["HATCHING_COMMAND_LIMIT"])
        rows = await _execute_db(query, params, fetch='all')
        if rows:
            reports = [json.loads(row[0]) for row in rows]
        if not reports:
            await safe_reply_text(u, "🦉 Token's nest is empty. No brand-new, structurally sound tokens right now.")
            return
        
    # Hatching header quips (newborn/hatch theme)
    header_quips = [
        "🐣 Got a few newborns — just cracked open",
        "🐣 Fresh hatches straight from the nest",
        "🐣 Brand-new drops Tony just spotted",
        "🐣 Token and I pulled these off the line",
        "🐣 Hot from launch — here’s the hatch batch",
        "🐣 New coins in the wild — eyes on ‘em",
        "🐣 Nest is busy — fresh cracks today",
        "🐣 A handful of hatchlings for you",
        "🐣 Straight out the shell — fresh batch",
        "🐣 Don’t blink — Tony’s got hatchers",
    ]
    refreshed = await _refresh_reports_with_latest(reports, allow_missing=True)
    log.info(f"/hatching pipeline: from_tags={len(reports)} after_refresh={len(refreshed)}")
    reports = _filter_items_for_command(refreshed, '/hatching')
    f"{pick_header_label('/hatching')} — {random.choice(header_quips)}"
    items = reports[:2]
    if not items:
        await safe_reply_text(u, "No hatchlings with tradable liquidity yet.")
        return
    final_text = build_segment_message('hatching', items, lite_mode=False)
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])
