# -*- coding: utf-8 -*-
"""/fresh command for Token Tony."""
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


async def fresh(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    reports = await get_reports_by_tag(
        "is_fresh_candidate",
        CONFIG["FRESH_COMMAND_LIMIT"],
        cooldown,
        min_score=CONFIG.get("FRESH_MIN_SCORE_TO_SHOW", CONFIG['MIN_SCORE_TO_SHOW'])
    )
    
    if not reports:
        log.warning("/fresh: Tag search found nothing. Activating Last Resort (ignoring tags).")
        exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND final_score >= {CONFIG.get('FRESH_MIN_SCORE_TO_SHOW', CONFIG['MIN_SCORE_TO_SHOW'])}
            AND (age_minutes IS NULL OR age_minutes < 1440)
            AND mint_address NOT IN ({exclude_placeholders})
            ORDER BY last_analyzed_time DESC, final_score DESC
            LIMIT ?
        """
        params = (*cooldown, CONFIG["FRESH_COMMAND_LIMIT"])
        rows = await _execute_db(query, params, fetch='all')
        if rows:
            reports = [json.loads(row[0]) for row in rows]

    if not reports:
        await safe_reply_text(u, "– Reservoir’s dry, Tony. No top-tier fresh signals right now. ⏱️")
        return

    # Fresh header quips (general scan/guard/tooling vibe)
    header_quips = [
        "🆕 Here’s a batch of fresh ones Tony approved",
        "🆕 These just passed the safety check",
        "🆕 Fresh off the truck — clean and ready",
        "🆕 Tony signed off on this stack",
        "🆕 Couple solid builds right here",
        "🆕 Passed inspection — no rust yet",
        "🆕 Tony’s fridge picks — crisp and clean",
        "🆕 Pulled a fresh set for you",
        "🆕 New kids on the block — safe enough to sniff",
        "🆕 Tony says: these are worth a look",
    ]
    # Refresh market snapshot and recompute scores just-in-time
    refreshed = await _refresh_reports_with_latest(reports, allow_missing=True)
    log.info(f"/fresh pipeline: from_tags={len(reports)} after_refresh={len(refreshed)}")
    reports = _filter_items_for_command(refreshed, '/fresh')
    f"{pick_header_label('/fresh')} — {random.choice(header_quips)}"
    items = reports[:2]
    if not items:
        await safe_reply_text(u, "No eligible fresh tokens at the moment.")
        return
    # Override with new skeleton formatter
    final_text = build_segment_message('fresh', items, lite_mode=False)
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])
