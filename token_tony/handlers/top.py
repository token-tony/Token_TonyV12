# -*- coding: utf-8 -*-
"""/top command for Token Tony."""
from __future__ import annotations

import logging
import random

from telegram import Update, ReplyKeyboardRemove
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from config import CONFIG
from token_tony.analysis import _refresh_reports_with_latest, _filter_items_for_command
from token_tony.db_core import get_recently_served_mints, mark_as_served, _execute_db
from token_tony.reports import build_segment_message, pick_header_label
from token_tony.utils.telegram import safe_reply_text, _maybe_send_typing

log = logging.getLogger(__name__)


async def top(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    
    exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
    query = f"""
        SELECT intel_json FROM TokenLog
        WHERE status IN ('analyzed','served')
        AND mint_address NOT IN ({exclude_placeholders})
        AND final_score >= {CONFIG['MIN_SCORE_TO_SHOW']}
        ORDER BY final_score DESC
        LIMIT ?
    """
    params = (*cooldown, CONFIG["TOP_COMMAND_LIMIT"])
    rows = await _execute_db(query, params, fetch='all')
    
    if not rows:
        await safe_reply_text(u, "– Nothin' but crickets. The pot's a bit thin right now, check back later. 🦗")
        return

    # Pull a bit more than we will display to allow post-refresh filtering/sorting
    more_params = (*cooldown, max(CONFIG["TOP_COMMAND_LIMIT"] * 5, CONFIG["TOP_COMMAND_LIMIT"]))
    rows_more = await _execute_db(query, more_params, fetch='all')
    reports = [json.loads(row[0]) for row in (rows_more or rows)]
    # Top header quips (leaderboard theme)
    top_quips = [
        "🏆 Tony’s proud picks — strongest of the bunch",
        "🏆 Here’s today’s winners’ circle",
        "🏆 Top shelf coins — only the best made it",
        "🏆 These few passed every test",
        "🏆 Tony’s shortlist — solid crew",
        "🏆 Couple standouts worth your time",
        "🏆 These are the cream of the crop",
        "🏆 Tony and Token hand-picked these",
        "🏆 Best of today — no slackers",
        "🏆 Tony says: these are built to last",
    ]
    f"{pick_header_label('/top')} — {random.choice(top_quips)}"
    refreshed = await _refresh_reports_with_latest(reports)
    log.info(f"/top pipeline: from_db={len(reports)} after_refresh={len(refreshed)}")
    reports = refreshed
    # Filter out obviously rugged/non-tradable and illiquid
    min_liq = float(CONFIG.get("MIN_LIQUIDITY_FOR_HATCHING", 100) or 100)
    filtered = []
    for j in reports:
        liq_raw = j.get("liquidity_usd", None)
        liq = None
        try:
            if liq_raw is not None:
                liq = float(liq_raw)
        except Exception:
            liq = None
        rug_txt = str(j.get("rugcheck_score") or "")
        # Enforce min liquidity only when we have a numeric value; unknown liquidity passes this check
        if liq is not None and liq < min_liq:
            continue
        if "High Risk" in rug_txt:
            continue
        filtered.append(j)
    # Filter out low scores (no 'DANGER' in /top)
    filtered = [j for j in filtered if int(j.get('score', 0) or 0) >= 40]
    # Apply global no-zero-liq rule for lists
    filtered = _filter_items_for_command(filtered, '/top')
    # Sort by freshly recomputed score, highest first
    filtered.sort(key=lambda x: int(x.get("score", 0) or 0), reverse=True)
    items = filtered[:CONFIG["TOP_COMMAND_LIMIT"]]
    if not items:
        await safe_reply_text(u, "No eligible top tokens after filters.")
        return
    final_text = build_segment_message('top', items, lite_mode=False)
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])
