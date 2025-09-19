# -*- coding: utf-8 -*-
"""/cooking command for Token Tony."""
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


async def _get_cooking_reports_command(cooldown: set) -> List[Dict[str, Any]]:
    """Collect cooking candidates with graceful fallbacks for the /cooking command.
    Order of precedence:
    1) Tagged candidates (is_cooking_candidate)
    2) Latest snapshots with high 24h volume (CONFIG['COOKING_FALLBACK_VOLUME_MIN_USD'])
    3) Recent analyzed tokens sorted by in-intel price_change_24h
    """
    # Primary: tagged
    items = await get_reports_by_tag("is_cooking_candidate", CONFIG["COOKING_COMMAND_LIMIT"], cooldown)
    if items:
        return items
    # Secondary: snapshot volume
    exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
    min_vol = float(CONFIG.get('COOKING_FALLBACK_VOLUME_MIN_USD', 200) or 200)
    query = f"""
        WITH latest AS (
            SELECT mint_address, MAX(snapshot_time) AS snapshot_time
            FROM TokenSnapshots
            GROUP BY mint_address
        )
        SELECT TL.intel_json
        FROM TokenLog TL
        JOIN latest L ON L.mint_address = TL.mint_address
        JOIN TokenSnapshots TS ON TS.mint_address = L.mint_address AND TS.snapshot_time = L.snapshot_time
        WHERE TL.status IN ('analyzed','served')
          AND TL.mint_address NOT IN ({exclude_placeholders})
          AND COALESCE(TS.volume_24h_usd, 0) >= ?
        ORDER BY TS.snapshot_time DESC, COALESCE(TS.volume_24h_usd, 0) DESC
        LIMIT ?
    """
    params = (*cooldown, float(min_vol), CONFIG["COOKING_COMMAND_LIMIT"]) if cooldown else (float(min_vol), CONFIG["COOKING_COMMAND_LIMIT"])
    rows = await _execute_db(query, params, fetch='all')
    items = [json.loads(row[0]) for row in rows] if rows else []
    if items:
        return items
    # Tertiary: recent analyzed sorted by in-intel price change
    query2 = f"""
        SELECT intel_json FROM TokenLog
        WHERE status IN ('analyzed','served')
          AND mint_address NOT IN ({exclude_placeholders})
        ORDER BY last_analyzed_time DESC
        LIMIT 50
    """
    params2 = (*cooldown,) if cooldown else ()
    rows2 = await _execute_db(query2, params2, fetch='all')
    if not rows2:
        return []
    pool = [json.loads(r[0]) for r in rows2]
    pool.sort(key=lambda x: float(x.get('price_change_24h') or 0), reverse=True)
    return pool[:CONFIG["COOKING_COMMAND_LIMIT"]]

async def cooking(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    reports = await _get_cooking_reports_command(cooldown)
    if not reports:
        await safe_reply_text(u, "🍳 Stove's cold. Nothing showing significant momentum right now.")
        return
    
    # Cooking header quips (heat/cooking theme)
    header_quips = [
        "🍳 Got a few sizzling right now",
        "🍳 These ones are cooking hot",
        "🍳 Momentum’s rising across this batch",
        "🍳 Tony’s grill has a couple popping",
        "🍳 Here’s a pan full of movers",
        "🍳 These drops are smoking fast",
        "🍳 Couple hot picks — handle with mitts",
        "🍳 Tony says: fire under all of these",
        "🍳 The skillet’s crowded — crackling picks",
        "🍳 Burning quick — keep eyes sharp",
    ]
    refreshed = await _refresh_reports_with_latest(reports, allow_missing=True)
    log.info(f"/cooking pipeline: from_tags={len(reports)} after_refresh={len(refreshed)}")
    reports = _filter_items_for_command(refreshed, '/cooking')
    f"{pick_header_label('/cooking')} — {random.choice(header_quips)}"
    items = reports[:2]
    if not items:
        await safe_reply_text(u, "No eligible cooking tokens after filters.")
        return
    final_text = build_segment_message('cooking', items, lite_mode=False)
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])
