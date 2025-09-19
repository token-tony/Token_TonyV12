# -*- coding: utf-8 -*-
"""/dbclean command for Token Tony."""
from __future__ import annotations

import random

from telegram import Update
from telegram.ext import ContextTypes

from config import CONFIG, OWNER_ID
from token_tony.db_core import _db_prune, _execute_db
from token_tony.reports import wrap_with_segment_header
from token_tony.utils.telegram import safe_reply_text


async def dbclean(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    quips = [
        "🧹 Tony swept the floor — cleanup done",
        "🧹 Database clear — junk’s gone",
        "🧹 Garage tidy again",
        "🧹 Old scraps tossed",
        "🧹 Tony likes a clean shop",
        "🧹 Prune finished — DB fresh",
        "🧹 Nothing left but the good stuff",
        "🧹 Workshop spotless",
        "🧹 Clutter cleared",
        "🧹 Tony says: floor’s clean, back to work",
    ]
    await safe_reply_text(u, wrap_with_segment_header('dbclean', random.choice(quips)))
    days_snap = int(CONFIG.get("SNAPSHOT_RETENTION_DAYS", 14))
    days_rej = int(CONFIG.get("REJECTED_RETENTION_DAYS", 7))
    ok = await _db_prune(days_snap, days_rej)
    try:
        await _execute_db("PRAGMA wal_checkpoint(TRUNCATE)", commit=True)
    except Exception:
        pass
    await safe_reply_text(u, wrap_with_segment_header('dbclean', "DB cleaned." if ok else "DB clean encountered an error."))
