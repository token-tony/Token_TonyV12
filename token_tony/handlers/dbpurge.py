# -*- coding: utf-8 -*-
"""/dbpurge command for Token Tony."""
from __future__ import annotations

from datetime import datetime, timezone
from telegram import Update
from telegram.ext import ContextTypes

from config import OWNER_ID
from token_tony.db_core import _db_purge_all, _execute_db
from token_tony.utils.telegram import safe_reply_text


async def dbpurge(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    text = (u.message.text or "").strip()
    if not text.lower().endswith("confirm"):
        return await safe_reply_text(u, "This erases all state. Run /dbpurge confirm to proceed.")
    await safe_reply_text(u, "Purging all state and vacuuming DB...")
    await _db_purge_all()
    await safe_reply_text(u, "All state wiped. Fresh start.")
    await _execute_db("INSERT OR REPLACE INTO KeyValueStore (key, value) VALUES (?, ?)", ('last_purge_time', datetime.now(timezone.utc).isoformat()), commit=True)
