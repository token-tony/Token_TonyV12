# -*- coding: utf-8 -*-
"""/dbprune command for Token Tony."""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes

from config import CONFIG, OWNER_ID
from token_tony.db_core import _db_prune, _execute_db
from token_tony.utils.telegram import safe_reply_text


async def dbprune(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    days_snap = int(CONFIG.get("SNAPSHOT_RETENTION_DAYS", 14))
    days_rej = int(CONFIG.get("REJECTED_RETENTION_DAYS", 7))
    await safe_reply_text(u, f"Pruning snapshots >{days_snap}d and rejected >{days_rej}d...")
    ok = await _db_prune(days_snap, days_rej)
    try:
        await _execute_db("PRAGMA wal_checkpoint(TRUNCATE)", commit=True)
    except Exception:
        pass
    await safe_reply_text(u, "DB prune complete." if ok else "DB prune encountered an error.")
