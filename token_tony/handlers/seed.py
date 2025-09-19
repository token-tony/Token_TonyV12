# -*- coding: utf-8 -*-
"""/seed command for Token Tony."""
from __future__ import annotations

import asyncio

from telegram import Update
from telegram.ext import ContextTypes

from config import OWNER_ID
from token_tony.analysis import process_discovered_token
from token_tony.utils.telegram import safe_reply_text


async def seed(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Owner-only: seed one or more mints into the discovery queue for testing."""
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    text = (u.message.text or "").strip()
    mints = text.split()[1:]
    for m in mints[:10]:
        asyncio.create_task(process_discovered_token(m))
    await safe_reply_text(u, f"Queued {len(mints[:10])} mint(s) for discovery.")
