# -*- coding: utf-8 -*-
"""/setpublic command for Token Tony."""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes

from config import OWNER_ID
from token_tony.utils.telegram import safe_reply_text
from token_tony.workers.scheduler import _schedule_pushes


async def setpublic(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Set the current chat as PUBLIC_CHAT_ID and schedule auto-pushes."""
    is_channel = (getattr(getattr(u, 'effective_chat', None), 'type', '') or '').lower() == 'channel'
    if not is_channel and (not getattr(u, 'effective_user', None) or getattr(u.effective_user, 'id', None) != OWNER_ID):
        return await safe_reply_text(u, "Only the boss can do that.")
    global PUBLIC_CHAT_ID
    chat = u.effective_chat
    if not chat:
        return await safe_reply_text(u, "Can't detect chat.")
    PUBLIC_CHAT_ID = int(chat.id)
    await _schedule_pushes(c, PUBLIC_CHAT_ID, "public")
    return await safe_reply_text(u, f"Public auto-pushes scheduled for chat {PUBLIC_CHAT_ID}.")
