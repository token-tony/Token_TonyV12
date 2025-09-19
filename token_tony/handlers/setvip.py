# -*- coding: utf-8 -*-
"""/setvip command for Token Tony."""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes

from config import OWNER_ID
from token_tony.utils.telegram import safe_reply_text
from token_tony.workers.scheduler import _schedule_pushes


async def setvip(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Set the current chat as VIP_CHAT_ID and schedule auto-pushes."""
    is_channel = (getattr(getattr(u, 'effective_chat', None), 'type', '') or '').lower() == 'channel'
    if not is_channel and (not getattr(u, 'effective_user', None) or getattr(u.effective_user, 'id', None) != OWNER_ID):
        return await safe_reply_text(u, "Only the boss can do that.")
    global VIP_CHAT_ID
    chat = u.effective_chat
    if not chat:
        return await safe_reply_text(u, "Can't detect chat.")
    VIP_CHAT_ID = int(chat.id)
    await _schedule_pushes(c, VIP_CHAT_ID, "vip")
    return await safe_reply_text(u, f"VIP auto-pushes scheduled for chat {VIP_CHAT_ID}.")
