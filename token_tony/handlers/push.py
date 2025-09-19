# -*- coding: utf-8 -*-
"""/push command for Token Tony."""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes

from config import OWNER_ID, PUBLIC_CHAT_ID, VIP_CHAT_ID
from token_tony.utils.telegram import safe_reply_text
from token_tony.workers.scheduler import push_segment_to_chat


async def push(u: Update, c: ContextTypes.DEFAULT_TYPE):
    is_channel = (getattr(getattr(u, 'effective_chat', None), 'type', '') or '').lower() == 'channel'
    if not is_channel and (not getattr(u, 'effective_user', None) or getattr(u.effective_user, 'id', None) != OWNER_ID):
        return await safe_reply_text(u, "Only the boss can do that.")
    text = (u.message.text or "").strip()
    parts = text.split()
    # Expect: /push <segment> [public|vip]
    if len(parts) < 2:
        return await safe_reply_text(u, "Usage: /push <hatching|cooking|top|fresh> [public|vip]")
    segment = parts[1].lower()
    if segment not in {"hatching", "cooking", "top", "fresh"}:
        return await safe_reply_text(u, "Segment must be one of: hatching, cooking, top, fresh")
    dest = parts[2].lower() if len(parts) >= 3 else None
    if dest == "public":
        chat_id = PUBLIC_CHAT_ID
    elif dest == "vip":
        chat_id = VIP_CHAT_ID
    else:
        chat_id = u.effective_chat.id
    if not chat_id:
        return await safe_reply_text(u, "Missing target chat ID. Set PUBLIC_CHAT_ID / VIP_CHAT_ID in env, or run in target chat.")
    await push_segment_to_chat(c.application, int(chat_id), segment)
    await safe_reply_text(u, f"Pushed {segment} to {('public' if chat_id==PUBLIC_CHAT_ID else 'vip' if chat_id==VIP_CHAT_ID else chat_id)}")
