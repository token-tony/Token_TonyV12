# -*- coding: utf-8 -*-
"""/testpush command for Token Tony."""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from config import PUBLIC_CHAT_ID, VIP_CHAT_ID
from token_tony.utils.telegram import safe_reply_text, _can_post_to_chat, OUTBOX


async def testpush(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Send a small test message to public/vip/here and return deep link info."""
    bot = u.get_bot()
    text = (u.message.text if getattr(u, 'message', None) else getattr(getattr(u, 'effective_message', None), 'text', '')) or ''
    parts = text.split()
    target = parts[1].lower() if len(parts) > 1 else 'here'
    if target == 'public':
        chat_id = PUBLIC_CHAT_ID
    elif target == 'vip':
        chat_id = VIP_CHAT_ID
    else:
        chat_id = getattr(getattr(u, 'effective_chat', None), 'id', None)
    if not chat_id:
        return await safe_reply_text(u, "No target chat. Usage: /testpush [public|vip|here]")
    # Check rights and username
    ok, reason = await _can_post_to_chat(bot, int(chat_id))
    ch = None
    try:
        ch = await bot.get_chat(int(chat_id))
    except Exception:
        pass
    uname = getattr(ch, 'username', None)
    typ = getattr(ch, 'type', '')
    # Send a tiny test message
    sent = None
    if ok:
        try:
            sent = await OUTBOX.send_text(bot, int(chat_id), "Test push ✅", is_group=(int(chat_id) < 0), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e:
            await safe_reply_text(u, f"Send failed: {e}")
            return
    else:
        await safe_reply_text(u, f"Cannot post to {chat_id}: {reason}")
        return
    mid = int(getattr(sent, 'message_id', 0)) if sent else 0
    link = f"https://t.me/{uname}/{mid}" if uname and mid else "(no public link)"
    await safe_reply_text(u, f"PUSH OK to {chat_id} (type={typ}) mid={mid}\nLink: {link}")
