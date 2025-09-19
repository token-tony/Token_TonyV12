# -*- coding: utf-8 -*-
"""Telegram utilities for Token Tony."""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Dict, Optional, Tuple

from telegram import Bot, Update
from telegram.constants import ChatAction, ParseMode

from config import OWNER_ID, API_RATE_LIMITS, CONFIG

log = logging.getLogger(__name__)


class TokenBucket:
    def __init__(self, capacity: int, refill_amount: int, interval_seconds: float) -> None:
        self.capacity = max(1, capacity)
        self.tokens = float(capacity)
        self.refill_amount = float(refill_amount)
        self.interval = float(interval_seconds)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, amount: float = 1.0) -> None:
        amount = float(amount)
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._last)
                if elapsed >= self.interval:
                    # Add whole-interval refills for stability under load
                    intervals = int(elapsed // self.interval)
                    self.tokens = min(self.capacity, self.tokens + intervals * self.refill_amount)
                    self._last = now if intervals > 0 else self._last
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
                # Compute time until next token becomes available
                needed = amount - self.tokens
                rate_per_sec = (self.refill_amount / self.interval) if self.interval > 0 else self.refill_amount
                wait = max(0.01, needed / max(1e-6, rate_per_sec))
            # jitter to avoid thundering herd
            await asyncio.sleep(min(2.0, wait + random.uniform(0, 0.05)))


class Outbox:
    """Manages Telegram message sending with rate limiting and flood control."""

    def __init__(self) -> None:
        self.global_bucket = TokenBucket(capacity=30, refill_amount=30, interval_seconds=1.0)  # 30 msg/sec global
        self._chat_buckets: Dict[int, TokenBucket] = {}
        self._group_buckets: Dict[int, TokenBucket] = {}
        self._lock = asyncio.Lock()

    async def _chat_bucket(self, chat_id: int) -> TokenBucket:
        async with self._lock:
            if chat_id not in self._chat_buckets:
                # 20 msg/min per chat
                self._chat_buckets[chat_id] = TokenBucket(capacity=20, refill_amount=20, interval_seconds=60.0)
            return self._chat_buckets[chat_id]

    async def _group_bucket(self, chat_id: int) -> TokenBucket:
        async with self._lock:
            if chat_id not in self._group_buckets:
                # 20 msg/min per group
                self._group_buckets[chat_id] = TokenBucket(capacity=20, refill_amount=20, interval_seconds=60.0)
            return self._group_buckets[chat_id]

    async def send_text(
        self, bot: Bot, chat_id: int, text: str, is_group: bool = False, **kwargs: Any
    ) -> Optional[Any]:
        await self.global_bucket.acquire(1)
        if is_group:
            await (await self._group_bucket(chat_id)).acquire(1)
        else:
            await (await self._chat_bucket(chat_id)).acquire(1)
        try:
            return await bot.send_message(chat_id=chat_id, text=text, **kwargs)
        except Exception as e:
            log.warning(f"Failed to send message to {chat_id}: {e}")
            return None

    async def send_photo(
        self, bot: Bot, chat_id: int, photo: bytes, is_group: bool = False, **kwargs: Any
    ) -> Optional[Any]:
        await self.global_bucket.acquire(1)
        if is_group:
            await (await self._group_bucket(chat_id)).acquire(1)
        else:
            await (await self._chat_bucket(chat_id)).acquire(1)
        try:
            return await bot.send_photo(chat_id=chat_id, photo=photo, **kwargs)
        except Exception as e:
            log.warning(f"Failed to send photo to {chat_id}: {e}")
            return None

OUTBOX = Outbox()


async def _notify_owner(bot: Bot, message: str) -> None:
    if OWNER_ID:
        try:
            await OUTBOX.send_text(bot, OWNER_ID, message, parse_mode=ParseMode.HTML)
        except Exception as e:
            log.error(f"Failed to notify owner: {e}")


async def safe_reply_text(update: Update, text: str, **kwargs: Any) -> Optional[Any]:
    """Safely replies to a message, handling potential errors."""
    if not update.effective_chat:
        return None
    try:
        is_group = update.effective_chat.type in ("group", "supergroup")
        return await OUTBOX.send_text(update.effective_chat.bot, update.effective_chat.id, text, is_group=is_group, **kwargs)
    except Exception as e:
        log.warning(f"Failed to reply to message in chat {update.effective_chat.id}: {e}")
        return None


async def _maybe_send_typing(update: Update) -> None:
    if update.effective_chat:
        try:
            await update.effective_chat.send_action(ChatAction.TYPING)
        except Exception:
            pass


async def _safe_is_group(update: Update) -> bool:
    if not update.effective_chat:
        return False
    return update.effective_chat.type in ("group", "supergroup")


async def safe_reply_photo(update: Update, photo: bytes, **kwargs: Any) -> Optional[Any]:
    """Safely replies with a photo, handling potential errors."""
    if not update.effective_chat:
        return None
    try:
        is_group = update.effective_chat.type in ("group", "supergroup")
        return await OUTBOX.send_photo(update.effective_chat.bot, update.effective_chat.id, photo, is_group=is_group, **kwargs)
    except Exception as e:
        log.warning(f"Failed to send photo in chat {update.effective_chat.id}: {e}")
        return None


async def _can_post_to_chat(bot, chat_id: int) -> tuple[bool, str]:
    """Check if the bot can post to the given chat (channel/group).
    Returns (ok, reason). ok=True when bot is admin (channels) or member with send rights (groups).
    """
    try:
        me = await bot.get_me()
        my_id = getattr(me, 'id', None)
        if not my_id:
            return False, "get_me returned no id"
    except Exception as e:
        return False, f"get_me failed: {e}"
    try:
        chat = await bot.get_chat(chat_id)
    except Exception as e:
        return False, f"get_chat failed: {e}"
    try:
        m = await bot.get_chat_member(chat_id, my_id)
        status = getattr(m, 'status', '')
        chat_type = getattr(chat, 'type', '') or ''
        is_channel = (chat_type == 'channel')
        if status in ("administrator", "creator"):
            # Admin of channel/group: check explicit permissions when available
            can_post = True
            # For channels, ensure can_post_messages if attribute exists
            if is_channel:
                can_post = bool(getattr(m, 'can_post_messages', True))
            # For groups, ensure can_send_messages if attribute exists (PTB uses ChatMemberAdministrator without this flag sometimes)
            if not is_channel:
                can_post = bool(getattr(m, 'can_send_messages', True))
            if can_post:
                return True, "ok"
            return False, f"admin but posting disabled (type={chat_type})"
        # Non-admin path: allow member in groups/supergroups if can_send_messages
        if not is_channel and status in ("member", "restricted"):
            can_send = getattr(m, 'can_send_messages', None)
            if can_send is None:
                # Assume allowed when flag missing
                return True, "ok"
            if bool(can_send):
                return True, "ok"
            return False, "member but cannot send messages"
        # For channels, non-admin cannot post
        return False, f"insufficient rights (type={chat_type}, status={status})"
    except Exception as e:
        return False, f"get_chat_member failed: {e}"
