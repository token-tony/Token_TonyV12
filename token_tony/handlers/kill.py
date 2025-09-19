# -*- coding: utf-8 -*-
"""/kill command for Token Tony."""
from __future__ import annotations

import asyncio
import logging

from telegram import Update
from telegram.ext import ContextTypes

from token_tony.utils.telegram import safe_reply_text

log = logging.getLogger(__name__)


async def kill(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await safe_reply_text(u, "Tony's punchin' out. Shutting down...")
    log.info(f"Shutdown command received from owner {u.effective_user.id}.")
    # Use a short delayed hard-exit for cross-platform reliability (Windows-safe)
    async def _delayed_exit():
        try:
            c.application.stop()
        except Exception as e:
            log.debug(f"Shutdown stop() error: {e}")
    try:
        asyncio.create_task(_delayed_exit())
    except Exception as e:
        log.debug(f"Shutdown scheduling error: {e}")
