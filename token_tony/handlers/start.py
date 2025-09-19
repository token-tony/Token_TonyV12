# -*- coding: utf-8 -*-
"""/start command for Token Tony."""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes


async def start(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None: 
    """Sends a welcome message when the /start command is issued."""
    await u.message.reply_text('Hi! I am Token Tony. Send me a command to get started.')
