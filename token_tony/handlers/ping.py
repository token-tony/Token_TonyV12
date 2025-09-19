# -*- coding: utf-8 -*-
"""/ping command for Token Tony."""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes


async def ping(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None: 
    """Sends a pong message when the /ping command is issued."""
    await u.message.reply_text('Pong!')
