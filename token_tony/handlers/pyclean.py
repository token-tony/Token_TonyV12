# -*- coding: utf-8 -*-
"""/pyclean command for Token Tony."""
from __future__ import annotations

from pathlib import Path

from telegram import Update
from telegram.ext import ContextTypes

from config import OWNER_ID
from token_tony.utils.telegram import safe_reply_text


async def pyclean(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Owner-only: remove all __pycache__ folders under the working directory."""
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    removed_dirs = 0
    try:
        root = Path.cwd()
        for d in root.rglob("__pycache__"):
            if d.is_dir():
                try:
                    for p in d.rglob("*"):
                        try:
                            p.unlink()
                        except Exception:
                            pass
                    d.rmdir()
                    removed_dirs += 1
                except Exception:
                    pass
        await safe_reply_text(u, f"Removed {removed_dirs} __pycache__ folder(s).")
    except Exception as e:
        await safe_reply_text(u, f"pyclean error: {e}")
