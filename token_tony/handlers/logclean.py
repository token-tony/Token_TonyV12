# -*- coding: utf-8 -*-
"""/logclean command for Token Tony."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Tuple

from telegram import Update
from telegram.ext import ContextTypes

from config import OWNER_ID
from token_tony.utils.telegram import safe_reply_text

LOG_FILE = os.getenv("TONY_LOG_FILE", "data/tony_log.log")


async def logclean(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Owner-only: remove old rotated logs beyond the latest 7 files."""
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    try:
        removed, kept = _cleanup_logs()
        await safe_reply_text(u, f"Removed {removed} old log file(s). Kept {kept} latest.")
    except Exception as e:
        await safe_reply_text(u, f"Log cleanup error: {e}")

def _cleanup_logs(keep: Optional[int] = None) -> Tuple[int, int]:
    base = Path(LOG_FILE)
    keep = int(os.getenv("LOG_KEEP_COUNT", str(keep or 7)) or 7)
    rotated = sorted([p for p in base.parent.glob(base.name + ".*") if p.is_file()], key=lambda p: p.stat().st_mtime)
    to_delete = rotated[:-keep] if len(rotated) > keep else []
    removed = 0
    for p in to_delete:
        try:
            p.unlink(missing_ok=True)
            removed += 1
        except Exception:
            pass
    return removed, min(len(rotated), keep)
