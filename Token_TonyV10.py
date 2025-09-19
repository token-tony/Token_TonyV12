
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Token Tony - v23.0 "The Alpha Refactor"
# Modular, clean, and ready for the next evolution.

import os
import asyncio
import logging
import sys
import re
from typing import Any, Dict
from pathlib import Path

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    filters,
    MessageHandler,
)
from telegram.request import HTTPXRequest
from config import (
    CONFIG,
    OWNER_ID,
    TELEGRAM_TOKEN,
)
from token_tony.handlers import (
    check,
    cooking,
    dbclean,
    dbprune,
    dbpurge,
    diag,
    fresh,
    hatching,
    kill,
    logclean,
    ping,
    push,
    pyclean,
    seed,
    set_config,
    setpublic,
    setvip,
    start,
    testpush,
    top,
)
from token_tony.workers import (
    post_init,
    pre_shutdown,
)

# --- Logging ---
# Default log path moved into 'data/' to keep project root clean; override with TONY_LOG_FILE
LOG_FILE = os.getenv("TONY_LOG_FILE", "data/tony_log.log")
try:
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
except Exception:
    pass

try:
    from logging.handlers import TimedRotatingFileHandler
    handlers = [TimedRotatingFileHandler(LOG_FILE, when='midnight', backupCount=7, encoding="utf-8"), logging.StreamHandler()]
except Exception:
    handlers = [logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()]
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s", handlers=handlers)
log = logging.getLogger("token_tony")
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.INFO)

# ---------------- Configuration Sanity ----------------
CONFIG_SANITY: Dict[str, Any] = {}

def _path_writable(p: str) -> bool:
    try:
        d = Path(p).parent
        d.mkdir(parents=True, exist_ok=True)
        test = d / ".tt_write_test.tmp"
        test.write_text("ok", encoding="utf-8")
        try:
            test.unlink()
        except Exception as e:
            log.warning(f"WAL checkpoint failed: {e}")
        return True
    except Exception as e:
        log.warning(f"Path not writable: {p} ({e})")
        return False



# Allow log level override via .env (LOG_LEVEL)
_lvl = os.getenv("LOG_LEVEL", "INFO").strip().upper()
if hasattr(logging, _lvl):
    log.setLevel(getattr(logging, _lvl))

# Precompiled regex for command routing
CMD_RE = re.compile(r"^/([A-Za-z0-9_]+)(?:@\w+)?(?:\s|$)")


if __name__ == "__main__":
    # Tony's config validation
    from config import validate_config
    issues, warnings = validate_config()
    
    if issues:
        log.critical("💥 FATAL: Configuration issues found:")
        for issue in issues:
            log.critical(f"  - {issue}")
        sys.exit(1)
    
    if warnings:
        log.warning("⚠️ Configuration warnings:")
        for warning in warnings:
            log.warning(f"  - {warning}")
    
    if not TELEGRAM_TOKEN:
        log.critical("💥 FATAL: TELEGRAM_TOKEN not set - Tony can't work without it")
        sys.exit(1)

    log.info("🚀 Token_Tony 'The Alpha Dad Guardian' is starting up...")
    
    # Log Tony's configuration
    if CONFIG.get('STARTUP_CONFIG_LOG', True):
        log_startup_config()
    
    # Build Tony's application with enhanced settings
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        # Configure getUpdates timeouts per PTB >=20.6 recommendations
        .get_updates_connect_timeout(float(CONFIG.get("TELEGRAM_CONNECT_TIMEOUT", 20.0) or 20.0))
        .get_updates_read_timeout(float(CONFIG.get("TELEGRAM_READ_TIMEOUT", 30.0) or 30.0))
        .get_updates_pool_timeout(float(CONFIG.get("TELEGRAM_POOL_TIMEOUT", 60.0) or 60.0))
        .request(
            HTTPXRequest(
                connection_pool_size=int(CONFIG.get("TELEGRAM_POOL_SIZE", 80) or 80),
                pool_timeout=float(CONFIG.get("TELEGRAM_POOL_TIMEOUT", 60.0) or 60.0),
                connect_timeout=float(CONFIG.get("TELEGRAM_CONNECT_TIMEOUT", 20.0) or 20.0),
                read_timeout=float(CONFIG.get("TELEGRAM_READ_TIMEOUT", 30.0) or 30.0),
                write_timeout=float(CONFIG.get("TELEGRAM_READ_TIMEOUT", 30.0) or 30.0),
            )
        )
        .build()
    )
    
    # Tony's command handlers - comprehensive coverage
    handlers = [CommandHandler(cmd, func) for cmd, func in [
        ("start", start),
        ("ping", ping),
        ("diag", diag),
        ("fresh", fresh),
        ("hatching", hatching),
        ("cooking", cooking),
        ("top", top),
        ("check", check),
        ("dbprune", dbprune),
        ("dbpurge", dbpurge),
        ("dbclean", dbclean),
    ]]
    
    # Owner-only commands - Tony's admin tools
    owner_commands = [
        ("kill", kill),
        ("seed", seed),
        ("set", set_config),
        ("setpublic", setpublic),
        ("setvip", setvip),
        ("push", push),
        ("testpush", testpush),
        ("logclean", logclean),
        ("pyclean", pyclean),
    ]
    
    for cmd, func in owner_commands:
        handlers.append(CommandHandler(cmd, func, filters=filters.User(user_id=OWNER_ID)))
    
    app.add_handlers(handlers)

    # Tony's channel command routing - he works everywhere
    async def _route_channel_commands(u: Update, c: ContextTypes.DEFAULT_TYPE):
        try:
            text = (getattr(getattr(u, 'effective_message', None), 'text', '') or '').strip()
            if not text.startswith('/'):
                return
                
            # Extract command and strip bot username
            m = CMD_RE.match(text)
            if not m:
                return
            cmd = m.group(1).lower()
            
            # Tony's command mapping
            command_map = {
                'start': start, 'ping': ping, 'diag': diag,
                'fresh': fresh, 'hatching': hatching, 'cooking': cooking,
                'top': top, 'check': check,
                'setpublic': setpublic, 'setvip': setvip,
                'push': push, 'testpush': testpush,
            }
            
            func = command_map.get(cmd)
            if func:
                await func(u, c)
        except Exception as e:
            log.error(f"💥 Channel command error: {e}")
            try:
                await safe_reply_text(u, f"💥 Command error: {e}")
            except Exception as e2:
                log.debug(f"Failed to send error reply: {e2}")

    app.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.COMMAND, _route_channel_commands))

    # Tony's lifecycle hooks
    app.post_init = post_init
    app.pre_shutdown = pre_shutdown
    
    # Tony's polling configuration - optimized for reliability
    try:
        app.run_polling(
            drop_pending_updates=True,
            poll_interval=0.5,
            bootstrap_retries=3,
        )
    except KeyboardInterrupt:
        log.info("🛑 Tony received shutdown signal")
    except Exception as e:
        log.error(f"💥 Tony's polling failed: {e}")
        sys.exit(1)
