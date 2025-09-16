#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Token Tony - v23.0 "The Alpha Refactor"
# Modular, clean, and ready for the next evolution.

import os
import asyncio
import logging
import sys
import json
import random
import websockets
import time
import statistics
import re
from datetime import datetime, timezone, time as dtime
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

import httpx
from telegram import Update, ReplyKeyboardRemove
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (Application, CommandHandler, ContextTypes,
                          filters)
from telegram.request import HTTPXRequest
from config import (ALCHEMY_RPC_URL, ALCHEMY_WS_URL, BIRDEYE_API_KEY, CONFIG,
                    HELIUS_API_KEY, HELIUS_RPC_URL, HELIUS_WS_URL, KNOWN_QUOTE_MINTS,
                    OWNER_ID, PUBLIC_CHAT_ID, SYNDICA_RPC_URL, SYNDICA_WS_URL,
                    TELEGRAM_TOKEN, VIP_CHAT_ID)
from analysis import (POOL_BIRTH_CACHE, enrich_token_intel,
                      _compute_mms, _compute_score, _compute_sss)
try:
    from .api_core import (
        API_HEALTH,
        API_PROVIDERS,
        GECKO_API_URL,
        LITE_MODE_UNTIL,
        _fetch,
        extract_mint_from_check_text,
        fetch_birdeye,
        fetch_dexscreener_by_mint,
        fetch_dexscreener_chart,
        fetch_market_snapshot,
    )
    from .db_core import (
        _execute_db,
        get_push_message_id,
        get_recently_served_mints,
        load_latest_snapshot,
        mark_as_served,
        save_snapshot,
        setup_database,
        set_push_message_id,
        upsert_token_intel,
    )
except ImportError:  # pragma: no cover - script execution fallback
    from api_core import (  # type: ignore
        API_HEALTH,
        API_PROVIDERS,
        GECKO_API_URL,
        LITE_MODE_UNTIL,
        _fetch,
        extract_mint_from_check_text,
        fetch_birdeye,
        fetch_dexscreener_by_mint,
        fetch_dexscreener_chart,
        fetch_market_snapshot,
    )
    from db_core import (  # type: ignore
        _execute_db,
        get_push_message_id,
        get_recently_served_mints,
        load_latest_snapshot,
        mark_as_served,
        save_snapshot,
        setup_database,
        set_push_message_id,
        upsert_token_intel,
    )
from reports import (
    build_full_report2,
    load_advanced_quips,
    build_segment_message,
    wrap_with_segment_header,
)
from utils import (_can_post_to_chat, _notify_owner,
                   is_valid_solana_address,
                   OUTBOX, TokenBucket)
from voice import (
    cycle_voice_preset,
    get_current_voice,
    get_voice_label,
    get_voice_profile,
    list_voice_presets,
    set_voice_preset,
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

def is_degraded_mode() -> bool:
    """Degraded when critical data sources are missing."""
    return not bool(HELIUS_API_KEY) or not bool(BIRDEYE_API_KEY)

def compute_config_sanity() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    out["telegram_token"] = bool(TELEGRAM_TOKEN)
    out["db_writable"] = _path_writable(CONFIG.get("DB_FILE", "data/tony_memory.db"))
    out["log_writable"] = _path_writable(LOG_FILE)
    out["helius_api"] = bool(HELIUS_API_KEY)
    out["birdeye_api"] = bool(BIRDEYE_API_KEY)
    out["public_chat_id"] = int(PUBLIC_CHAT_ID or 0)
    out["vip_chat_id"] = int(VIP_CHAT_ID or 0)
    out["ws_endpoints"] = {
        "helius_ws": bool(HELIUS_WS_URL),
        "alchemy_ws": bool(ALCHEMY_WS_URL),
        "syndica_ws": bool(SYNDICA_WS_URL),
    }
    out["degraded_mode"] = is_degraded_mode()
    return out

# Allow log level override via .env (LOG_LEVEL)
_lvl = os.getenv("LOG_LEVEL", "INFO").strip().upper()
if hasattr(logging, _lvl):
    log.setLevel(getattr(logging, _lvl))

# Precompiled regex for command routing
CMD_RE = re.compile(r"^/([A-Za-z0-9_]+)(?:@\w+)?(?:\s|$)")

# Shared HTTP clients to reduce TLS/connection overhead
_HTTP_CLIENT: Optional[httpx.AsyncClient] = None
_HTTP_CLIENT_DS: Optional[httpx.AsyncClient] = None  # DexScreener prefers HTTP/1.1 in practice
async def get_http_client(*, ds: bool = False) -> httpx.AsyncClient:
    global _HTTP_CLIENT, _HTTP_CLIENT_DS
    if ds:
        if _HTTP_CLIENT_DS is None:
            # Use HTTP/1.1 for DexScreener endpoints to avoid edge-caching oddities
            _HTTP_CLIENT_DS = httpx.AsyncClient(http2=False, timeout=CONFIG["HTTP_TIMEOUT"])  # re-used across tasks
        return _HTTP_CLIENT_DS
    if _HTTP_CLIENT is None:
        _HTTP_CLIENT = httpx.AsyncClient(http2=True, timeout=CONFIG["HTTP_TIMEOUT"])  # re-used across tasks
    return _HTTP_CLIENT


async def get_reports_by_tag(tag: str, limit: int, cooldown: set, min_score: int = 0) -> List[Dict[str, Any]]:
    """Get reports from TokenLog by tag (is_hatching_candidate, is_cooking_candidate, is_fresh_candidate)."""
    exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"

    # Map tag names to column names
    tag_column_map = {
        "is_hatching_candidate": "is_hatching_candidate",
        "is_cooking_candidate": "is_cooking_candidate",
        "is_fresh_candidate": "is_fresh_candidate"
    }

    if tag not in tag_column_map:
        return []

    column = tag_column_map[tag]

    if cooldown:
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND {column} = 1
            AND final_score >= ?
            AND mint_address NOT IN ({exclude_placeholders})
            ORDER BY last_analyzed_time DESC, final_score DESC
            LIMIT ?
        """
        params = (min_score, *cooldown, limit)
    else:
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND {column} = 1
            AND final_score >= ?
            ORDER BY last_analyzed_time DESC, final_score DESC
            LIMIT ?
        """
        params = (min_score, limit)

    rows = await _execute_db(query, params, fetch='all')
    return [json.loads(row[0]) for row in rows] if rows else []

async def _refresh_reports_with_latest(reports: List[Dict[str, Any]], allow_missing: bool = False) -> List[Dict[str, Any]]:
    """Refresh market data for reports and recompute scores."""
    if not reports:
        return []

    refreshed = []
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
        for report in reports:
            mint = report.get("mint")
            if not mint:
                if not allow_missing:
                    continue
                refreshed.append(report)
                continue

            try:
                # Fetch fresh market snapshot
                snapshot = await fetch_market_snapshot(client, mint)
                if snapshot:
                    # Update report with fresh data
                    updated_report = report.copy()
                    updated_report.update({
                        "liquidity_usd": snapshot.get("liquidity_usd"),
                        "volume_24h_usd": snapshot.get("volume_24h_usd"),
                        "market_cap_usd": snapshot.get("market_cap_usd"),
                        "price_change_24h": snapshot.get("price_change_24h"),
                        "price_usd": snapshot.get("price_usd")
                    })

                    # Recompute scores with fresh data
                    sss_score = _compute_sss(updated_report)
                    mms_score = _compute_mms(updated_report)
                    final_score = _compute_score(updated_report, sss_score, mms_score)

                    updated_report.update({
                        "sss_score": sss_score,
                        "mms_score": mms_score,
                        "score": final_score
                    })

                    refreshed.append(updated_report)
                else:
                    # Keep original if refresh failed but allow_missing is True
                    if allow_missing:
                        refreshed.append(report)
            except Exception as e:
                log.warning(f"Failed to refresh report for {mint}: {e}")
                if allow_missing:
                    refreshed.append(report)

    return refreshed

def _filter_items_for_command(items: List[Dict[str, Any]], command: str) -> List[Dict[str, Any]]:
    """Apply global filtering rules for command outputs (e.g., no-zero-liquidity rule)."""
    if not items:
        return items

    filtered = []
    for item in items:
        # Global no-zero-liquidity rule for lists
        liq_raw = item.get("liquidity_usd", None)
        if liq_raw is not None:
            try:
                liq = float(liq_raw)
                if liq <= 0:
                    continue  # Skip zero liquidity items
            except (ValueError, TypeError):
                pass  # Keep items with non-numeric liquidity (treated as unknown)

        # Additional command-specific filters can be added here
        filtered.append(item)

    return filtered

def pick_header_label(command: str | None = None) -> str:
    """Selects a random, flavorful header for a command response."""
    headers = {
        "fresh": ["🪺 Fresh Hatch", "✨ Just Minted", "🔧 New Bolts"],
        "hatching": ["🐣 Nest Cracking", "🪺 New Brood", "🛰️ First Flight"],
        "cooking": ["🍳 Now Cooking", "🔥 Heat Rising", "🥓 Sizzle Check"],
        "top": ["🏆 Top Shelf", "⛰️ Peak View", "👑 Crowned Picks"],
        "check": ["🔎 Deep Scan", "🧪 Lab Read", "🧰 Toolbox Check"],
        "general": [
            "🛡️ Guard Duty", "🧭 Compass Check", "🧰 Toolbox Open",
            "🟢 Green Light", "⚡ Power Check",
        ],
    }
    cmd_to_bucket = {
        "/fresh": "fresh", "/hatching": "hatching", "/cooking": "cooking",
        "/top": "top", "/check": "check",
    }
    bucket = cmd_to_bucket.get(command or "", "general")
    pool = headers.get(bucket, headers["general"])
    return random.choice(pool)

# A mapping of DEX names to their program ID and the base58-encoded discriminator
# for their specific "create new pool" instruction. This is the most efficient way
# to subscribe to only the events we care about.
DEX_PROGRAMS_FOR_FIREHOSE = {
    # Slimmed to 3 Raydium families to reduce Helius usage/cost.
    # v4 (legacy AMM; most common new pools)
    "Raydium_v4": {
        "program_id": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        "discriminator": "3iVvT9Yd6oY"  # initialize2
    },
    # CLMM (concentrated)
    "Raydium_CLMM": {
        "program_id": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
        "discriminator": "3iVvT9Yd6oY"  # initialize2
    },
    # CPMM (new constant-product router)
    "Raydium_CPMM": {
        "program_id": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
        "discriminator": "3iVvT9Yd6oY"  # initialize2
    },
}

# --- Global State ---
FIREHOSE_STATUS: Dict[str, str] = {}
provider_state: Dict[str, Dict[str, Any]] = {}
PUMPFUN_STATUS = "🔴 Disconnected"
# Adaptive processing state
from collections import deque
recent_processing_times = deque(maxlen=50) # Now local to this module
adaptive_batch_size = CONFIG["MIN_BATCH_SIZE"]
DB_MARKER_FILE = "tony_db.marker"
DISCOVERY_BUCKET = TokenBucket(capacity=8, refill_amount=8, interval_seconds=1.0)

# ======================================================================================
# Block 4: Unified Discovery Engine
# ======================================================================================

_seen_mints = deque(maxlen=2000)

def _sanitize_mint(m: Optional[str]) -> Optional[str]:
    """Heuristic cleanup for occasionally malformed mints coming from some sources.
    - Strips common textual suffixes accidentally appended (e.g., 'pump', 'bonk').
    - Returns the cleaned value only if it still looks like a Solana address.
    """
    if not m:
        return m
    s = m.strip()
    # Some sources occasionally append token platform names as suffixes
    for suf in ("pump", "bonk"):
        if s.endswith(suf):
            s2 = s[: -len(suf)]
            if is_valid_solana_address(s2):
                return s2
    return s if is_valid_solana_address(s) else None
CHANNEL_ALERT_SENT: Dict[str, bool] = {} # In-memory cache for this session

async def pumpportal_worker():
    """Single-socket PumpPortal subscriber with reconnect + resubscribe."""
    url = "wss://pumpportal.fun/api/data"
    backoff = 1.0
    while True:
        try:
            log.info("PumpPortal: Connecting...")
            global PUMPFUN_STATUS
            PUMPFUN_STATUS = "🟡 Connecting"
            async with websockets.connect(url, ping_interval=15, ping_timeout=10) as ws:
                backoff = 1.0
                # Subscribe to new tokens + migrations using a single socket
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                await ws.send(json.dumps({"method": "subscribeMigration"}))
                log.info("PumpPortal: Subscribed (new tokens + migration).")
                PUMPFUN_STATUS = "🟢 Connected"
                while True:
                    msg = await ws.recv()
                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue
                    # Accept any payload containing a plausible mint
                    if isinstance(data, dict):
                        cand = data.get("mint") or data.get("token") or data.get("tokenMint")
                        cand = _sanitize_mint(cand) # sanitize_mint already validates
                        if cand:
                            async def _queue():
                                await DISCOVERY_BUCKET.acquire(1)
                                await process_discovered_token(cand)
                            asyncio.create_task(_queue())
        except Exception as e:
            log.warning(f"PumpPortal: Disconnected: {e}. Reconnecting in {backoff:.1f}s...")
            PUMPFUN_STATUS = "🔴 Disconnected"
            await asyncio.sleep(backoff + random.uniform(0, 0.5))
            backoff = min(30.0, backoff * 2)


## Removed legacy Pump.fun client-api worker and Helius programSubscribe variant (unused).

async def _fetch_transaction(c: httpx.AsyncClient, rpc_url: str, signature: str) -> Optional[Dict[str, Any]]:
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
        "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
    }
    res = await _fetch(c, rpc_url, method="POST", json=payload, timeout=10.0)
    return (res or {}).get("result") if res else None

def _extract_mints_from_tx_result(tx_result: Dict[str, Any]) -> List[str]:
    """Best-effort extraction of base/quote mints from a transaction result."""
    mints: set = set()
    meta = tx_result.get("meta") or {}
    for bal in meta.get("postTokenBalances", []) + meta.get("preTokenBalances", []):
        if mint := bal.get("mint"):
            mints.add(mint)
    # Also scan any parsed instruction infos that expose a 'mint'
    try:
        tx = tx_result.get("transaction", {})
        msg = tx.get("message", {})
        for ix in msg.get("instructions", []):
            if (parsed := ix.get("parsed")) and isinstance(parsed, dict):
                info = parsed.get("info", {})
                if mint := info.get("mint"):
                    mints.add(mint)
    except Exception:
        pass
    # Filter out known quote mints and limit number
    filtered = [m for m in mints if m not in KNOWN_QUOTE_MINTS]
    return filtered[:4]

POOL_BIRTH_KEYWORDS = {"createpool", "initializepool", "initialize_pool", "pool-init", "open_pool", "initialize2"}
GO_LIVE_KEYWORDS = {"addliquidity", "increase_liquidity"}
FLOW_KEYWORDS = {"swap"}

async def _logs_subscriber(provider_name: str, ws_url: str, rpc_url: str):
    key = f"Logs-{provider_name}"
    state = provider_state.setdefault(
        provider_name,
        {
            "consecutive_failures": 0,
            "last_success": 0.0,
            "last_failure": 0.0,
            "messages_received": 0,
            "current_backoff": 0.0,
            "last_error": "",
        },
    )
    subscriptions = []
    base_backoff = 10
    while True:
        try:
            FIREHOSE_STATUS[key] = "🟡 Connecting"
            log.info(f"Logs Firehose ({provider_name}): Connecting {ws_url} ...")
            # Helius suggests pings approx every 60s; keep heartbeat under that
            async with websockets.connect(ws_url, ping_interval=55) as websocket:
                # Subscribe per DEX program using logsSubscribe mentions
                for name, d in DEX_PROGRAMS_FOR_FIREHOSE.items():
                    sub = {
                        "jsonrpc": "2.0", "id": random.randint(1000, 999999), "method": "logsSubscribe",
                        # Use mentions array per Solana WS API
                        "params": [{"mentions": [d["program_id"]]}, {"commitment": "processed"}]
                    }
                    await websocket.send(json.dumps(sub))
                    subscriptions.append(sub["id"])
                state["consecutive_failures"] = 0
                state["current_backoff"] = 0.0
                state["last_success"] = time.time()
                state["last_error"] = ""
                FIREHOSE_STATUS[key] = "🟢 Connected"
                log.info(f"✅ Logs Firehose ({provider_name}): Subscribed to {len(DEX_PROGRAMS_FOR_FIREHOSE)} programs.")
                client = await get_http_client()
                while websocket.open:
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=90.0)
                    except asyncio.TimeoutError:
                        log.info(f"Logs Firehose ({provider_name}): idle, connection alive.")
                        continue

                    msg = json.loads(raw)
                    if msg.get("method") != "logsNotification":
                        continue
                    result = msg.get("params", {}).get("result", {})
                    signature = result.get("value", {}).get("signature")
                    if not signature:
                        continue
                    state["messages_received"] += 1
                    state["last_success"] = time.time()
                    if state["messages_received"] % 500 == 0:
                        log.info(
                            "Logs Firehose (%s): processed %s messages.",
                            provider_name,
                            state["messages_received"],
                        )
                    # Check logs text for relevant signals before fetching tx
                    logs_list = (result.get("value", {}).get("logs") or [])
                    logs_text = "\n".join(logs_list).lower()
                    # Dial back: only react to pool birth to reduce Helius load
                    if not any(k in logs_text for k in POOL_BIRTH_KEYWORDS):
                        continue

                    # Rate-limit transaction lookups to reduce RPC spend
                    tx_res = await _fetch_transaction(client, rpc_url, signature)
                    if not tx_res:
                        continue
                    # Optional: ignore very old transactions to avoid backfill floods
                    try:
                        bt = tx_res.get("blockTime")
                        if bt and (time.time() - int(bt)) > 600:
                            continue
                    except Exception:
                        pass
                    bt = tx_res.get("blockTime")
                    for mint in _extract_mints_from_tx_result(tx_res):
                        mint = _sanitize_mint(mint)
                        if not mint:
                            continue
                        if bt:
                            try:
                                POOL_BIRTH_CACHE[mint] = int(bt)
                            except Exception:
                                pass
                        log.info(f"Logs Firehose ({provider_name}): discovered candidate mint {mint} from signature {signature}")
                        async def _queue():
                            await DISCOVERY_BUCKET.acquire(1)
                            await process_discovered_token(mint)
                        asyncio.create_task(_queue())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
            state["last_failure"] = time.time()
            state["last_error"] = str(e)
            backoff = min(300, base_backoff * (2 ** max(0, state["consecutive_failures"] - 1)))
            state["current_backoff"] = float(backoff)
            FIREHOSE_STATUS[key] = f"🔴 Error: {e.__class__.__name__} (retry in {int(backoff)}s)"
            log.error(
                "Logs Firehose (%s): connection failed after %s consecutive errors: %s. Retrying in %ss...",
                provider_name,
                state["consecutive_failures"],
                e,
                int(backoff),
            )
            await asyncio.sleep(backoff)

async def logs_firehose_worker():
    """Start logsSubscribe firehose across configured providers (Helius/Syndica/Alchemy)."""
    providers = []
    if HELIUS_API_KEY:
        providers.append(("Helius", HELIUS_WS_URL, HELIUS_RPC_URL))
    if SYNDICA_WS_URL and SYNDICA_RPC_URL:
        providers.append(("Syndica", SYNDICA_WS_URL, SYNDICA_RPC_URL))
    if ALCHEMY_WS_URL and ALCHEMY_RPC_URL:
        providers.append(("Alchemy", ALCHEMY_WS_URL, ALCHEMY_RPC_URL))
    if not providers:
        log.warning("Logs Firehose disabled: no provider URLs configured (HELIUS/SYNDICA/ALCHEMY).")
        return
    log.info(f"Logs Firehose: launching {len(providers)} providers...")
    await asyncio.gather(*[_logs_subscriber(name, ws, http) for name, ws, http in providers])

async def discover_from_gecko_new_pools(client: httpx.AsyncClient) -> List[str]:
    """Discover recent Raydium pools on Solana via GeckoTerminal v2.
    Endpoint: /api/v2/networks/solana/new_pools?include=base_token,quote_token,dex,network
    """
    mints: set = set()
    headers = {
        "Accept": "application/json;version=20230302",
        "User-Agent": "Mozilla/5.0"
    }
    url = f"{GECKO_API_URL}/networks/solana/new_pools?include=base_token,quote_token,dex,network"
    try:
        res = await _fetch(client, url, headers=headers)
        data = (res or {}).get("data") or []
        included = (res or {}).get("included") or []
        tok_addr = {item.get("id"): (item.get("attributes") or {}).get("address") for item in included if item.get("type") == "tokens"}
        dex_name = {item.get("id"): (item.get("attributes") or {}).get("name", "").lower() for item in included if item.get("type") == "dexes"}

        for pool in data:
            rel = pool.get("relationships", {})
            base_rel = (rel.get("base_token") or {}).get("data") or {}
            quote_rel = (rel.get("quote_token") or {}).get("data") or {}
            dex_rel = (rel.get("dex") or {}).get("data") or {}
            # Filter to Raydium where possible to reduce noise
            dex_id = dex_rel.get("id")
            if dex_id and dex_name.get(dex_id) and "raydium" not in dex_name.get(dex_id, ""):
                continue
            base = tok_addr.get(base_rel.get("id"))
            quote = tok_addr.get(quote_rel.get("id"))
            if base and base not in KNOWN_QUOTE_MINTS:
                mints.add(base)
            if quote and quote not in KNOWN_QUOTE_MINTS and quote != base:
                mints.add(quote)
    except Exception as e:
        log.warning(f"GeckoTerminal new_pools discovery failed: {e}")
    return list(mints)

async def _discover_from_gecko_search(client: httpx.AsyncClient, query: str) -> List[str]:
    """Search pools globally and filter to Solana/Raydium."""
    mints: set = set()
    from analysis import GECKO_SEARCH_CACHE

    headers = {
        "Accept": "application/json;version=20230302",
        "User-Agent": "Mozilla/5.0"
    }
    url = f"{GECKO_API_URL}/search/pools?query={query}&include=base_token,quote_token,dex,network"
    if (cached := GECKO_SEARCH_CACHE.get(url)):
        return cached
    try:
        res = await _fetch(client, url, headers=headers)
        data = (res or {}).get("data") or []
        included = (res or {}).get("included") or []
        tok_addr = {item.get("id"): (item.get("attributes") or {}).get("address") for item in included if item.get("type") == "tokens"}
        dex_name = {item.get("id"): (item.get("attributes") or {}).get("name", "").lower() for item in included if item.get("type") == "dexes"}
        networks = {item.get("id"): (item.get("attributes") or {}).get("identifier", "").lower() for item in included if item.get("type") == "networks"}

        for pool in data:
            rel = pool.get("relationships", {})
            base_rel = (rel.get("base_token") or {}).get("data") or {}
            quote_rel = (rel.get("quote_token") or {}).get("data") or {}
            dex_rel = (rel.get("dex") or {}).get("data") or {}
            net_rel = (rel.get("network") or {}).get("data") or {}
            if networks.get(net_rel.get("id")) and networks.get(net_rel.get("id")) != "solana":
                continue
            if dex_name.get(dex_rel.get("id")) and "raydium" not in dex_name.get(dex_rel.get("id"), ""):
                continue
            base = tok_addr.get(base_rel.get("id"))
            quote = tok_addr.get(quote_rel.get("id"))
            if base and base not in KNOWN_QUOTE_MINTS:
                mints.add(base)
            if quote and quote not in KNOWN_QUOTE_MINTS and quote != base:
                mints.add(quote)
    except Exception as e:
        log.warning(f"GeckoTerminal search discovery for query '{query}' failed: {e}")
    result = list(mints)
    GECKO_SEARCH_CACHE[url] = result
    return result

async def discover_from_gecko_search_pools(client: httpx.AsyncClient) -> List[str]:
    """Search pools globally and filter to Solana/Raydium."""
    return await _discover_from_gecko_search(client, "solana")

async def discover_from_gecko_search_tokens(client: httpx.AsyncClient) -> List[str]:
    """Use GeckoTerminal search pools API (alternate query) and filter to Solana/Raydium."""
    return await _discover_from_gecko_search(client, "bonk")

async def discover_from_dexscreener_new_pairs(client: httpx.AsyncClient) -> List[str]:
    """Discover recent pairs on Solana via DexScreener and resolve their mints.
    DexScreener occasionally returns a JSON with schemaVersion but null pairs due to edge caching.
    Mitigate with HTTP/1.1, no-cache headers, and a jittered query param to bust stale edges.
    """
    from analysis import DS_NEW_CACHE
    mints = set()
    base_url = "https://api.dexscreener.com/latest/dex/pairs/solana/new"
    try:
        if (cached := DS_NEW_CACHE.get(base_url)):
            return cached
        ds_headers = {
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": "Mozilla/5.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": "https://dexscreener.com/solana",
            "Origin": "https://dexscreener.com"
        }
        # Use HTTP/1.1 for DS to reduce null responses
        async def _ds_get_json() -> Optional[Dict[str, Any]]:
            try:
                ds_c = await get_http_client(ds=True)
                # Add a tiny jitter param to avoid stale CDN edges without breaking cache keying
                req_url = f"{base_url}?t={int(time.time()) % 7}"
                r = await ds_c.get(req_url, headers=ds_headers, follow_redirects=True)
                r.raise_for_status()
                return r.json()
            except Exception:
                return None

        res = await _ds_get_json()
        if not res or not (pairs := res.get("pairs")):
            # One retry with small jitter
            await asyncio.sleep(2.0 + random.uniform(0, 0.5))
            res = await _ds_get_json()
            pairs = (res or {}).get("pairs") if res else None
            if not res or not pairs:
                # Reduce noise: log at debug; this happens sporadically due to DS edge caching
                if res:
                    preview = str(res)[:180]
                    log.debug(f"DexScreener /new returned null pairs. Preview: {preview}")
                return []
        
        # The /new endpoint already contains the token addresses. No need for a second, redundant API call.
        for pair in pairs:
            if base_token := pair.get("baseToken", {}).get("address"):
                if base_token not in KNOWN_QUOTE_MINTS:
                    mints.add(base_token)
            if quote_token := pair.get("quoteToken", {}).get("address"):
                if quote_token not in KNOWN_QUOTE_MINTS and quote_token != base_token:
                    mints.add(quote_token)
        result = list(mints)
        DS_NEW_CACHE[base_url] = result
        return result
    except Exception as e:
        log.warning(f"DexScreener discovery failed: {e}")
        return []

# Removed broken DexScreener "recent" endpoint usage (/latest/dex/pairs/solana).
# Use /latest/dex/search and /latest/dex/pairs/solana/new instead.

async def discover_from_dexscreener_search_recent(client: httpx.AsyncClient) -> List[str]:
    """Fallback: use DexScreener search API and filter Solana pairs by recent creation time."""
    mints = set()
    url = "https://api.dexscreener.com/latest/dex/search?q=solana"
    try:
        ds_headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://dexscreener.com/solana"
        }
        ds_c = await get_http_client(ds=True)
        r = await ds_c.get(url, headers=ds_headers, follow_redirects=True)
        r.raise_for_status()
        res = r.json()
        pairs = (res or {}).get("pairs") or []
        if not pairs:
            if res:
                preview = str(res)[:180]
                log.info(f"DexScreener search: no pairs. Preview: {preview}")
            return []

        now_ms = int(time.time() * 1000)
        # Slightly wider window (10 minutes) to catch true new pairs reliably
        freshness_minutes = 10

        for p in pairs:
            if p.get("chainId") != "solana":
                continue
            created_ms = p.get("pairCreatedAt") or p.get("createdAt")
            try:
                if not created_ms:
                    continue
                age_min = (now_ms - int(created_ms)) / 60000.0
            except (ValueError, TypeError):
                continue
            if age_min > freshness_minutes:
                continue

            base_token = (p.get("baseToken") or {}).get("address")
            quote_token = (p.get("quoteToken") or {}).get("address")
            if base_token and base_token not in KNOWN_QUOTE_MINTS:
                mints.add(base_token)
            if quote_token and quote_token not in KNOWN_QUOTE_MINTS and quote_token != base_token:
                mints.add(quote_token)
    except Exception as e:
        log.warning(f"DexScreener search discovery failed: {e}")
    return list(mints)

async def aggregator_poll_worker():
    """Background worker to periodically poll aggregators for new tokens."""
    log.info("🦎 Aggregator Poller: Worker starting.")
    while True:
        try:
            client = await get_http_client()
            # Optional: Disable GeckoTerminal sources if they’re unstable or rate-limited
            disable_gecko = str(os.getenv("DISABLE_GECKO", "0")).strip().lower() in {"1","true","yes","y"}
            gecko_task = discover_from_gecko_new_pools(client) if not disable_gecko else asyncio.sleep(0)
            gecko_search_task = discover_from_gecko_search_pools(client) if not disable_gecko else asyncio.sleep(0)
            gecko_token_search_task = discover_from_gecko_search_tokens(client) if not disable_gecko else asyncio.sleep(0)
            dexscreener_new_task = discover_from_dexscreener_new_pairs(client)
            dexscreener_search_task = discover_from_dexscreener_search_recent(client)
            results = await asyncio.gather(
                gecko_task,
                gecko_search_task,
                gecko_token_search_task,
                dexscreener_new_task,
                dexscreener_search_task,
                return_exceptions=True
            )

            all_new_mints = set()
            for i, result in enumerate(results):
                source_name = (
                    "GeckoTerminal new" if i == 0 else
                    "GeckoTerminal search pools" if i == 1 else
                    "GeckoTerminal search tokens" if i == 2 else
                    "DexScreener /new" if i == 3 else
                    "DexScreener search filtered"
                )
                if isinstance(result, Exception):
                    log.warning(f"🦎 Aggregator Poller: {source_name} task failed: {result}")
                elif result:
                    log.info(f"🦎 Aggregator Poller: Found {len(result)} potential new tokens from {source_name}.")
                    all_new_mints.update(result)
                else:
                    log.info(f"🦎 Aggregator Poller: {source_name} returned no new tokens this cycle.")
            if all_new_mints:
                total = len(all_new_mints)
                max_new = int(CONFIG.get("AGGREGATOR_MAX_NEW_PER_CYCLE", 0) or 0)
                to_queue = list(all_new_mints)
                if max_new > 0 and total > max_new:
                    to_queue = random.sample(to_queue, max_new)
                    log.info(f"🦎 Aggregator Poller: {total} found, capping to {max_new} this cycle.")
                else:
                    log.info(f"🦎 Aggregator Poller: Total unique new mints this cycle: {total}.")
                for mint in to_queue:
                    async def _queue(m=mint):
                        await DISCOVERY_BUCKET.acquire(1)
                        await process_discovered_token(m)
                    asyncio.create_task(_queue())
        except Exception as e:
            log.error(f"🦎 Aggregator Poller: Error during poll cycle: {e}")
        
        await asyncio.sleep(CONFIG["AGGREGATOR_POLL_INTERVAL_MINUTES"] * 60)

# ======================================================================================
# Block 5: Dynamic Pot System & Analysis Pipeline
# ======================================================================================

async def process_discovered_token(mint: str):
    """Single entry point for any newly discovered token from any source."""
    try:
        mint = _sanitize_mint(mint) or ""
        if not is_valid_solana_address(mint) or mint in _seen_mints:
            return
        
        if await _execute_db("SELECT 1 FROM TokenLog WHERE mint_address = ?", (mint,), fetch='one'):
            return

        _seen_mints.append(mint)
        log.info(f"DISCOVERED: {mint}. Queued for initial analysis.")
        
        # Just insert it with 'discovered' status. The initial_analyzer_worker will pick it up.
        await _execute_db("INSERT OR IGNORE INTO TokenLog (mint_address, status) VALUES (?, 'discovered')", (mint,), commit=True)
    except Exception as e:
        # This is a critical catch-all to ensure we see any errors during the very first step.
        log.error(f"CRITICAL ERROR in process_discovered_token for mint '{mint}': {e}", exc_info=True)

# Priority calculation helper (engine)
def calculate_priority(i: Dict[str, Any]) -> int:
    try:
        score = float(i.get("score", 0) or 0)
    except Exception:
        score = 0.0
    try:
        liq = float(i.get("liquidity_usd", 0) or 0)
    except Exception:
        liq = 0.0
    try:
        vol = float(i.get("volume_24h_usd", 0) or 0)
    except Exception:
        vol = 0.0
    try:
        age_m = float(i.get("age_minutes", 0) or 0)
    except Exception:
        age_m = 0.0

    def norm(x: float, k: float) -> float:
        return x / (x + k) if x >= 0 else 0.0

    pr = 0.0
    pr += 0.6 * score
    pr += 20.0 * norm(liq, 25_000)
    pr += 20.0 * norm(vol, 50_000)
    if age_m > 60:
        pr -= min(15.0, (age_m - 60) / 60.0 * 5.0)
    return int(max(0, min(100, pr)))

async def update_token_tags(mint: str, intel: Dict):
    """Updates the boolean candidate flags and enhanced bucket based on the latest intel."""
    # Compute multiple age signals (minutes)
    ages = []
    try:
        if (a := intel.get("age_minutes")) is not None:
            ages.append(float(a))
    except Exception:
        pass
    try:
        if (iso := intel.get("created_at_pool")):
            dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
            ages.append((datetime.now(timezone.utc) - dt).total_seconds() / 60)
    except Exception:
        pass
    try:
        row = await _execute_db("SELECT discovered_at FROM TokenLog WHERE mint_address=?", (mint,), fetch='one')
        if row and row[0]:
            ddt = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
            ages.append((datetime.now(timezone.utc) - ddt).total_seconds() / 60)
    except Exception:
        pass
    recent_age = min(ages) if ages else None

    # Hatching: newborn (≤ HATCHING_MAX_AGE_MINUTES) and has minimum liquidity.
    # If liquidity is unknown (None), allow into hatching; only exclude when explicit liq is below threshold.
    liq_val = intel.get("liquidity_usd", None)
    meets_liq = True if liq_val is None else (float(liq_val or 0) >= CONFIG["MIN_LIQUIDITY_FOR_HATCHING"])
    is_hatching = (
        (recent_age is not None and recent_age <= CONFIG["HATCHING_MAX_AGE_MINUTES"]) and
        meets_liq
    )

    # Cooking: momentum heuristic — combine price move and minimum volume
    try:
        price_change = float(intel.get("price_change_24h", 0) or 0)
    except Exception:
        price_change = 0.0
    try:
        vol24h = float(intel.get("volume_24h_usd", 0) or 0)
    except Exception:
        vol24h = 0.0
    vol_floor = float(CONFIG.get("COOKING_FALLBACK_VOLUME_MIN_USD", 200) or 200)
    is_cooking = (price_change >= 15.0 and vol24h >= max(500.0, vol_floor))

    # Fresh: young (≤24h)
    is_fresh = (intel.get("age_minutes", 99999) < 24 * 60)

    priority = calculate_priority(intel)
    score = intel.get("score", 0)

    bucket = "standby"
    if priority >= 80:
        bucket = "priority"
    elif is_hatching:
        bucket = "hatching"
    elif is_fresh:
        bucket = "fresh"
    elif is_cooking:
        bucket = "cooking"
    elif score >= 70:
        bucket = "top"

    log.info(f"Updating tags for {mint}: hatching={is_hatching}, cooking={is_cooking}, fresh={is_fresh}, priority={priority}, score={score}, bucket={bucket}")

    query = """
        UPDATE TokenLog SET
            is_hatching_candidate = ?,
            is_cooking_candidate = ?,
            is_fresh_candidate = ?,
            enhanced_bucket = ?,
            priority = ?
        WHERE mint_address = ?
    """
    await _execute_db(query, (is_hatching, is_cooking, is_fresh, bucket, priority, mint), commit=True)

async def re_analyzer_worker():
    """Periodically refreshes market snapshot + retags a subset of tokens based on staleness.
    Uses bucket-specific cadences and caps batch size to avoid network/DB overload.
    """
    log.info("🤖 Re-Analyzer Engine: Firing up the refresh cycle.")
    while True:
        try:
            # Global pacing between cycles (seconds)
            try:
                _interval_sec = int(float(CONFIG.get("RE_ANALYZER_INTERVAL_MINUTES", 2) or 2) * 60)
            except Exception:
                _interval_sec = 120
            # Pace the loop to avoid tight spins
            await asyncio.sleep(max(5, _interval_sec))
            hm = f"-{int(CONFIG['HATCHING_REANALYZE_MINUTES'])} minutes"
            fm = f"-{int(CONFIG['FRESH_REANALYZE_MINUTES'])} minutes"
            cm = f"-{int(CONFIG['COOKING_REANALYZE_MINUTES'])} minutes"
            om = f"-{int(CONFIG['OTHER_REANALYZE_MINUTES'])} minutes"
            pm = f"-{int(CONFIG['HATCHING_REANALYZE_MINUTES'])} minutes" # Priority tokens should be re-analyzed frequently, like hatching
            limit = int(CONFIG.get("RE_ANALYZER_BATCH_LIMIT", 60))
            # Upgraded query to use enhanced_bucket for smarter re-analysis
            query = """
                SELECT mint_address, intel_json
                FROM TokenLog
                WHERE status IN ('analyzed','served') AND (
                    (enhanced_bucket = 'hatching' AND (last_snapshot_time IS NULL OR last_snapshot_time <= datetime('now', ?)))
                 OR (enhanced_bucket = 'cooking' AND (last_snapshot_time IS NULL OR last_snapshot_time <= datetime('now', ?)))
                 OR (enhanced_bucket = 'fresh' AND (last_snapshot_time IS NULL OR last_snapshot_time <= datetime('now', ?)))
                 OR (enhanced_bucket = 'priority' AND (last_snapshot_time IS NULL OR last_snapshot_time <= datetime('now', ?)))
                 OR (enhanced_bucket NOT IN ('hatching', 'cooking', 'fresh', 'priority') AND (last_snapshot_time IS NULL OR last_snapshot_time <= datetime('now', ?)))
                )
                ORDER BY
                    CASE enhanced_bucket
                        WHEN 'priority' THEN 0
                        WHEN 'hatching' THEN 1
                        WHEN 'cooking' THEN 2
                        WHEN 'fresh' THEN 3
                        ELSE 4
                    END,
                    COALESCE(last_snapshot_time, '1970-01-01') ASC
                LIMIT ?
            """
            rows = await _execute_db(query, (hm, cm, fm, pm, om, limit), fetch='all')
            if not rows: 
                log.info("🤖 Re-Analyzer: No tokens need refresh this cycle.")
                continue
            log.info(f"🤖 Re-Analyzer: Starting cycle for {len(rows)} tokens...")

            client = await get_http_client()
            # Concurrently fetch market data with bounded concurrency to avoid saturating network
            limit = int(CONFIG.get("RE_ANALYZER_FETCH_CONCURRENCY", 6))
            sem = asyncio.Semaphore(max(1, limit))
            async def _task(mint: str):
                async with sem:
                    return await fetch_market_snapshot(client, mint)
            tasks = [_task(row[0]) for row in rows]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process each token sequentially for DB updates to reduce write contention
            for i, result in enumerate(results):
                mint, old_intel_json = rows[i]
                if isinstance(result, Exception) or not result:
                    # Graceful fallback: try recent snapshot instead of dropping the token
                    try:
                        snap = await load_latest_snapshot(mint)
                        stale_sec = int(CONFIG.get("SNAPSHOT_STALENESS_SECONDS", 600) or 600)
                        if isinstance(snap, dict) and (snap.get("snapshot_age_sec") or 1e9) <= stale_sec:
                            intel = json.loads(old_intel_json)
                            for k in ("liquidity_usd", "volume_24h_usd", "market_cap_usd"):
                                if k in snap:
                                    intel[k] = snap[k]
                            # Recompute scores with cached values
                            intel["mms_score"] = _compute_mms(intel)
                            intel["score"] = _compute_score(intel)
                            await upsert_token_intel(mint, intel)
                            await update_token_tags(mint, intel)
                            # Do not save another snapshot (we just used an existing one)
                            log.info(f"🤖 Re-Analyzer: Used cached snapshot for {mint} (live refresh unavailable).")
                            continue
                    except Exception:
                        pass
                    log.warning(f"🤖 Re-Analyzer: Failed to refresh market data for {mint} (no live or cached snapshot).")
                    continue

                intel = json.loads(old_intel_json)

                # Recalculate age on every cycle, prefer pool creation time
                try:
                    creation_dt = None
                    if creation_time_str := intel.get("created_at_pool"):
                        creation_dt = datetime.fromisoformat(str(creation_time_str).replace("Z", "+00:00"))
                    elif creation_time_str := intel.get("created_at"):
                        creation_dt = datetime.fromisoformat(str(creation_time_str).replace("Z", "+00:00"))
                    else:
                        # fallback to DB discovery time
                        row = await _execute_db("SELECT discovered_at FROM TokenLog WHERE mint_address=?", (mint,), fetch='one')
                        if row and row[0]:
                            creation_dt = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
                    if creation_dt:
                        intel["age_minutes"] = (datetime.now(timezone.utc) - creation_dt).total_seconds() / 60
                except Exception:
                    pass

                intel.update(result)
                # If live result has pair_created_ms or pool_created_at, normalize into created_at_pool for tagging
                try:
                    pool_dt = None
                    if (ms := result.get("pair_created_ms")):
                        pool_dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
                    elif (iso := result.get("pool_created_at")):
                        pool_dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
                    if pool_dt:
                        intel["created_at_pool"] = pool_dt.isoformat()
                        intel["age_minutes"] = (datetime.now(timezone.utc) - pool_dt).total_seconds() / 60
                except Exception:
                    pass

                intel["mms_score"] = _compute_mms(intel)
                intel["score"] = _compute_score(intel)

                # Apply updates one token at a time to avoid DB lock storms
                await upsert_token_intel(mint, intel)
                await update_token_tags(mint, intel)
                # Save a lightweight snapshot for quick fallbacks
                await save_snapshot(mint, intel)
            log.info("🤖 Re-Analyzer: Cycle complete.")
        except Exception as e:
            log.error(f"🤖 Re-Analyzer: Error during cycle: {e}")

async def second_chance_worker():
    """Periodically re-evaluates a few 'rejected' tokens to see if they've improved."""
    log.info("🧐 Second Chance Protocol: Engaging the scrap heap scanner.")
    await asyncio.sleep(120) # Initial delay
    while True:
        try:
            # Look at a small, random sample of rejected tokens
            rows = await _execute_db("SELECT mint_address FROM TokenLog WHERE status = 'rejected' ORDER BY RANDOM() LIMIT 5", fetch='all')
            if not rows:
                await asyncio.sleep(3600) # Nothing to do, wait an hour
                continue

            mints_to_recheck = [row[0] for row in rows]
            log.info(f"🧐 Second Chance: Re-evaluating {len(mints_to_recheck)} rejected tokens.")

            client = await get_http_client()
            for mint in mints_to_recheck:
                # Just do a quick, cheap market data check
                market_data = await fetch_birdeye(client, mint)
                # Normalize BirdEye result into unified keys if present
                if market_data and isinstance(market_data.get("data"), dict):
                    be = market_data["data"]
                    market_data = {
                        "liquidity_usd": float(be.get("liquidity", 0.0)),
                        "market_cap_usd": float(be.get("mc", 0.0)),
                        "volume_24h_usd": float(be.get("v24h", 0.0)),
                        "price_change_24h": float(be.get("priceChange24h", 0.0)),
                    }
                if not market_data:
                    market_data = await fetch_dexscreener_by_mint(client, mint)
                
                if market_data and float(market_data.get("liquidity_usd", 0) or 0) >= CONFIG["MIN_LIQUIDITY_FOR_HATCHING"]:
                    log.info(f"🌟 REDEMPTION: {mint} now has enough liquidity! Moving back to discovery queue.")
                    await _execute_db("UPDATE TokenLog SET status = 'discovered' WHERE mint_address = ?", (mint,), commit=True)
                await asyncio.sleep(5) # Rate limit ourselves
        except Exception as e:
            log.error(f"🧐 Second Chance Worker: Error during cycle: {e}")
        await asyncio.sleep(3600) # Run once per hour

async def _process_one_initial_token(mint: str):
    """Helper to analyze one discovered token, wait, and update its status."""
    log.info(f"🧐 Initial Analyzer: Processing {mint}. Waiting for APIs to index...")
    # Indexing delay is handled upstream by selecting only sufficiently old
    # discovered rows in the processing query. Avoid per-token sleeps here.

    try:
        client = await get_http_client()
        log.info(f"🧐 Initial Analyzer: Wait over. Starting enrichment for {mint}.")
        intel = await enrich_token_intel(client, mint, deep_dive=False)

        if not intel:
                await _execute_db("UPDATE TokenLog SET status = 'rejected' WHERE mint_address = ?", (mint,), commit=True)
                log.info(f"REJECTED: {mint} - Failed enrichment (no data).")
        else:
                await upsert_token_intel(mint, intel)
                await update_token_tags(mint, intel)
                if any(intel.get(k) is not None for k in ("liquidity_usd", "volume_24h_usd", "market_cap_usd")):
                    await save_snapshot(mint, intel)
                log.info(f"✅ ADDED TO POT: {intel.get('symbol', mint)} (Score: {intel.get('score')}, Liq: ${intel.get('liquidity_usd', 0):,.2f})")
    except Exception as e:
        log.error(f"🧐 Initial Analyzer: Error processing {mint}: {e}. Marking as rejected.")
        await _execute_db("UPDATE TokenLog SET status = 'rejected' WHERE mint_address = ?", (mint,), commit=True)

async def process_discovery_queue():
    """Enhanced processing worker with adaptive batching. Replaces initial_analyzer_worker."""
    global adaptive_batch_size
    log.info("🚀 Blueprint Engine: Adaptive intake worker is online.")
    await asyncio.sleep(15) # Initial delay

    while True:
        try:
            start_time = time.time()

            # Calculate adaptive batch size
            if CONFIG["ADAPTIVE_BATCH_SIZE"] and len(recent_processing_times) >= 5:
                avg_time = statistics.mean(recent_processing_times)
                target_time = CONFIG["TARGET_PROCESSING_TIME"]

                if avg_time < target_time * 0.7:
                    adaptive_batch_size = min(adaptive_batch_size + 2, CONFIG["MAX_BATCH_SIZE"])
                elif avg_time > target_time * 1.3:
                    adaptive_batch_size = max(adaptive_batch_size - 1, CONFIG["MIN_BATCH_SIZE"])

            # Gate selection on discovered_at to let indexers catch up, instead of sleeping per token
            idx_wait = int(CONFIG.get("INDEXING_WAIT_SECONDS", 60) or 60)
            rows = await _execute_db(
                "SELECT mint_address FROM TokenLog WHERE status = 'discovered' AND discovered_at <= datetime('now', ?) ORDER BY discovered_at ASC LIMIT ?",
                (f"-{idx_wait} seconds", adaptive_batch_size), fetch='all'
            )

            if not rows:
                await asyncio.sleep(30)
                continue

            mints_to_process = [row[0] for row in rows]
            log.info(f"Enhanced Processor: Found {len(mints_to_process)} new tokens (age≥{idx_wait}s). Processing batch...")

            # Cap concurrency to avoid bursts against APIs and DB
            conc = int(CONFIG.get("INITIAL_ANALYSIS_CONCURRENCY", 8) or 8)
            sem = asyncio.Semaphore(max(1, conc))
            async def _run(m: str):
                async with sem:
                    await _process_one_initial_token(m)
            await asyncio.gather(*[_run(m) for m in mints_to_process])
            
            processing_time = time.time() - start_time
            recent_processing_times.append(processing_time)
            log.info(f"📊 Processed {len(mints_to_process)} tokens in {processing_time:.2f}s (batch size: {adaptive_batch_size})")
        except Exception as e:
            log.error(f"Enhanced processing queue error: {e}", exc_info=True)
            await asyncio.sleep(60)

# ======================================================================================
# Block 7: Scoring, Quips & Reporting
# ======================================================================================

# === Database maintenance & owner commands ===

async def _db_prune(retain_snap_days: int, retain_rejected_days: int) -> int:
    removed = 0
    try:
        # Remove old snapshots
        await _execute_db(
            "DELETE FROM TokenSnapshots WHERE snapshot_time < datetime('now', ?)",
            (f"-{retain_snap_days} days",), commit=True
        )
        # Remove old rejected tokens
        await _execute_db(
            "DELETE FROM TokenLog WHERE status='rejected' AND (last_analyzed_time IS NULL OR last_analyzed_time < datetime('now', ?))",
            (f"-{retain_rejected_days} days",), commit=True
        )
        # VACUUM
        await _execute_db("VACUUM", commit=True)
        removed = 1
    except Exception as e:
        log.error(f"DB prune failed: {e}")
    return removed

async def _db_purge_all() -> None:
    try:
        await _execute_db("DELETE FROM TokenSnapshots", commit=True)
        await _execute_db("DELETE FROM TokenLog", commit=True)
        await _execute_db("VACUUM", commit=True)
        # reset marker
        try:
            Path(DB_MARKER_FILE).write_text(datetime.now(timezone.utc).isoformat(), encoding='utf-8')
        except Exception as e:
            log.warning(f"VACUUM failed: {e}")
    except Exception as e:
        log.error(f"DB purge failed: {e}")

async def maintenance_worker():
    log.info("🧹 Maintenance Protocol: Tony's cleaning crew is on the clock.")
    # Initialize DB marker if missing
    while True:
        try:
            await _db_prune(CONFIG["SNAPSHOT_RETENTION_DAYS"], CONFIG["REJECTED_RETENTION_DAYS"])
            # Checkpoint and truncate WAL to prevent uncontrolled growth
            try:
                await _execute_db("PRAGMA wal_checkpoint(TRUNCATE)", commit=True)
            except Exception:
                pass
            # Drop very old 'discovered' rows to prevent backlog bloat
            try:
                hrs = int(CONFIG.get("DISCOVERED_RETENTION_HOURS", 0) or 0)
                if hrs > 0:
                    await _execute_db(
                        "DELETE FROM TokenLog WHERE status='discovered' AND discovered_at < datetime('now', ?)",
                        (f"-{hrs} hours",), commit=True
                    )
            except Exception:
                pass
            # Optional full purge by age
            if (CONFIG.get("FULL_PURGE_INTERVAL_DAYS") or 0) > 0:
                try:
                    row = await _execute_db("SELECT value FROM KeyValueStore WHERE key = 'last_purge_time'", fetch='one')
                    if row and row[0]:
                        dt = datetime.fromisoformat(row[0])
                        age_days = (datetime.now(timezone.utc) - dt).days
                        if age_days >= int(CONFIG["FULL_PURGE_INTERVAL_DAYS"]):
                            log.warning("DB age exceeded FULL_PURGE_INTERVAL_DAYS. Purging all state.")
                            await _db_purge_all()
                            await _execute_db("INSERT OR REPLACE INTO KeyValueStore (key, value) VALUES (?, ?)", ('last_purge_time', datetime.now(timezone.utc).isoformat()), commit=True)
                except Exception:
                    pass
        except Exception as e:
            log.error(f"Maintenance cycle error: {e}")
        await asyncio.sleep(max(3600, int(CONFIG["MAINTENANCE_INTERVAL_HOURS"]) * 3600))

"""Deprecated report/grade helpers (replaced by *_label/*2 variants) removed for clarity."""

# --- Circuit breaker reset worker ---
async def circuit_breaker_reset_worker():
    """Periodically relax provider circuit breakers and decay failure counts.
    This allows temporarily failing providers to recover without hammering them.
    """
    while True:
        try:
            for name, stats in API_PROVIDERS.items():
                # Light decay of failure count; keep success as-is
                fail = stats.get('failure', 0)
                stats['failure'] = max(0, int(fail * 0.8))
                # If circuit is open, probe by closing it after cooldown window
                if stats.get('circuit_open'):
                    if stats['failure'] < 10:
                        stats['circuit_open'] = False
                        log.info(f"Circuit reset for provider {name}.")
        except Exception as e:
            log.warning(f"Log cleanup failed: {e}")
        await asyncio.sleep(120)

# Removed old _confidence_bar in favor of _confidence_bar2

# ======================================================================================
# Block 8: Telegram Handlers & Main Application
# ======================================================================================

async def _safe_is_group(u: Update) -> bool:
    try:
        t = (u.effective_chat.type or "").lower()
        return t in {"group", "supergroup"}
    except Exception:
        return False

async def _maybe_send_typing(u: Update):
    """Tony's typing indicator - with proper error handling."""
    try:
        if u.message and u.message.chat:
            await u.message.chat.send_action(ChatAction.TYPING)
    except Exception as e:
        log.debug(f"🎭 Typing indicator failed (not critical): {e}")

async def safe_reply_text(u: Update, text: str, **kwargs):
    bot = u.get_bot()
    chat_id = u.effective_chat.id
    chat_type = (getattr(u.effective_chat, 'type', '') or '').lower()
    # Map PTB's reply convenience to API fields
    if kwargs.get("quote"):
        kwargs["reply_to_message_id"] = getattr(getattr(u, "effective_message", None), "message_id", None)
        kwargs.pop("quote", None)
    # Channels don't support reply keyboards; drop reply_markup to avoid 400s
    if chat_type == 'channel' and 'reply_markup' in kwargs:
        kwargs.pop('reply_markup', None)
    return await OUTBOX.send_text(bot, chat_id, text, is_group=await _safe_is_group(u), **kwargs)

async def safe_reply_photo(u: Update, photo: bytes, **kwargs):
    bot = u.get_bot()
    chat_id = u.effective_chat.id
    if kwargs.get("quote"):
        kwargs["reply_to_message_id"] = getattr(getattr(u, "effective_message", None), "message_id", None)
        kwargs.pop("quote", None)
    return await OUTBOX.send_photo(bot, chat_id, photo, is_group=await _safe_is_group(u), **kwargs)

# create_links_keyboard removed; use action_row() instead

# ======================================================================================
# Block 9: Scheduled Pushes (Public/VIP) using cache-only reads
# ======================================================================================

SEGMENT_TO_TAG = {
    'fresh': 'is_fresh_candidate',
    'hatching': 'is_hatching_candidate',
    'cooking': 'is_cooking_candidate',
}

async def _select_items_for_segment(segment: str, cooldown: set) -> List[Dict[str, Any]]:
    seg = segment.lower().strip()
    if seg in SEGMENT_TO_TAG:
        tag = SEGMENT_TO_TAG[seg]
        # Use per-command floors if present
        if seg == 'fresh':
            min_score = CONFIG.get("FRESH_MIN_SCORE_TO_SHOW", CONFIG['MIN_SCORE_TO_SHOW'])
            limit = CONFIG.get("FRESH_COMMAND_LIMIT", 2)
        elif seg == 'hatching':
            min_score = CONFIG.get("HATCHING_MIN_SCORE_TO_SHOW", 0)
            limit = CONFIG.get("HATCHING_COMMAND_LIMIT", 2)
        else:
            min_score = CONFIG['MIN_SCORE_TO_SHOW']
            limit = CONFIG.get("COOKING_COMMAND_LIMIT", 2)
        items = await get_reports_by_tag(tag, int(limit), cooldown, min_score=int(min_score))
        # Fallback like /fresh and /hatching commands if tags are empty
        if not items and seg == 'fresh':
            exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
            query = f"""
                SELECT intel_json FROM TokenLog
                WHERE status IN ('analyzed','served')
                AND final_score >= {CONFIG.get('FRESH_MIN_SCORE_TO_SHOW', CONFIG['MIN_SCORE_TO_SHOW'])}
                AND (age_minutes IS NULL OR age_minutes < 1440)
                AND mint_address NOT IN ({exclude_placeholders})
                ORDER BY last_analyzed_time DESC, final_score DESC
                LIMIT ?
            """
            params = (*cooldown, int(limit))
            rows = await _execute_db(query, params, fetch='all')
            items = [json.loads(row[0]) for row in rows] if rows else []
        if not items and seg == 'cooking':
            # Fallback: pick high-volume tokens by joining the latest snapshot per mint
            exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
            min_vol = float(CONFIG.get('COOKING_FALLBACK_VOLUME_MIN_USD', 1000) or 1000)
            query = f"""
                WITH latest AS (
                    SELECT mint_address, MAX(snapshot_time) AS snapshot_time
                    FROM TokenSnapshots
                    GROUP BY mint_address
                )
                SELECT TL.intel_json
                FROM TokenLog TL
                JOIN latest L ON L.mint_address = TL.mint_address
                JOIN TokenSnapshots TS ON TS.mint_address = L.mint_address AND TS.snapshot_time = L.snapshot_time
                WHERE TL.status IN ('analyzed','served')
                  AND TL.mint_address NOT IN ({exclude_placeholders})
                  AND COALESCE(TS.volume_24h_usd, 0) >= ?
                ORDER BY TS.snapshot_time DESC, COALESCE(TS.volume_24h_usd, 0) DESC
                LIMIT ?
            """
            params = (*cooldown, float(min_vol), int(limit)) if cooldown else (float(min_vol), int(limit))
            rows = await _execute_db(query, params, fetch='all')
            items = [json.loads(row[0]) for row in rows] if rows else []
        if not items and seg == 'cooking':
            # Tertiary fallback: recent analyzed sorted by in-intel 24h price change
            exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
            query = f"""
                SELECT intel_json FROM TokenLog
                WHERE status IN ('analyzed','served')
                  AND mint_address NOT IN ({exclude_placeholders})
                ORDER BY last_analyzed_time DESC
                LIMIT 50
            """
            params = (*cooldown,) if cooldown else ()
            rows = await _execute_db(query, params, fetch='all')
            if rows:
                pool = [json.loads(r[0]) for r in rows]
                pool.sort(key=lambda x: float(x.get('price_change_24h') or 0), reverse=True)
                items = pool[:int(limit)]
        if not items and seg == 'hatching':
            exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
            age_limit = int(CONFIG.get('HATCHING_MAX_AGE_MINUTES', 30))
            query = f"""
                SELECT intel_json FROM TokenLog
                WHERE status IN ('analyzed','served')
                AND (age_minutes IS NULL OR age_minutes <= {age_limit})
                AND final_score >= {CONFIG.get('HATCHING_MIN_SCORE_TO_SHOW', 0)}
                AND mint_address NOT IN ({exclude_placeholders})
                ORDER BY last_analyzed_time DESC
                LIMIT ?
            """
            params = (*cooldown, int(limit))
            rows = await _execute_db(query, params, fetch='all')
            items = [json.loads(row[0]) for row in rows] if rows else []
        return items

    if seg == 'top':
        # Top by final_score, then recent first
        limit = int(CONFIG.get("TOP_COMMAND_LIMIT", 2))
        exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND mint_address NOT IN ({exclude_placeholders})
            AND final_score >= {CONFIG['MIN_SCORE_TO_SHOW']}
            ORDER BY final_score DESC, last_analyzed_time DESC
            LIMIT ?
        """
        params = (*cooldown, limit)
        rows = await _execute_db(query, params, fetch='all')
        return [json.loads(r[0]) for r in rows] if rows else []

    return []

async def _prepare_segment_text_from_cache(segment: str) -> Tuple[Optional[str], List[str]]:
    """Builds the segment text without triggering live HTTP calls.
    Returns (text, minted_ids_served). Adds 'Lite Mode' when cache is stale or circuit breaker is active.
    """
    cooldown_hours = int(CONFIG.get("PUSH_COOLDOWN_HOURS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours) 
    items = await _select_items_for_segment(segment, cooldown)
    if not items:
        # Provide a compact nothing-found message per segment
        empty_lines = {
            'fresh': "– Reservoir’s dry, Tony. No top-tier fresh signals right now. ⏱️",
            'hatching': "🦉 Token's nest is empty. No brand-new, structurally sound tokens right now.",
            'cooking': "🍳 Stove's cold. Nothing showing significant momentum right now.",
            'top': "– Nothin' but crickets. The pot's a bit thin right now, check back later. 🦗",
        }
        return empty_lines.get(segment, "Nothing to show right now."), []

    # Determine Lite Mode: if circuit breaker tripped OR snapshots stale
    lite_mode = False
    try:
        if LITE_MODE_UNTIL and LITE_MODE_UNTIL > time.time():
            lite_mode = True
        else:
            snaps = await asyncio.gather(*[load_latest_snapshot(i.get('mint')) for i in items], return_exceptions=True)
            staleness = int(CONFIG.get("SNAPSHOT_STALENESS_SECONDS", 600) or 600)
            for s in snaps:
                if isinstance(s, dict):
                    if (s.get('snapshot_age_sec') or 1e9) > staleness:
                        lite_mode = True
                        break
                else:
                    # No snapshot available => treat as lite
                    lite_mode = True
                    break
    except Exception:
        pass

    header = pick_header_label(f"/{segment}")
    if lite_mode:
        header = f"{header} — ⚡ Lite Mode"
    limit = int(CONFIG.get(f"{segment.upper()}_COMMAND_LIMIT", 2) or 2)
    final = build_segment_message(segment, items[:limit], lite_mode=lite_mode)
    served = [i.get('mint') for i in items[:limit] if i.get('mint')]
    return final, served

async def push_segment_to_chat(app: Application, chat_id: int, segment: str) -> None:
    """Edit the existing segment message in a chat or send a new one if missing."""
    try:
        text, served = await _prepare_segment_text_from_cache(segment)
        if not text:
            return
        mid = await get_push_message_id(chat_id, segment)
        # Try to edit first
        if mid:
            try:
                # Apply basic gating to avoid pool bursts before editing
                try:
                    await OUTBOX.global_bucket.acquire(1)
                    if int(chat_id) < 0:
                        await (await OUTBOX._group_bucket(int(chat_id))).acquire(1)
                    await (await OUTBOX._chat_bucket(int(chat_id))).acquire(1)
                except Exception:
                    pass
                await app.bot.edit_message_text(chat_id=chat_id, message_id=mid, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e:
                msg = str(e)
                if "message to edit not found" in msg.lower() or "message_id" in msg.lower():
                    mid = None  # fall through to send new
                elif "message is not modified" in msg.lower():
                    pass
                else:
                    # Unexpected edit error — try sending a fresh message
                    mid = None
        if not mid:
            sent = await app.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            try:
                await set_push_message_id(chat_id, segment, sent.message_id)
            except Exception:
                log.debug("Failed to persist push message id for chat %s segment %s", chat_id, segment)
    except Exception as e:
        log.error(f"Error pushing segment {segment} to chat {chat_id}: {e}")







# Global push tracking to prevent duplicates
ACTIVE_PUSHES = set()
PUSH_FAILURES = {}

async def scheduled_push_job(context: ContextTypes.DEFAULT_TYPE):
    """Rock-solid push job with Tony's reliability standards."""
    data = context.job.data or {}
    seg = data.get('segment')
    chat_id = data.get('chat_id')
    
    if not seg or not chat_id:
        log.warning(f"🚨 Push job missing critical data: segment={seg}, chat_id={chat_id}")
        return
    
    job_key = f"push_{chat_id}_{seg}"
    
    # Prevent duplicate pushes - Tony doesn't repeat himself
    if job_key in ACTIVE_PUSHES:
        log.info(f"🛡️ Tony's already pushing {seg} to {chat_id} - skipping duplicate")
        return
    
    try:
        ACTIVE_PUSHES.add(job_key)
        
        # Check for recent failures and implement backoff
        failure_key = f"{chat_id}_{seg}"
        if failure_key in PUSH_FAILURES:
            last_failure, count = PUSH_FAILURES[failure_key]
            backoff_time = min(300, 30 * (2 ** count))  # Max 5min backoff
            if time.time() - last_failure < backoff_time:
                log.info(f"⏳ Tony's backing off {seg} push to {chat_id} for {backoff_time}s")
                return
        
        await push_segment_to_chat(context.application, int(chat_id), str(seg))
        
        # Clear failure tracking on success
        if failure_key in PUSH_FAILURES:
            del PUSH_FAILURES[failure_key]
            log.info(f"✅ Tony's back online for {seg} pushes to {chat_id}")
        
    except Exception as e:
        log.error(f"💥 Push job failed for {chat_id}/{seg}: {e}")
        
        # Track failures for intelligent backoff
        failure_key = f"{chat_id}_{seg}"
        if failure_key in PUSH_FAILURES:
            PUSH_FAILURES[failure_key] = (time.time(), PUSH_FAILURES[failure_key][1] + 1)
        else:
            PUSH_FAILURES[failure_key] = (time.time(), 1)
            
    finally:
        ACTIVE_PUSHES.discard(job_key)


async def _schedule_pushes(c: ContextTypes.DEFAULT_TYPE, chat_id: int, chat_type: str):
    """Schedules the push notifications for a given chat."""
    jq = c.application.job_queue
    # Cancel prior jobs
    try:
        for name in (f"{chat_type}_hatching", f"{chat_type}_cooking", f"{chat_type}_top", f"{chat_type}_fresh"):
            for j in jq.get_jobs_by_name(name):
                j.schedule_removal()
    except Exception:
        pass

    # Recreate schedules
    if chat_id:
        prefix = chat_type
        # Standardize both public and vip per your 60s spec
        jq.run_repeating(scheduled_push_job, interval=5 * 60, first=5.0, name=f"{prefix}_hatching", data={"chat_id": chat_id, "segment": "hatching"})
        jq.run_repeating(scheduled_push_job, interval=60, first=7.0, name=f"{prefix}_cooking", data={"chat_id": chat_id, "segment": "cooking"})
        jq.run_repeating(scheduled_push_job, interval=60 * 60, first=9.0, name=f"{prefix}_top", data={"chat_id": chat_id, "segment": "top"})
        jq.run_repeating(scheduled_push_job, interval=60, first=11.0, name=f"{prefix}_fresh", data={"chat_id": chat_id, "segment": "fresh"})

async def setpublic(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Set the current chat as PUBLIC_CHAT_ID and schedule auto-pushes."""
    is_channel = (getattr(getattr(u, 'effective_chat', None), 'type', '') or '').lower() == 'channel'
    if not is_channel and (not getattr(u, 'effective_user', None) or getattr(u.effective_user, 'id', None) != OWNER_ID):
        return await safe_reply_text(u, "Only the boss can do that.")
    global PUBLIC_CHAT_ID
    chat = u.effective_chat
    if not chat:
        return await safe_reply_text(u, "Can't detect chat.")
    PUBLIC_CHAT_ID = int(chat.id)
    await _schedule_pushes(c, PUBLIC_CHAT_ID, "public")
    return await safe_reply_text(u, f"Public auto-pushes scheduled for chat {PUBLIC_CHAT_ID}.")

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

async def fresh(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    reports = await get_reports_by_tag(
        "is_fresh_candidate",
        CONFIG["FRESH_COMMAND_LIMIT"],
        cooldown,
        min_score=CONFIG.get("FRESH_MIN_SCORE_TO_SHOW", CONFIG['MIN_SCORE_TO_SHOW'])
    )
    
    if not reports:
        log.warning("/fresh: Tag search found nothing. Activating Last Resort (ignoring tags).")
        exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND final_score >= {CONFIG.get('FRESH_MIN_SCORE_TO_SHOW', CONFIG['MIN_SCORE_TO_SHOW'])}
            AND (age_minutes IS NULL OR age_minutes < 1440)
            AND mint_address NOT IN ({exclude_placeholders})
            ORDER BY last_analyzed_time DESC, final_score DESC
            LIMIT ?
        """
        params = (*cooldown, CONFIG["FRESH_COMMAND_LIMIT"])
        rows = await _execute_db(query, params, fetch='all')
        if rows:
            reports = [json.loads(row[0]) for row in rows]

    if not reports:
        await safe_reply_text(u, "– Reservoir’s dry, Tony. No top-tier fresh signals right now. ⏱️")
        return

    # Fresh header quips (general scan/guard/tooling vibe)
    header_quips = [
        "🆕 Here’s a batch of fresh ones Tony approved",
        "🆕 These just passed the safety check",
        "🆕 Fresh off the truck — clean and ready",
        "🆕 Tony signed off on this stack",
        "🆕 Couple solid builds right here",
        "🆕 Passed inspection — no rust yet",
        "🆕 Tony’s fridge picks — crisp and clean",
        "🆕 Pulled a fresh set for you",
        "🆕 New kids on the block — safe enough to sniff",
        "🆕 Tony says: these are worth a look",
    ]
    # Refresh market snapshot and recompute scores just-in-time
    refreshed = await _refresh_reports_with_latest(reports, allow_missing=True)
    log.info(f"/fresh pipeline: from_tags={len(reports)} after_refresh={len(refreshed)}")
    reports = _filter_items_for_command(refreshed, '/fresh')
    f"{pick_header_label('/fresh')} — {random.choice(header_quips)}"
    items = reports[:2]
    if not items:
        await safe_reply_text(u, "No eligible fresh tokens at the moment.")
        return
    # Override with new skeleton formatter
    final_text = build_segment_message('fresh', items, lite_mode=False)
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])

async def hatching(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    reports = await get_reports_by_tag(
        "is_hatching_candidate",
        CONFIG["HATCHING_COMMAND_LIMIT"],
        cooldown,
        min_score=CONFIG.get("HATCHING_MIN_SCORE_TO_SHOW", 0)
    )
    if not reports:
        # Last resort: query very young analyzed tokens directly (even if tags weren't set due to earlier failures)
        log.warning("/hatching: Tag search found nothing. Activating Last Resort (age-based scan).")
        exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
        age_limit = int(CONFIG.get('HATCHING_MAX_AGE_MINUTES', 30))
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND (age_minutes IS NULL OR age_minutes <= {age_limit})
            AND final_score >= {CONFIG.get('HATCHING_MIN_SCORE_TO_SHOW', 0)}
            AND mint_address NOT IN ({exclude_placeholders})
            ORDER BY last_analyzed_time DESC
            LIMIT ?
        """
        params = (*cooldown, CONFIG["HATCHING_COMMAND_LIMIT"])
        rows = await _execute_db(query, params, fetch='all')
        if rows:
            reports = [json.loads(row[0]) for row in rows]
        if not reports:
            await safe_reply_text(u, "🦉 Token's nest is empty. No brand-new, structurally sound tokens right now.")
            return
        
    # Hatching header quips (newborn/hatch theme)
    header_quips = [
        "🐣 Got a few newborns — just cracked open",
        "🐣 Fresh hatches straight from the nest",
        "🐣 Brand-new drops Tony just spotted",
        "🐣 Token and I pulled these off the line",
        "🐣 Hot from launch — here’s the hatch batch",
        "🐣 New coins in the wild — eyes on ‘em",
        "🐣 Nest is busy — fresh cracks today",
        "🐣 A handful of hatchlings for you",
        "🐣 Straight out the shell — fresh batch",
        "🐣 Don’t blink — Tony’s got hatchers",
    ]
    refreshed = await _refresh_reports_with_latest(reports, allow_missing=True)
    log.info(f"/hatching pipeline: from_tags={len(reports)} after_refresh={len(refreshed)}")
    reports = _filter_items_for_command(refreshed, '/hatching')
    f"{pick_header_label('/hatching')} — {random.choice(header_quips)}"
    items = reports[:2]
    if not items:
        await safe_reply_text(u, "No hatchlings with tradable liquidity yet.")
        return
    final_text = build_segment_message('hatching', items, lite_mode=False)
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])

async def _get_cooking_reports_command(cooldown: set) -> List[Dict[str, Any]]:
    """Collect cooking candidates with graceful fallbacks for the /cooking command.
    Order of precedence:
    1) Tagged candidates (is_cooking_candidate)
    2) Latest snapshots with high 24h volume (CONFIG['COOKING_FALLBACK_VOLUME_MIN_USD'])
    3) Recent analyzed tokens sorted by in-intel price_change_24h
    """
    # Primary: tagged
    items = await get_reports_by_tag("is_cooking_candidate", CONFIG["COOKING_COMMAND_LIMIT"], cooldown)
    if items:
        return items
    # Secondary: snapshot volume
    exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
    min_vol = float(CONFIG.get('COOKING_FALLBACK_VOLUME_MIN_USD', 200) or 200)
    query = f"""
        WITH latest AS (
            SELECT mint_address, MAX(snapshot_time) AS snapshot_time
            FROM TokenSnapshots
            GROUP BY mint_address
        )
        SELECT TL.intel_json
        FROM TokenLog TL
        JOIN latest L ON L.mint_address = TL.mint_address
        JOIN TokenSnapshots TS ON TS.mint_address = L.mint_address AND TS.snapshot_time = L.snapshot_time
        WHERE TL.status IN ('analyzed','served')
          AND TL.mint_address NOT IN ({exclude_placeholders})
          AND COALESCE(TS.volume_24h_usd, 0) >= ?
        ORDER BY TS.snapshot_time DESC, COALESCE(TS.volume_24h_usd, 0) DESC
        LIMIT ?
    """
    params = (*cooldown, float(min_vol), CONFIG["COOKING_COMMAND_LIMIT"]) if cooldown else (float(min_vol), CONFIG["COOKING_COMMAND_LIMIT"])
    rows = await _execute_db(query, params, fetch='all')
    items = [json.loads(row[0]) for row in rows] if rows else []
    if items:
        return items
    # Tertiary: recent analyzed sorted by in-intel price change
    query2 = f"""
        SELECT intel_json FROM TokenLog
        WHERE status IN ('analyzed','served')
          AND mint_address NOT IN ({exclude_placeholders})
        ORDER BY last_analyzed_time DESC
        LIMIT 50
    """
    params2 = (*cooldown,) if cooldown else ()
    rows2 = await _execute_db(query2, params2, fetch='all')
    if not rows2:
        return []
    pool = [json.loads(r[0]) for r in rows2]
    pool.sort(key=lambda x: float(x.get('price_change_24h') or 0), reverse=True)
    return pool[:CONFIG["COOKING_COMMAND_LIMIT"]]

async def cooking(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    reports = await _get_cooking_reports_command(cooldown)
    if not reports:
        await safe_reply_text(u, "🍳 Stove's cold. Nothing showing significant momentum right now.")
        return
    
    # Cooking header quips (heat/cooking theme)
    header_quips = [
        "🍳 Got a few sizzling right now",
        "🍳 These ones are cooking hot",
        "🍳 Momentum’s rising across this batch",
        "🍳 Tony’s grill has a couple popping",
        "🍳 Here’s a pan full of movers",
        "🍳 These drops are smoking fast",
        "🍳 Couple hot picks — handle with mitts",
        "🍳 Tony says: fire under all of these",
        "🍳 The skillet’s crowded — crackling picks",
        "🍳 Burning quick — keep eyes sharp",
    ]
    refreshed = await _refresh_reports_with_latest(reports, allow_missing=True)
    log.info(f"/cooking pipeline: from_tags={len(reports)} after_refresh={len(refreshed)}")
    reports = _filter_items_for_command(refreshed, '/cooking')
    f"{pick_header_label('/cooking')} — {random.choice(header_quips)}"
    items = reports[:2]
    if not items:
        await safe_reply_text(u, "No eligible cooking tokens after filters.")
        return
    final_text = build_segment_message('cooking', items, lite_mode=False)
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])

async def top(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    
    exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
    query = f"""
        SELECT intel_json FROM TokenLog
        WHERE status IN ('analyzed','served')
        AND mint_address NOT IN ({exclude_placeholders})
        AND final_score >= {CONFIG['MIN_SCORE_TO_SHOW']}
        ORDER BY final_score DESC
        LIMIT ?
    """
    params = (*cooldown, CONFIG["TOP_COMMAND_LIMIT"])
    rows = await _execute_db(query, params, fetch='all')
    
    if not rows:
        await safe_reply_text(u, "– Nothin' but crickets. The pot's a bit thin right now, check back later. 🦗")
        return

    # Pull a bit more than we will display to allow post-refresh filtering/sorting
    more_params = (*cooldown, max(CONFIG["TOP_COMMAND_LIMIT"] * 5, CONFIG["TOP_COMMAND_LIMIT"]))
    rows_more = await _execute_db(query, more_params, fetch='all')
    reports = [json.loads(row[0]) for row in (rows_more or rows)]
    # Top header quips (leaderboard theme)
    top_quips = [
        "🏆 Tony’s proud picks — strongest of the bunch",
        "🏆 Here’s today’s winners’ circle",
        "🏆 Top shelf coins — only the best made it",
        "🏆 These few passed every test",
        "🏆 Tony’s shortlist — solid crew",
        "🏆 Couple standouts worth your time",
        "🏆 These are the cream of the crop",
        "🏆 Tony and Token hand-picked these",
        "🏆 Best of today — no slackers",
        "🏆 Tony says: these are built to last",
    ]
    f"{pick_header_label('/top')} — {random.choice(top_quips)}"
    refreshed = await _refresh_reports_with_latest(reports)
    log.info(f"/top pipeline: from_db={len(reports)} after_refresh={len(refreshed)}")
    reports = refreshed
    # Filter out obviously rugged/non-tradable and illiquid
    min_liq = float(CONFIG.get("MIN_LIQUIDITY_FOR_HATCHING", 100) or 100)
    filtered = []
    for j in reports:
        liq_raw = j.get("liquidity_usd", None)
        liq = None
        try:
            if liq_raw is not None:
                liq = float(liq_raw)
        except Exception:
            liq = None
        rug_txt = str(j.get("rugcheck_score") or "")
        # Enforce min liquidity only when we have a numeric value; unknown liquidity passes this check
        if liq is not None and liq < min_liq:
            continue
        if "High Risk" in rug_txt:
            continue
        filtered.append(j)
    # Filter out low scores (no 'DANGER' in /top)
    filtered = [j for j in filtered if int(j.get('score', 0) or 0) >= 40]
    # Apply global no-zero-liq rule for lists
    filtered = _filter_items_for_command(filtered, '/top')
    # Sort by freshly recomputed score, highest first
    filtered.sort(key=lambda x: int(x.get("score", 0) or 0), reverse=True)
    items = filtered[:CONFIG["TOP_COMMAND_LIMIT"]]
    if not items:
        await safe_reply_text(u, "No eligible top tokens after filters.")
        return
    final_text = build_segment_message('top', items, lite_mode=False)
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])

async def check(u: Update, c: ContextTypes.DEFAULT_TYPE):
    # Robustly extract text from any update type (DM, group, channel)
    try:
        text = (getattr(getattr(u, 'effective_message', None), 'text', '') or '').strip()
    except Exception:
        text = ''
    # Encourage DM-only deep checks to avoid exposing details in groups
    try:
        if await _safe_is_group(u) and u.effective_user.id != OWNER_ID:
            return await safe_reply_text(u, "For privacy, run /check in DM with me.")
    except Exception:
        pass
    # Ensure any old ReplyKeyboard is removed (Telegram persists it otherwise)
    await safe_reply_text(u, "Running a quick scan... I’ll follow up with extras.", quote=True, reply_markup=ReplyKeyboardRemove())
    await _maybe_send_typing(u)
    try:
        client = await get_http_client()
        mint_address = await extract_mint_from_check_text(client, text)
        if not mint_address:
            return await safe_reply_text(u, "Give me a Solana token mint, pair link, or token URL, boss!")
        intel = await enrich_token_intel(client, mint_address, deep_dive=False)
        
        if not intel: return await safe_reply_text(u, "Couldn't find hide nor hair of that one. Bad address or no data.")
        
        # Header line like other commands
        check_quips = [
            "🔍 Tony put this one on the bench — full breakdown",
            "🔍 Here’s the inspection report",
            "🔍 Tony pulled it apart — no shortcuts",
            "🔍 Token double-checked the details",
            "🔍 Rugcheck complete — truth below",
            "🔍 Tony says: under the hood now",
            "🔍 Every gauge read — log below",
            "🔍 Inspection done — nothing hidden",
            "🔍 Tony left no gaps — all here",
            "🔍 Report delivered — raw and clear",
        ]
        header_line = f"{pick_header_label('/check')} — {random.choice(check_quips)}"
        report_text = build_full_report2(intel, include_links=True)
        final_text = header_line + "\n\n" + report_text
        # Send initial response quickly
        sent_msg = await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

        # Follow-up background enrichment (Bitquery/Twitter + chart)
        async def _follow_up_enrichment():
            try:
                deep = await enrich_token_intel(client, mint_address, deep_dive=True)
                if not deep:
                    return
                new_text = header_line + "\n\n" + build_full_report2(deep, include_links=True)
                try:
                    await u.get_bot().edit_message_text(chat_id=sent_msg.chat_id, message_id=sent_msg.message_id, text=new_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                except Exception as e_edit:
                    log.debug(f"/check edit fallback: {e_edit}")
                    await safe_reply_text(u, new_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                try:
                    photo_content2 = await fetch_dexscreener_chart(deep.get('pair_address'))
                    if photo_content2:
                        await safe_reply_photo(u, photo=photo_content2)
                except Exception as e_photo:
                    log.debug(f"/check chart send failed: {e_photo}")
            except Exception as e2:
                log.debug(f"/check follow-up enrichment failed: {e2}")

        try:
            asyncio.create_task(_follow_up_enrichment())
        except Exception as e_bg:
            log.debug(f"/check: could not schedule follow-up: {e_bg}")
    except Exception as e:
        log.error(f"Error in /check for text '{text}': {e}", exc_info=True)
        await safe_reply_text(u, "💀 Tony’s tools are jammed. Can't get a read on that one right now.")

async def start(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message when the /start command is issued."""
    await u.message.reply_text('Hi! I am Token Tony. Send me a command to get started.')

async def ping(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a pong message when the /ping command is issued."""
    await u.message.reply_text('Pong!')

async def set_config(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    """Sets a configuration value."""
    await u.message.reply_text('This command is not yet implemented.')


async def voice(u: Update, c: ContextTypes.DEFAULT_TYPE) -> None:
    """Toggle Tony's voice presets for AI explanations and fallbacks."""
    user_id = getattr(getattr(u, "effective_user", None), "id", None)
    if user_id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can change Tony's voice.")

    args = list(getattr(c, "args", []) or [])
    presets = list_voice_presets()
    current_key = get_current_voice()
    current_profile = get_voice_profile()
    current_label = get_voice_label()
    current_description = current_profile.get("description", "")

    if not args:
        options = "\n".join(
            f"• <code>{key}</code> — {desc}" for key, desc in presets.items()
        ) or "(no presets configured)"
        message = (
            "🎙️ Tony's voice is currently "
            f"<b>{current_label}</b> (<code>{current_key}</code>).\n"
            f"{current_description}\n\n"
            "Available presets:\n"
            f"{options}\n\n"
            "Use <code>/voice &lt;preset&gt;</code> to switch or <code>/voice toggle</code> to cycle."
        )
        return await safe_reply_text(
            u, message, parse_mode=ParseMode.HTML, disable_web_page_preview=True
        )

    choice = args[0].strip().lower()
    try:
        if choice in {"toggle", "next"}:
            profile = cycle_voice_preset()
            new_key = get_current_voice()
        else:
            target_key = None
            if choice in presets:
                target_key = choice
            else:
                for key in presets:
                    if key.startswith(choice):
                        target_key = key
                        break
            if not target_key:
                raise KeyError(choice)
            profile = set_voice_preset(target_key)
            new_key = target_key
    except KeyError:
        options = "\n".join(
            f"• <code>{key}</code> — {desc}" for key, desc in presets.items()
        ) or "(no presets configured)"
        return await safe_reply_text(
            u,
            "Unknown voice preset.\n\nAvailable presets:\n" + options,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    label = profile.get("label", new_key)
    description = profile.get("description", presets.get(new_key, ""))
    log.info(f"🎙️ Voice preset updated to %s", new_key)
    message = (
        f"🎙️ Voice preset changed to <b>{label}</b> (<code>{new_key}</code>).\n"
        f"{description}\n\nTony will use this tone for AI prompts and quips immediately."
    )
    return await safe_reply_text(
        u, message, parse_mode=ParseMode.HTML, disable_web_page_preview=True
    )

async def diag(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Tony's comprehensive diagnostic report - everything you need to know."""
    await _maybe_send_typing(u)
    
    status_lines = ["🔧 **Tony's Full System Diagnostic**\n"]
    
    # Tony's config sanity check
    status_lines.append("**📋 Configuration Status:**")
    db_path = CONFIG.get('DB_FILE', 'data/tony_memory.db')
    log_path = CONFIG.get('TONY_LOG_FILE', 'data/tony_log.log')
    
    # Check file system access
    try:
        import os
        db_writable = os.access(os.path.dirname(db_path), os.W_OK) if os.path.exists(os.path.dirname(db_path)) else False
        log_writable = os.access(os.path.dirname(log_path), os.W_OK) if os.path.exists(os.path.dirname(log_path)) else False
    except Exception:
        db_writable = log_writable = False
    
    status_lines.append(f"• Database: `{db_path}` {'✅' if db_writable else '❌'}")
    status_lines.append(f"• Log file: `{log_path}` {'✅' if log_writable else '❌'}")
    status_lines.append(f"• Aggregator interval: {CONFIG.get('AGGREGATOR_POLL_INTERVAL_MINUTES', 1)}min")
    status_lines.append(f"• Re-analyzer batch: {CONFIG.get('RE_ANALYZER_BATCH_LIMIT', 40)}")
    status_lines.append(f"• Command cooldown: {CONFIG.get('COMMAND_COOLDOWN_HOURS', 12)}h")
    
    # Tony's API key inventory
    status_lines.append("\n**🔑 API Arsenal:**")
    status_lines.append(f"• Telegram: {'✅' if TELEGRAM_TOKEN else '❌'}")
    status_lines.append(f"• Helius: {'✅' if HELIUS_API_KEY else '❌'}")
    status_lines.append(f"• BirdEye: {'✅' if BIRDEYE_API_KEY else '❌'}")
    status_lines.append(f"• Gemini AI: {'✅' if os.getenv('GEMINI_API_KEY') else '❌'}")

    # Tony's voice preset overview
    try:
        voice_profile = get_voice_profile()
        voice_label = get_voice_label()
        status_lines.append("\n**🎙️ Voice Preset:**")
        status_lines.append(
            f"• Current tone: {voice_label} (`{get_current_voice()}`)"
        )
        description = voice_profile.get("description")
        if description:
            status_lines.append(f"• Flavor: {description}")
    except Exception as e:
        status_lines.append(f"\n**🎙️ Voice Preset:** Error - {e}")

    # Tony's API health monitoring
    status_lines.append("\n**🌐 API Health Status:**")
    for provider, stats in API_HEALTH.items():
        total = stats['success'] + stats['failure']
        if total > 0:
            success_rate = (stats['success'] / total) * 100
            circuit_status = "🔴 OPEN" if stats['circuit_open'] else "🟢 CLOSED"
            last_success = stats.get('last_success', 0)
            age = int(time.time() - last_success) if last_success else 999999
            age_str = f"{age}s ago" if age < 3600 else f"{age//3600}h ago" if age < 86400 else "old"
            status_lines.append(f"• {provider.title()}: {success_rate:.1f}% success, circuit {circuit_status}, last success {age_str}")
        else:
            status_lines.append(f"• {provider.title()}: No requests yet")
    
    # Tony's lite mode status
    if LITE_MODE_UNTIL > time.time():
        remaining = int(LITE_MODE_UNTIL - time.time())
        status_lines.append(f"\n⚠️ **Lite Mode Active** ({remaining}s remaining)")
        status_lines.append("*Tony's being conservative due to API issues*")
    
    # Tony's AI brain status
    try:
        from ai_router import get_ai_health_status
        ai_status = get_ai_health_status()
        status_lines.append("\n**🤖 AI Brain Status:**")
        status_lines.append(f"• Gemini configured: {'✅' if ai_status['gemini_configured'] else '❌'}")
        status_lines.append(f"• Explanation cache: {ai_status['cache_size']} entries")
        if ai_status.get('cache_hits', 0) + ai_status.get('cache_misses', 0) > 0:
            hit_rate = ai_status['cache_hits'] / (ai_status['cache_hits'] + ai_status['cache_misses']) * 100
            status_lines.append(f"• Cache hit rate: {hit_rate:.1f}%")
    except Exception as e:
        status_lines.append(f"\n**🤖 AI Brain Status:** Error - {e}")
    
    # Tony's queue monitoring
    try:
        discovered_count = await _execute_db("SELECT COUNT(*) FROM TokenLog WHERE status = 'discovered'", fetch='one')
        analyzing_count = await _execute_db("SELECT COUNT(*) FROM TokenLog WHERE status = 'analyzing'", fetch='one')
        analyzed_count = await _execute_db("SELECT COUNT(*) FROM TokenLog WHERE status = 'analyzed'", fetch='one')
        served_count = await _execute_db("SELECT COUNT(*) FROM TokenLog WHERE status = 'served'", fetch='one')
        
        status_lines.append("\n**📊 Tony's Queue Status:**")
        status_lines.append(f"• Discovered: {discovered_count[0] if discovered_count else 0}")
        status_lines.append(f"• Analyzing: {analyzing_count[0] if analyzing_count else 0}")
        status_lines.append(f"• Analyzed: {analyzed_count[0] if analyzed_count else 0}")
        status_lines.append(f"• Served: {served_count[0] if served_count else 0}")
    except Exception as e:
        status_lines.append(f"\n❌ Queue status error: {e}")
    
    # Tony's firehose monitoring
    status_lines.append("\n**🔥 Data Firehose Status:**")
    for source, status in FIREHOSE_STATUS.items():
        status_lines.append(f"• {source}: {status}")
    if provider_state:
        status_lines.append("\n**📡 Provider Health:**")
        now = time.time()
        for provider, stats in provider_state.items():
            last_success = stats.get("last_success") or 0
            if last_success:
                age = int(now - last_success)
                if age < 60:
                    last_success_str = f"{age}s ago"
                elif age < 3600:
                    last_success_str = f"{age // 60}m ago"
                elif age < 86400:
                    last_success_str = f"{age // 3600}h ago"
                else:
                    last_success_str = "stale"
            else:
                last_success_str = "never"
            last_failure = stats.get("last_failure") or 0
            if last_failure:
                fail_age = int(now - last_failure)
                if fail_age < 60:
                    last_failure_str = f"{fail_age}s ago"
                elif fail_age < 3600:
                    last_failure_str = f"{fail_age // 60}m ago"
                elif fail_age < 86400:
                    last_failure_str = f"{fail_age // 3600}h ago"
                else:
                    last_failure_str = "stale"
            else:
                last_failure_str = "never"
            failures = stats.get("consecutive_failures", 0)
            msg_total = stats.get("messages_received", 0)
            backoff = int(stats.get("current_backoff") or 0)
            parts = [
                f"• {provider}: {msg_total} msgs",
                f"last success {last_success_str}",
                f"consecutive failures {failures}",
            ]
            if last_failure_str != "never":
                parts.append(f"last failure {last_failure_str}")
            if failures:
                parts.append(f"backoff {backoff}s")
            if stats.get("last_error"):
                err = stats["last_error"]
                if len(err) > 80:
                    err = err[:77] + "..."
                parts.append(f"error `{err}`")
            status_lines.append(
                ", ".join(parts)
            )

    # Tony's bucket distribution
    try:
        bucket_query = """
            SELECT enhanced_bucket, COUNT(*) 
            FROM TokenLog 
            WHERE status IN ('analyzed', 'served') 
            AND enhanced_bucket IS NOT NULL
            GROUP BY enhanced_bucket
            ORDER BY COUNT(*) DESC
        """
        bucket_rows = await _execute_db(bucket_query, fetch='all')
        if bucket_rows:
            status_lines.append("\n**🪣 Token Buckets:**")
            for bucket, count in bucket_rows:
                status_lines.append(f"• {bucket}: {count}")
    except Exception as e:
        log.warning(f"Bucket stats error: {e}")
    
    # Tony's push status
    status_lines.append("\n**📢 Push Status:**")
    status_lines.append(f"• Active pushes: {len(ACTIVE_PUSHES)}")
    status_lines.append(f"• Failed pushes: {len(PUSH_FAILURES)}")
    if PUSH_FAILURES:
        for key, (last_fail, count) in list(PUSH_FAILURES.items())[:3]:
            age = int(time.time() - last_fail)
            status_lines.append(f"  - {key}: {count} failures, last {age}s ago")
    
    # Tony's performance metrics
    try:
        if hasattr(globals(), 'recent_processing_times') and recent_processing_times:
            import statistics
            avg_time = statistics.mean(recent_processing_times)
            status_lines.append("\n**⚡ Performance:**")
            status_lines.append(f"• Avg processing time: {avg_time:.1f}s")
            status_lines.append(f"• Current batch size: {adaptive_batch_size}")
    except Exception:
        pass
    
    report = "\n".join(status_lines)
    
    # Split if too long for Telegram
    if len(report) > 4000:
        parts = [report[i:i+4000] for i in range(0, len(report), 4000)]
        for i, part in enumerate(parts):
            if i == 0:
                await u.message.reply_text(part, parse_mode=ParseMode.MARKDOWN)
            else:
                await u.message.reply_text(f"**Diagnostic Report (Part {i+1}):**\n\n{part}", parse_mode=ParseMode.MARKDOWN)
    else:
        await u.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)

def log_startup_config():
    """Tony's startup config summary - he likes to know what he's working with."""
    log.info("🚀 Tony's Configuration Summary:")
    log.info(f"  Database: {CONFIG.get('DB_FILE', 'data/tony_memory.db')}")
    log.info(f"  Log file: {CONFIG.get('TONY_LOG_FILE', 'data/tony_log.log')}")
    log.info(f"  Aggregator interval: {CONFIG.get('AGGREGATOR_POLL_INTERVAL_MINUTES', 1)}min")
    log.info(f"  Re-analyzer batch: {CONFIG.get('RE_ANALYZER_BATCH_LIMIT', 40)}")
    log.info(f"  API Keys: Helius={'✓' if HELIUS_API_KEY else '✗'}, BirdEye={'✓' if BIRDEYE_API_KEY else '✗'}, Gemini={'✓' if os.getenv('GEMINI_API_KEY') else '✗'}")
    log.info(f"  Chat IDs: Public={PUBLIC_CHAT_ID or 'None'}, VIP={VIP_CHAT_ID or 'None'}")
    log.info(f"  Performance: Adaptive batching={'✓' if CONFIG.get('ADAPTIVE_BATCH_SIZE') else '✗'}")

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

async def pre_shutdown(app: Application) -> None:
    """Gracefully cancel all running background tasks before shutdown."""
    log.info("Initiating graceful shutdown. Canceling background tasks...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if not tasks:
        return
    log.info(f"Canceling {len(tasks)} background tasks...")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("All background tasks canceled. Shutdown complete.")
    # Close shared HTTP client
    try:
        global _HTTP_CLIENT, _HTTP_CLIENT_DS
        if _HTTP_CLIENT is not None:
            await _HTTP_CLIENT.aclose()
            _HTTP_CLIENT = None
        if _HTTP_CLIENT_DS is not None:
            await _HTTP_CLIENT_DS.aclose()
            _HTTP_CLIENT_DS = None
    except Exception as e:
        log.debug(f"HTTP client close error: {e}")

async def seed(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Owner-only: seed one or more mints into the discovery queue for testing."""
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    text = (u.message.text or "").strip()
    mints = text.split()[1:]
    for m in mints[:10]:
        asyncio.create_task(process_discovered_token(m))
    await safe_reply_text(u, f"Queued {len(mints[:10])} mint(s) for discovery.")

async def dbprune(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    days_snap = int(CONFIG.get("SNAPSHOT_RETENTION_DAYS", 14))
    days_rej = int(CONFIG.get("REJECTED_RETENTION_DAYS", 7))
    await safe_reply_text(u, f"Pruning snapshots >{days_snap}d and rejected >{days_rej}d...")
    ok = await _db_prune(days_snap, days_rej)
    try:
        await _execute_db("PRAGMA wal_checkpoint(TRUNCATE)", commit=True)
    except Exception:
        pass
    await safe_reply_text(u, "DB prune complete." if ok else "DB prune encountered an error.")

async def dbpurge(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    text = (u.message.text or "").strip()
    if not text.lower().endswith("confirm"):
        return await safe_reply_text(u, "This erases all state. Run /dbpurge confirm to proceed.")
    await safe_reply_text(u, "Purging all state and vacuuming DB...")
    await _db_purge_all()
    await safe_reply_text(u, "All state wiped. Fresh start.")
    await _execute_db("INSERT OR REPLACE INTO KeyValueStore (key, value) VALUES (?, ?)", ('last_purge_time', datetime.now(timezone.utc).isoformat()), commit=True)

async def dbclean(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    quips = [
        "🧹 Tony swept the floor — cleanup done",
        "🧹 Database clear — junk’s gone",
        "🧹 Garage tidy again",
        "🧹 Old scraps tossed",
        "🧹 Tony likes a clean shop",
        "🧹 Prune finished — DB fresh",
        "🧹 Nothing left but the good stuff",
        "🧹 Workshop spotless",
        "🧹 Clutter cleared",
        "🧹 Tony says: floor’s clean, back to work",
    ]
    await safe_reply_text(u, wrap_with_segment_header('dbclean', random.choice(quips)))
    days_snap = int(CONFIG.get("SNAPSHOT_RETENTION_DAYS", 14))
    days_rej = int(CONFIG.get("REJECTED_RETENTION_DAYS", 7))
    ok = await _db_prune(days_snap, days_rej)
    try:
        await _execute_db("PRAGMA wal_checkpoint(TRUNCATE)", commit=True)
    except Exception:
        pass
    await safe_reply_text(u, wrap_with_segment_header('dbclean', "DB cleaned." if ok else "DB clean encountered an error."))

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

async def shutdown_handler(app: Application):
    """Enhanced shutdown handler with proper cleanup."""
    log.info("🛑 Token Tony shutting down...")
    try:
        # Cancel all background tasks
        tasks = [t for t in asyncio.all_tasks() if not t.done()]
        if tasks:
            log.info(f"Cancelling {len(tasks)} background tasks...")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Close HTTP clients
        if hasattr(app, '_http_clients'):
            for client in app._http_clients.values():
                try:
                    await client.aclose()
                except Exception as e:
                    log.debug(f"Error closing HTTP client: {e}")
        
        log.info("✅ Shutdown complete")
    except Exception as e:
        log.error(f"Error during shutdown: {e}")

async def post_init(app: Application) -> None:
    """Runs async setup and starts background workers after the bot is initialized."""
    await setup_database()
    load_advanced_quips()
    # Config sanity summary at startup
    try:
        global CONFIG_SANITY
        CONFIG_SANITY = compute_config_sanity()
        log.info(
            "Config Sanity: "
            + f"DB={'OK' if CONFIG_SANITY.get('db_writable') else 'FAIL'}, "
            + f"LOG={'OK' if CONFIG_SANITY.get('log_writable') else 'FAIL'}, "
            + f"HELIUS={'OK' if CONFIG_SANITY.get('helius_api') else 'MISSING'}, "
            + f"BIRDEYE={'OK' if CONFIG_SANITY.get('birdeye_api') else 'MISSING'}, "
            + f"DEGRADED={'YES' if CONFIG_SANITY.get('degraded_mode') else 'no'}"
        )
    except Exception as e:
        log.warning(f"Config sanity check failed: {e}")
    
    log.info("✅ Blueprint Engine: Firing up background workers...")
    # Using PumpPortal WS (single socket). Skip client-api.* pump.fun endpoints entirely.
    # Single-socket streams (keep counts low to avoid upstream limits)
    app.create_task(pumpportal_worker(), name="PumpPortalWS") # Tony's discovery worker
    # Use logsSubscribe-based firehose across providers (if configured)
    app.create_task(logs_firehose_worker(), name="LogsFirehoseWorker")
    app.create_task(aggregator_poll_worker(), name="AggregatorPollWorker")
    app.create_task(second_chance_worker(), name="SecondChanceWorker")
    app.create_task(process_discovery_queue(), name="EnhancedProcessingWorker")
    app.create_task(re_analyzer_worker(), name="ReAnalyzerWorker")
    app.create_task(maintenance_worker(), name="MaintenanceWorker")
    app.create_task(circuit_breaker_reset_worker(), name="CircuitBreakerResetWorker")

    if not all([BIRDEYE_API_KEY, HELIUS_API_KEY]):
        log.warning("One or more critical API keys (Helius, Birdeye) are missing. Analysis quality will be degraded.")
        FIREHOSE_STATUS.clear()
        FIREHOSE_STATUS["System"] = "🔴 Missing API Key(s)"

    # Schedule Public/VIP push cadences if chat IDs provided

    def _sched_repeating(name: str, secs: int, chat_id: int, segment: str, delay: float = 5.0):
        """Enhanced scheduling with validation and conflict prevention."""
        if not chat_id:
            log.warning(f"Skipping {name}: no chat_id provided")
            return
        # Remove existing job if present to prevent duplicates
        existing_jobs = [job for job in jq.jobs() if job.name == name]
        for job in existing_jobs:
            job.schedule_removal()
            log.info(f"Removed existing job: {name}")
        jq.run_repeating(
            scheduled_push_job,
            interval=secs,
            first=delay + random.uniform(0, 5.0),
            name=name,
            data={"chat_id": chat_id, "segment": segment},
        )
        log.info(f"Scheduled {name} every {secs}s for chat {chat_id} (segment: {segment})")

    jq = app.job_queue

    # Public cadence - only if bot has rights to post
    if PUBLIC_CHAT_ID:
        ok, reason = await _can_post_to_chat(app.bot, PUBLIC_CHAT_ID)
        if ok:
            _sched_repeating("public_hatching", 5 * 60, PUBLIC_CHAT_ID, "hatching")
            _sched_repeating("public_cooking", 60, PUBLIC_CHAT_ID, "cooking") # User request: 60s
            _sched_repeating("public_top", 60 * 60, PUBLIC_CHAT_ID, "top")
            # Continuous fresh cadence every 60 seconds
            _sched_repeating("public_fresh", 60, PUBLIC_CHAT_ID, "fresh")
        else:
            log.error(f"PUBLIC_CHAT_ID={PUBLIC_CHAT_ID} is not writable: {reason}. Auto-pushes not scheduled.")
            await _notify_owner(app.bot, f"<b>Setup required:</b> Bot lacks post rights for PUBLIC chat <code>{PUBLIC_CHAT_ID}</code> ({reason}).\nAdd the bot as <b>Admin</b> in the channel and re-run /setpublic here or restart.")

    # VIP cadence - only if bot has rights to post
    if VIP_CHAT_ID:
        ok, reason = await _can_post_to_chat(app.bot, VIP_CHAT_ID)
        if ok:
            _sched_repeating("vip_hatching", 2 * 60, VIP_CHAT_ID, "hatching")
            _sched_repeating("vip_cooking", 60, VIP_CHAT_ID, "cooking") # User request: 60s
            _sched_repeating("vip_top", 20 * 60, VIP_CHAT_ID, "top")
            # Continuous fresh cadence every 60 seconds
            _sched_repeating("vip_fresh", 60, VIP_CHAT_ID, "fresh")
        else:
            log.error(f"VIP_CHAT_ID={VIP_CHAT_ID} is not writable: {reason}. Auto-pushes not scheduled.")
            await _notify_owner(app.bot, f"<b>Setup required:</b> Bot lacks post rights for VIP chat <code>{VIP_CHAT_ID}</code> ({reason}).\nAdd the bot as <b>Admin</b> in the channel and re-run /setvip here or restart.")

    # Weekly maintenance: Sunday 03:30 UTC — VACUUM + WAL truncate + log cleanup
    async def weekly_maintenance_job(context: ContextTypes.DEFAULT_TYPE):
        try:
            await _execute_db("PRAGMA wal_checkpoint(TRUNCATE)", commit=True)
        except Exception:
            pass
        try:
            await _execute_db("VACUUM", commit=True)
        except Exception:
            pass
        try:
            removed, kept = _cleanup_logs()
            log.info(f"Weekly maintenance: removed {removed} logs, kept {kept} latest.")
        except Exception:
            pass

    try:
        jq.run_daily(weekly_maintenance_job, time=dtime(3, 30, tzinfo=timezone.utc), days=(6,), name="WeeklyMaintenance")
        log.info("Scheduled weekly maintenance job (Sun 03:30 UTC).")
    except Exception as e:
        log.warning(f"Failed to schedule weekly maintenance: {e}")
def main() -> None:
    """Tony's main function - bulletproof startup with full validation."""
    
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
        ("voice", voice),
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
                'voice': voice,
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

    from telegram.ext import MessageHandler
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

if __name__ == "__main__":
    main()
