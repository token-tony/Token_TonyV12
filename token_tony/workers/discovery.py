# -*- coding: utf-8 -*-
"""Discovery workers for Token Tony."""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections import deque
from typing import Any, Dict, List, Optional, Set, Tuple

import websockets

from config import (
    ALCHEMY_WS_URL,
    CONFIG,
    DEX_PROGRAMS_FOR_FIREHOSE,
    GECKO_API_URL,
    HELIUS_API_KEY,
    HELIUS_WS_URL,
    KNOWN_QUOTE_MINTS,
    PRIMARY_FIREHOSE_PROGRAMS,
    BACKUP_FIREHOSE_PROGRAMS,
    SYNDICA_WS_URL,
)
from http_client import get_http_client, _fetch
from rpc_providers import RPC_PROVIDERS, rpc_post
from token_tony.analysis import (
    DS_NEW_CACHE,
    GECKO_SEARCH_CACHE,
    POOL_BIRTH_CACHE,
    process_discovered_token,
)
from token_tony.utils.solana import is_valid_solana_address

log = logging.getLogger(__name__)

_PROCESSED_SIGNATURE_LIMIT = int(CONFIG.get("FIREHOSE_SIGNATURE_CACHE", 8000) or 8000)
_processed_signatures = deque()
_processed_signature_set: Set[str] = set()

DISCOVERY_BUCKET = TokenBucket(capacity=8, refill_amount=8, interval_seconds=1.0)

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

def _remember_signature(signature: str) -> None:
    _processed_signatures.append(signature)
    _processed_signature_set.add(signature)
    if len(_processed_signatures) > _PROCESSED_SIGNATURE_LIMIT:
        old = _processed_signatures.popleft()
        _processed_signature_set.discard(old)

def _signature_already_processed(signature: str) -> bool:
    return signature in _processed_signature_set


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


async def _fetch_transaction_via_rpc(signature: str) -> Optional[Dict[str, Any]]:
    """Fetch a transaction using the RPC failover pool."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0},
        ],
    }
    response = await rpc_post(payload)
    return (response or {}).get("result") if response else None

def _extract_mints_from_tx_result(tx_result: Dict[str, Any]) -> List[str]:
    """Best-effort extraction of base/quote mints from a transaction result."""
    mints: set = set()
    meta = tx_result.get("meta") or {}
    for bal in meta.get("postTokenBalances", []) + meta.get("preTokenBalances", []):
        if mint := bal.get("mint"):
            mints.add(mint)
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
    filtered = [m for m in mints if m not in KNOWN_QUOTE_MINTS]
    return filtered[:4]

POOL_BIRTH_KEYWORDS = {"createpool", "initializepool", "initialize_pool", "pool-init", "open_pool", "initialize2"}

async def _logs_subscriber(provider_name: str, ws_url: str, program_keys: Tuple[str, ...], *, is_primary: bool):
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
    base_backoff = 10
    while True:
        try:
            FIREHOSE_STATUS[key] = "?? Connecting"
            log.info(f"Logs Firehose ({provider_name}): Connecting {ws_url} ...")
            async with websockets.connect(ws_url, ping_interval=55) as websocket:
                subscriptions: List[int] = []
                for program_name in program_keys:
                    program = DEX_PROGRAMS_FOR_FIREHOSE.get(program_name)
                    if not program:
                        log.debug(f"Logs Firehose ({provider_name}): program {program_name} not configured, skipping")
                        continue
                    sub = {
                        "jsonrpc": "2.0",
                        "id": random.randint(1000, 999999),
                        "method": "logsSubscribe",
                        "params": [{"mentions": [program["program_id"]]}, {"commitment": "processed"}],
                    }
                    await websocket.send(json.dumps(sub))
                    subscriptions.append(sub["id"])
                if not subscriptions:
                    FIREHOSE_STATUS[key] = "?? Idle"
                    log.warning(f"Logs Firehose ({provider_name}): no programs subscribed; sleeping 30s")
                    await asyncio.sleep(30)
                    continue
                state["consecutive_failures"] = 0
                state["current_backoff"] = 0.0
                state["last_success"] = time.time()
                state["last_error"] = ""
                FIREHOSE_STATUS[key] = "?? Connected"
                log.info(f"? Logs Firehose ({provider_name}): Subscribed to {len(subscriptions)} programs.")
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
                    if not is_primary and _primary_is_healthy():
                        continue
                    if _signature_already_processed(signature):
                        continue
                    logs_list = (result.get("value", {}).get("logs") or [])
                    logs_text = "\n".join(logs_list).lower()
                    if not any(k in logs_text for k in POOL_BIRTH_KEYWORDS):
                        continue
                    try:
                        tx_res = await _fetch_transaction_via_rpc(signature)
                    except Exception as exc:
                        log.warning(
                            "Logs Firehose (%s): RPC lookup for %s failed (%s)",
                            provider_name,
                            signature,
                            exc,
                        )
                        continue
                    if not tx_res:
                        continue
                    _remember_signature(signature)
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
                        log.info(
                            f"Logs Firehose ({provider_name}): discovered candidate mint {mint} from signature {signature}",
                        )
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
            FIREHOSE_STATUS[key] = f"?? Error: {e.__class__.__name__} (retry in {int(backoff)}s)"
            log.error(
                "Logs Firehose (%s): connection failed after %s consecutive errors: %s. Retrying in %ss...",
                provider_name,
                state["consecutive_failures"],
                e,
                int(backoff),
            )
            await asyncio.sleep(backoff)


def _primary_is_healthy() -> bool:
    # Placeholder for actual health check logic
    return True

async def logs_firehose_worker():
    """Start logsSubscribe firehose across configured providers (Helius/Syndica/Alchemy)."""
    providers: List[Tuple[str, str, Tuple[str, ...], bool]] = []
    if HELIUS_API_KEY and HELIUS_WS_URL:
        providers.append((PRIMARY_PROVIDER_NAME, HELIUS_WS_URL, PRIMARY_FIREHOSE_PROGRAMS, True))
    enable_backups = bool(CONFIG.get("ENABLE_BACKUP_STREAMS", False))
    if enable_backups and SYNDICA_WS_URL:
        providers.append(("Syndica", SYNDICA_WS_URL, BACKUP_FIREHOSE_PROGRAMS, False))
    if enable_backups and ALCHEMY_WS_URL:
        providers.append(("Alchemy", ALCHEMY_WS_URL, BACKUP_FIREHOSE_PROGRAMS, False))
    if not providers:
        log.warning("Logs Firehose disabled: no provider URLs configured (HELIUS/SYNDICA/ALCHEMY).")
        return
    if not RPC_PROVIDERS:
        log.warning("Logs Firehose disabled: no HTTP RPC providers configured for transaction lookups.")
        return
    tasks = []
    for name, ws, programs, is_primary in providers:
        if not programs:
            continue
        tasks.append(_logs_subscriber(name, ws, programs, is_primary=is_primary))
    if not tasks:
        log.warning("Logs Firehose disabled: no valid program subscriptions configured.")
        return
    log.info(f"Logs Firehose: launching {len(tasks)} provider workers...")
    await asyncio.gather(*tasks)

async def discover_from_gecko_new_pools() -> List[str]:
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
        res = await _fetch(url, headers=headers)
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
            quote = tok_addr.get(quote_rel.get("id")),
            if base and base not in KNOWN_QUOTE_MINTS:
                mints.add(base)
            if quote and quote not in KNOWN_QUOTE_MINTS and quote != base:
                mints.add(quote)
    except Exception as e:
        log.warning(f"GeckoTerminal new_pools discovery failed: {e}")
    return list(mints)

async def _discover_from_gecko_search(query: str) -> List[str]:
    """Search pools globally and filter to Solana/Raydium."""
    mints: set = set()

    headers = {
        "Accept": "application/json;version=20230302",
        "User-Agent": "Mozilla/5.0"
    }
    url = f"{GECKO_API_URL}/search/pools?query={query}&include=base_token,quote_token,dex,network"
    if (cached := GECKO_SEARCH_CACHE.get(url)):
        return cached
    try:
        res = await _fetch(url, headers=headers)
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
            base = tok_addr.get(base_rel.get("id")),
            quote = tok_addr.get(quote_rel.get("id")),
            if base and base not in KNOWN_QUOTE_MINTS:
                mints.add(base)
            if quote and quote not in KNOWN_QUOTE_MINTS and quote != base:
                mints.add(quote)
    except Exception as e:
        log.warning(f"GeckoTerminal search discovery for query '{query}' failed: {e}")
    result = list(mints)
    GECKO_SEARCH_CACHE[url] = result
    return result

async def discover_from_gecko_search_pools() -> List[str]:
    """Search pools globally and filter to Solana/Raydium."""
    return await _discover_from_gecko_search("solana")


async def discover_from_gecko_search_tokens() -> List[str]:
    """Use GeckoTerminal search pools API (alternate query) and filter to Solana/Raydium."""
    return await _discover_from_gecko_search("bonk")


async def discover_from_dexscreener_new_pairs() -> List[str]:
    """Discover recent pairs on Solana via DexScreener and resolve their mints.

    DexScreener occasionally returns a JSON with schemaVersion but null pairs due to edge caching.
    Mitigate with HTTP/1.1, no-cache headers, and a jittered query param to bust stale edges.
    """
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
            "Origin": "https://dexscreener.com",
        }

        async def _ds_get_json() -> Optional[Dict[str, Any]]:
            try:
                ds_c = await get_http_client(ds=True)
                req_url = f"{base_url}?t={int(time.time()) % 7}"
                r = await ds_c.get(req_url, headers=ds_headers, follow_redirects=True)
                r.raise_for_status()
                return r.json()
            except Exception:
                return None

        res = await _ds_get_json()
        if not res or not (pairs := res.get("pairs")):
            await asyncio.sleep(2.0 + random.uniform(0, 0.5))
            res = await _ds_get_json()
            pairs = (res or {}).get("pairs") if res else None
            if not res or not pairs:
                if res:
                    preview = str(res)[:180]
                    log.debug(f"DexScreener /new returned null pairs. Preview: {preview}")
                return []

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


async def discover_from_dexscreener_search_recent() -> List[str]:
    """Fallback: use DexScreener search API and filter Solana pairs by recent creation time."""
    mints = set()
    url = "https://api.dexscreener.com/latest/dex/search?q=solana"
    try:
        ds_headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://dexscreener.com/solana",
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
        return []
    return list(mints)



async def aggregator_poll_worker() -> None:
    """Background worker to periodically poll aggregators for new tokens."""
    log.info("🦎 Aggregator Poller: Worker starting.")
    while True:
        try:
            disable_gecko = str(os.getenv("DISABLE_GECKO", "0")).strip().lower() in {"1", "true", "yes", "y"}
            gecko_task = discover_from_gecko_new_pools() if not disable_gecko else asyncio.sleep(0)
            gecko_search_task = discover_from_gecko_search_pools() if not disable_gecko else asyncio.sleep(0)
            gecko_token_search_task = discover_from_gecko_search_tokens() if not disable_gecko else asyncio.sleep(0)
            dexscreener_new_task = discover_from_dexscreener_new_pairs()
            dexscreener_search_task = discover_from_dexscreener_search_recent()
            results = await asyncio.gather(
                gecko_task,
                gecko_search_task,
                gecko_token_search_task,
                dexscreener_new_task,
                dexscreener_search_task,
                return_exceptions=True,
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