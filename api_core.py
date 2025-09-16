# -*- coding: utf-8 -*-
"""Core HTTP helpers and third-party API integrations for Token Tony."""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, Iterable, Optional

import httpx

from config import (
    BIRDEYE_API_KEY,
    BITQUERY_API_KEY,
    CONFIG,
    GECKO_API_URL,
    HELIUS_API_KEY,
    HELIUS_RPC_URL,
    JUP_QUOTE_URL,
    KNOWN_QUOTE_MINTS,
    RUGCHECK_API_URL,
    RUGCHECK_JWT,
    TOKEN2022_PROGRAM_ID,
    TOKEN_PROGRAM_ID,
    USDC_MINT,
    X_BEARER_TOKEN,
)
from utils import is_valid_solana_address

log = logging.getLogger("tony_helpers.api")

# --------------------------------------------------------------------------------------
# Provider health tracking / circuit breaker state
# --------------------------------------------------------------------------------------

_PROVIDER_LOCK = Lock()


def _new_provider_stats() -> Dict[str, Any]:
    return {
        "success": 0,
        "failure": 0,
        "circuit_open": False,
        "circuit_expires": 0.0,
        "last_error": "",
        "last_success": 0.0,
        "last_failure": 0.0,
        "avg_latency_ms": 0.0,
    }


_INITIAL_PROVIDERS = (
    "helius",
    "birdeye",
    "dexscreener",
    "gecko",
    "bitquery",
    "jupiter",
    "rugcheck",
    "twitter",
    "ipfs",
)

API_PROVIDERS: Dict[str, Dict[str, Any]] = {
    name: _new_provider_stats() for name in _INITIAL_PROVIDERS
}
# Backwards compatibility alias used by diagnostics output
API_HEALTH = API_PROVIDERS

# Lite mode flag is toggled when a provider circuit trips
LITE_MODE_UNTIL: float = 0.0


def _ensure_provider(name: str) -> Dict[str, Any]:
    if not name:
        raise ValueError("Provider name must be non-empty")
    with _PROVIDER_LOCK:
        if name not in API_PROVIDERS:
            API_PROVIDERS[name] = _new_provider_stats()
        return API_PROVIDERS[name]


def _set_lite_mode(until: float) -> None:
    global LITE_MODE_UNTIL
    if until > LITE_MODE_UNTIL:
        LITE_MODE_UNTIL = until


def _record_success(provider: str, latency_ms: float) -> None:
    stats = _ensure_provider(provider)
    stats["success"] += 1
    stats["last_success"] = time.time()
    # Simple running average for latency
    total = stats["success"] + stats["failure"]
    prev = float(stats.get("avg_latency_ms") or 0.0)
    stats["avg_latency_ms"] = prev + ((latency_ms - prev) / max(1, total))
    # Close circuit on success once cooldown elapsed
    if stats.get("circuit_open") and time.time() >= stats.get("circuit_expires", 0.0):
        stats["circuit_open"] = False


def _record_failure(provider: str, exc: Exception) -> None:
    stats = _ensure_provider(provider)
    stats["failure"] += 1
    stats["last_failure"] = time.time()
    stats["last_error"] = str(exc)[:200]
    total = stats["success"] + stats["failure"]
    threshold = float(CONFIG.get("CIRCUIT_BREAKER_FAILURE_THRESHOLD", 0.6) or 0.6)
    min_requests = int(CONFIG.get("CIRCUIT_BREAKER_MIN_REQUESTS", 5) or 5)
    reset_time = int(CONFIG.get("CIRCUIT_BREAKER_RESET_TIME", 300) or 300)
    if (
        total >= min_requests
        and not stats.get("circuit_open")
        and stats["failure"] / max(1, total) >= threshold
    ):
        stats["circuit_open"] = True
        stats["circuit_expires"] = time.time() + reset_time
        _set_lite_mode(stats["circuit_expires"])
        log.warning(
            "Circuit opened for provider %s (failure ratio %.2f)",
            provider,
            stats["failure"] / max(1, total),
        )


def _infer_provider_from_url(url: str) -> Optional[str]:
    low = url.lower()
    if "helius" in low:
        return "helius"
    if "birdeye" in low:
        return "birdeye"
    if "dexscreener" in low:
        return "dexscreener"
    if "geckoterminal" in low:
        return "gecko"
    if "bitquery" in low:
        return "bitquery"
    if "jup.ag" in low or "jupiter" in low:
        return "jupiter"
    if "rugcheck" in low:
        return "rugcheck"
    if "twitter" in low or "x.com" in low:
        return "twitter"
    if "ipfs" in low:
        return "ipfs"
    return None


async def _fetch(
    client: httpx.AsyncClient,
    url: str,
    *,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    json: Optional[Any] = None,
    data: Optional[Any] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
    provider: Optional[str] = None,
    allow_status: Iterable[int] = (200,),
    retries: Optional[int] = None,
) -> Optional[Any]:
    """Generic HTTP helper with retries and circuit breaker integration."""

    provider_name = provider or _infer_provider_from_url(url) or "generic"
    stats = _ensure_provider(provider_name)
    if stats.get("circuit_open") and time.time() < stats.get("circuit_expires", 0.0):
        log.debug("Skipping %s request to %s (circuit open)", provider_name, url)
        return None

    attempts = (int(CONFIG.get("HTTP_RETRIES", 2) or 2) + 1) if retries is None else max(1, retries + 1)
    timeout_val = timeout if timeout is not None else float(CONFIG.get("HTTP_TIMEOUT", 15.0) or 15.0)
    last_error: Optional[Exception] = None

    for attempt in range(attempts):
        start = time.perf_counter()
        try:
            response = await client.request(
                method,
                url,
                params=params,
                json=json,
                data=data,
                headers=headers,
                timeout=timeout_val,
            )
            latency_ms = (time.perf_counter() - start) * 1000.0
            if response.status_code not in allow_status:
                raise httpx.HTTPStatusError(
                    f"HTTP {response.status_code}", request=response.request, response=response
                )
            _record_success(provider_name, latency_ms)
            ctype = response.headers.get("content-type", "")
            if "json" in ctype:
                try:
                    return response.json()
                except json.JSONDecodeError:
                    return json.loads(response.text or "{}")
            return response.content if response.content else response.text
        except Exception as exc:  # pragma: no cover - network heavy paths
            last_error = exc
            _record_failure(provider_name, exc)
            if attempt + 1 >= attempts:
                break
            backoff = min(2.5, 0.5 * (2 ** attempt)) + random.uniform(0.0, 0.25)
            await asyncio.sleep(backoff)

    if last_error:
        log.debug("Request to %s failed after %s attempts: %s", url, attempts, last_error)
    return None


# --------------------------------------------------------------------------------------
# Domain specific helpers
# --------------------------------------------------------------------------------------

_IPFS_PREFIX = re.compile(r"^ipfs://", re.IGNORECASE)
_BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")


def _is_ipfs_uri(uri: str) -> bool:
    return bool(uri and _IPFS_PREFIX.match(uri))


async def fetch_ipfs_json(client: httpx.AsyncClient, uri: str) -> Optional[Dict[str, Any]]:
    """Resolve an IPFS URI using a set of HTTP gateways."""
    if not _is_ipfs_uri(uri):
        result = await _fetch(client, uri, provider="ipfs")
        return result if isinstance(result, dict) else None

    cid_path = uri[7:]
    if "/" in cid_path:
        cid, path = cid_path.split("/", 1)
        suffix = "/" + path
    else:
        cid, suffix = cid_path, ""

    gateways = [
        f"https://cloudflare-ipfs.com/ipfs/{cid}{suffix}",
        f"https://ipfs.io/ipfs/{cid}{suffix}",
        f"https://gateway.pinata.cloud/ipfs/{cid}{suffix}",
    ]
    timeout_s = float(CONFIG.get("IPFS_FETCH_TIMEOUT_SECONDS", 5.0) or 5.0)

    for idx, url in enumerate(gateways):
        result = await _fetch(client, url, timeout=timeout_s, provider="ipfs")
        if isinstance(result, (dict, list)):
            return result  # type: ignore[return-value]
        if isinstance(result, (bytes, bytearray)):
            try:
                return json.loads(result.decode("utf-8"))
            except Exception:
                pass
        if isinstance(result, str):
            try:
                return json.loads(result)
            except json.JSONDecodeError:
                pass
        # Hedge after the first gateway if configured
        hedge_ms = int(CONFIG.get("IPFS_HEDGE_MS", 0) or 0)
        if idx == 0 and hedge_ms > 0:
            await asyncio.sleep(hedge_ms / 1000.0)
    return None


async def fetch_helius_asset(client: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    if not HELIUS_API_KEY or not HELIUS_RPC_URL:
        return None
    payload = {
        "jsonrpc": "2.0",
        "id": "token-tony",
        "method": "getAsset",
        "params": {"id": mint},
    }
    result = await _fetch(client, HELIUS_RPC_URL, method="POST", json=payload, provider="helius")
    return result if isinstance(result, dict) else None


async def fetch_top10_via_rpc(client: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    if not HELIUS_RPC_URL:
        return None
    payload_accounts = {
        "jsonrpc": "2.0",
        "id": "token-tony",
        "method": "getTokenLargestAccounts",
        "params": [mint, {"commitment": "confirmed"}],
    }
    payload_supply = {
        "jsonrpc": "2.0",
        "id": "token-tony",
        "method": "getTokenSupply",
        "params": [mint],
    }
    accounts = await _fetch(client, HELIUS_RPC_URL, method="POST", json=payload_accounts, provider="helius")
    supply = await _fetch(client, HELIUS_RPC_URL, method="POST", json=payload_supply, provider="helius")

    try:
        supply_val = int((supply or {}).get("result", {}).get("value", {}).get("amount", "0"))
    except Exception:
        supply_val = 0

    holders = []
    try:
        holders = (accounts or {}).get("result", {}).get("value", []) or []
    except Exception:
        holders = []

    if not holders:
        return None

    holders_count = sum(1 for item in holders if float(item.get("uiAmount", 0) or 0) > 0)
    if supply_val <= 0:
        return {"holders_count": holders_count}

    top10_sum = 0
    for item in holders[:10]:
        try:
            top10_sum += int(item.get("amount", "0") or 0)
        except Exception:
            continue
    pct = round((top10_sum / supply_val) * 100.0, 2) if supply_val else None
    return {
        "holders_count": holders_count,
        "top10_holder_percentage": pct,
    }


async def fetch_holders_count_via_rpc(client: httpx.AsyncClient, mint: str) -> Optional[int]:
    data = await fetch_top10_via_rpc(client, mint)
    if data:
        return int(data.get("holders_count") or 0)
    return None


async def fetch_birdeye(client: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    if not BIRDEYE_API_KEY:
        return None
    url = f"https://public-api.birdeye.so/public/marketstat/solana/{mint}"
    headers = {"X-API-KEY": BIRDEYE_API_KEY, "Accept": "application/json"}
    result = await _fetch(client, url, headers=headers, provider="birdeye")
    return result if isinstance(result, dict) else None


async def fetch_dexscreener_by_mint(client: httpx.AsyncClient, identifier: str) -> Optional[Dict[str, Any]]:
    """Fetch token details from DexScreener by mint address."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{identifier}"
    result = await _fetch(client, url, provider="dexscreener")
    if not isinstance(result, dict):
        return None
    pairs = result.get("pairs") or []
    if not pairs:
        return None
    # Pick the pair with the highest USD liquidity
    def _liq(pair: Dict[str, Any]) -> float:
        try:
            return float((pair.get("liquidity") or {}).get("usd") or 0.0)
        except Exception:
            return 0.0

    best = max(pairs, key=_liq)
    base = best.get("baseToken", {}) or {}
    quote = best.get("quoteToken", {}) or {}
    created_ms = best.get("pairCreatedAt") or None
    created_iso: Optional[str] = None
    if created_ms:
        try:
            created_iso = datetime.fromtimestamp(int(created_ms) / 1000, tz=timezone.utc).isoformat()
        except Exception:
            created_iso = None

    normalized = {
        "pair_address": best.get("pairAddress"),
        "pair_url": best.get("url"),
        "dex": best.get("dexId"),
        "price_usd": float(best.get("priceUsd") or 0.0),
        "price_change_24h": float(best.get("priceChange24h") or 0.0),
        "volume_24h_usd": float((best.get("volume") or {}).get("h24") or 0.0),
        "liquidity_usd": float((best.get("liquidity") or {}).get("usd") or 0.0),
        "market_cap_usd": float(best.get("fdv") or 0.0),
        "pair_created_ms": created_ms,
        "pool_created_at": created_iso or best.get("info", {}).get("createdAt"),
        "base_token": {
            "address": base.get("address"),
            "symbol": base.get("symbol"),
            "name": base.get("name"),
        },
        "quote_token": {
            "address": quote.get("address"),
            "symbol": quote.get("symbol"),
            "name": quote.get("name"),
        },
    }
    return normalized


async def _fetch_dexscreener_pair(client: httpx.AsyncClient, pair_address: str) -> Optional[Dict[str, Any]]:
    url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{pair_address}"
    result = await _fetch(client, url, provider="dexscreener")
    if not isinstance(result, dict):
        return None
    pairs = result.get("pairs") or []
    return pairs[0] if pairs else None


async def fetch_dexscreener_chart(pair_address: Optional[str]) -> Optional[bytes]:
    if not pair_address:
        return None
    url = f"https://cdn.dexscreener.com/candles/solana/{pair_address}.png"
    async with httpx.AsyncClient() as client:
        result = await _fetch(client, url, provider="dexscreener")
    if isinstance(result, (bytes, bytearray)):
        return bytes(result)
    if isinstance(result, str):
        return result.encode("utf-8")
    return None


async def fetch_gecko_market_data(client: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    headers = {"Accept": "application/json;version=20230302"}
    url = f"{GECKO_API_URL}/networks/solana/tokens/{mint}?include=market_data"  # type: ignore[str-format]
    result = await _fetch(client, url, headers=headers, provider="gecko")
    if not isinstance(result, dict):
        return None
    data = (result.get("data") or {}).get("attributes", {})
    if not data:
        return None
    market = data.get("market_data") or {}
    return {
        "price_usd": float(market.get("price_usd") or 0.0),
        "price_change_24h": float(market.get("price_change_percent_24h") or 0.0),
        "volume_24h_usd": float(market.get("volume_usd") or 0.0),
        "liquidity_usd": float(market.get("liquidity_usd") or 0.0),
        "market_cap_usd": float(market.get("market_cap_usd") or 0.0),
        "pair_created_ms": data.get("pool_created_at") or None,
        "pool_created_at": data.get("pool_created_at"),
    }


async def fetch_market_snapshot(client: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    # Primary: DexScreener
    ds = await fetch_dexscreener_by_mint(client, mint)
    if ds:
        return ds
    # Secondary: BirdEye
    be = await fetch_birdeye(client, mint)
    if be and isinstance(be.get("data"), dict):
        data = be["data"]
        try:
            return {
                "price_usd": float(data.get("price", 0.0)),
                "price_change_24h": float(data.get("priceChange24h", 0.0)),
                "volume_24h_usd": float(data.get("v24h", 0.0)),
                "liquidity_usd": float(data.get("liquidity", 0.0)),
                "market_cap_usd": float(data.get("mc", 0.0)),
            }
        except Exception:
            pass
    # Tertiary: GeckoTerminal
    return await fetch_gecko_market_data(client, mint)


async def fetch_jupiter_has_route(client: httpx.AsyncClient, mint: str) -> Optional[bool]:
    params = {
        "inputMint": mint,
        "outputMint": USDC_MINT,
        "amount": 1_000_000,  # 1 token assuming 6 decimals; Jupiter handles scaling
        "slippageBps": 50,
        "onlyDirectRoutes": "true",
    }
    result = await _fetch(client, JUP_QUOTE_URL, params=params, provider="jupiter")
    if not isinstance(result, dict):
        return None
    # Jupiter v6 returns either 'data' or 'routePlan' when a route exists
    if result.get("data"):
        return True
    if isinstance(result.get("routePlan"), list) and result["routePlan"]:
        return True
    return False if result.get("error") else None


async def fetch_rugcheck_score(client: httpx.AsyncClient, mint: str) -> Optional[str]:
    url = f"{RUGCHECK_API_URL.rstrip('/')}/token/{mint}"
    headers = {"Accept": "application/json"}
    if RUGCHECK_JWT:
        headers["Authorization"] = f"Bearer {RUGCHECK_JWT}"
    result = await _fetch(client, url, headers=headers, provider="rugcheck")
    if not isinstance(result, dict):
        return None
    summary = result.get("summary") or {}
    label = summary.get("risk") or summary.get("label")
    if label:
        return str(label)
    if result.get("risk"):
        return str(result["risk"])
    return None


async def fetch_creator_dossier_bitquery(client: httpx.AsyncClient, creator: str) -> Optional[int]:
    if not BITQUERY_API_KEY:
        return None
    query = """
    query ($creator: String!) {
      solana {
        minting: transfers(
          transferType: {is: Mint}
          receiverAddress: {is: $creator}
        ) {
          count
        }
      }
    }
    """
    variables = {"creator": creator}
    headers = {
        "X-API-KEY": BITQUERY_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {"query": query, "variables": variables}
    result = await _fetch(
        client,
        "https://graphql.bitquery.io",
        method="POST",
        json=payload,
        headers=headers,
        provider="bitquery",
    )
    if not isinstance(result, dict):
        return None
    try:
        transfers = (
            result.get("data", {})
            .get("solana", {})
            .get("minting", [])
        )
        if transfers:
            return int(transfers[0].get("count") or 0)
    except Exception:
        pass
    return None


async def fetch_twitter_stats(client: httpx.AsyncClient, url_or_handle: str) -> Optional[Dict[str, Any]]:
    if not X_BEARER_TOKEN:
        return None
    handle = url_or_handle.strip()
    if handle.startswith("http"):
        match = re.search(r"twitter\.com/(?:#!\/)?([^/?#]+)", handle)
        if not match:
            match = re.search(r"x\.com/(?:#!\/)?([^/?#]+)", handle)
        handle = match.group(1) if match else handle
    handle = handle.lstrip("@")
    if not handle:
        return None
    url = f"https://api.twitter.com/2/users/by/username/{handle}"
    params = {"user.fields": "public_metrics,created_at"}
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
    user = await _fetch(client, url, params=params, headers=headers, provider="twitter")
    if not isinstance(user, dict):
        return None
    data = user.get("data") or {}
    metrics = data.get("public_metrics") or {}
    try:
        return {
            "username": data.get("username"),
            "name": data.get("name"),
            "created_at": data.get("created_at"),
            "followers": int(metrics.get("followers_count", 0) or 0),
            "following": int(metrics.get("following_count", 0) or 0),
            "tweet_count": int(metrics.get("tweet_count", 0) or 0),
            "listed_count": int(metrics.get("listed_count", 0) or 0),
        }
    except Exception:
        return None


async def extract_mint_from_check_text(client: httpx.AsyncClient, text: str) -> Optional[str]:
    if not text:
        return None
    cleaned = text.strip()
    cleaned = re.sub(r"^/[A-Za-z0-9_]+\s*", "", cleaned)

    # Direct base58 candidates
    for candidate in _BASE58_RE.findall(cleaned):
        if is_valid_solana_address(candidate) and candidate not in KNOWN_QUOTE_MINTS:
            return candidate

    # Known URL patterns carrying the mint directly
    url_patterns = [
        r"birdeye\.so/(?:token|coin)/([1-9A-HJ-NP-Za-km-z]{32,44})",
        r"solscan\.io/token/([1-9A-HJ-NP-Za-km-z]{32,44})",
        r"pump\.fun/coin/([1-9A-HJ-NP-Za-km-z]{32,44})",
        r"dexscreener\.com/(?:solana|pump|raydium)/token/([1-9A-HJ-NP-Za-km-z]{32,44})",
    ]
    for pattern in url_patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            cand = match.group(1)
            if is_valid_solana_address(cand) and cand not in KNOWN_QUOTE_MINTS:
                return cand

    # Query parameter extraction (e.g., token=)
    q_match = re.search(r"token=([1-9A-HJ-NP-Za-km-z]{32,44})", cleaned)
    if q_match:
        cand = q_match.group(1)
        if is_valid_solana_address(cand) and cand not in KNOWN_QUOTE_MINTS:
            return cand

    # DexScreener pair link fallback -> fetch pair details
    pair_match = re.search(r"dexscreener\.com/[^\s]+/([A-Za-z0-9]{20,})", cleaned)
    if pair_match:
        pair = pair_match.group(1).split("?")[0]
        ds_pair = await _fetch_dexscreener_pair(client, pair)
        if ds_pair:
            base = ds_pair.get("baseToken", {}).get("address")
            if base and is_valid_solana_address(base) and base not in KNOWN_QUOTE_MINTS:
                return base

    return None


async def fetch_holders_via_program_accounts(
    client: httpx.AsyncClient, mint: str
) -> Optional[int]:
    """Approximate holders via programAccounts scanning (fallback)."""
    if not HELIUS_RPC_URL:
        return None
    payload = {
        "jsonrpc": "2.0",
        "id": "token-tony",
        "method": "getProgramAccounts",
        "params": [
            TOKEN_PROGRAM_ID,
            {
                "encoding": "base64",
                "filters": [
                    {"dataSize": 165},
                    {
                        "memcmp": {
                            "offset": 0,
                            "bytes": mint,
                        }
                    },
                ],
            },
        ],
    }
    result = await _fetch(client, HELIUS_RPC_URL, method="POST", json=payload, provider="helius")
    if not isinstance(result, dict):
        # Try Token2022 program as fallback
        payload["params"][0] = TOKEN2022_PROGRAM_ID
        result = await _fetch(client, HELIUS_RPC_URL, method="POST", json=payload, provider="helius")
    if not isinstance(result, dict):
        return None
    try:
        return len(result.get("result", []) or [])
    except Exception:
        return None


async def fetch_market_pair_address(client: httpx.AsyncClient, mint: str) -> Optional[str]:
    """Utility to fetch the most liquid DexScreener pair address for a mint."""
    ds = await fetch_dexscreener_by_mint(client, mint)
    if ds:
        return ds.get("pair_address")
    return None


__all__ = [
    "API_HEALTH",
    "API_PROVIDERS",
    "LITE_MODE_UNTIL",
    "_fetch",
    "_is_ipfs_uri",
    "extract_mint_from_check_text",
    "fetch_birdeye",
    "fetch_creator_dossier_bitquery",
    "fetch_dexscreener_by_mint",
    "fetch_dexscreener_chart",
    "fetch_gecko_market_data",
    "fetch_helius_asset",
    "fetch_holders_count_via_rpc",
    "fetch_holders_via_program_accounts",
    "fetch_ipfs_json",
    "fetch_jupiter_has_route",
    "fetch_market_pair_address",
    "fetch_market_snapshot",
    "fetch_rugcheck_score",
    "fetch_top10_via_rpc",
    "fetch_twitter_stats",
]
