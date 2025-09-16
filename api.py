# -*- coding: utf-8 -*-
"""High level HTTP helpers for Token Tony.

This module provides the implementation that historically lived in the
``tony_helpers.api`` package.  The original project imported helpers such as
``fetch_birdeye`` or ``fetch_dexscreener_by_mint`` from that namespace.  The
package is no longer published on PyPI so we vendor the functionality here
directly and expose the exact same API surface.

The helpers share a few common goals:

* provide a thin asynchronous wrapper around the external REST/GraphQL
  services used by the bot;
* track basic success/failure counters so the diagnostic command can report
  provider health and so the circuit breaker worker can cool down noisy
  endpoints; and
* degrade gracefully when configuration or network prerequisites are missing
  (returning ``None`` instead of raising and never crashing the bot).

The functions intentionally return ``dict``/``list`` payloads that match the
shape expected by the rest of the project.  Only a subset of the upstream
response is normalised – enough for the analysis and reporting pipeline to
work without re-shipping the entire payload.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx
from cachetools import TTLCache

from config import (
    BIRDEYE_API_KEY,
    BITQUERY_API_KEY,
    BITQUERY_URL,
    CONFIG,
    GECKO_API_URL,
    HELIUS_API_KEY,
    HELIUS_RPC_URL,
    JUP_QUOTE_URL,
    KNOWN_QUOTE_MINTS,
    RUGCHECK_API_URL,
    RUGCHECK_JWT,
    SOL_MINT,
    USDC_MINT,
    USDT_MINT,
    X_BEARER_TOKEN,
    get_ipfs_gateways,
)
from utils import is_valid_solana_address

log = logging.getLogger("token_tony.api")

# --------------------------------------------------------------------------------------
# Provider bookkeeping
# --------------------------------------------------------------------------------------


def _provider_state() -> Dict[str, Any]:
    return {
        "success": 0,
        "failure": 0,
        "circuit_open": False,
        "last_success": 0.0,
        "opened_at": 0.0,
    }


API_PROVIDERS: Dict[str, Dict[str, Any]] = {
    name: _provider_state()
    for name in (
        "birdeye",
        "dexscreener",
        "gecko",
        "helius",
        "rugcheck",
        "jupiter",
        "bitquery",
        "twitter",
        "ipfs",
    )
}

# ``API_HEALTH`` is imported directly by ``/diag`` so keep it as a reference to
# ``API_PROVIDERS`` for compatibility with the original project.
API_HEALTH = API_PROVIDERS

# When the circuit breaker observes sustained failures we temporarily enable a
# lite mode.  ``Token_TonyV10.py`` reads this value to adjust the copy in
# outgoing reports.
LITE_MODE_UNTIL: float = 0.0

_FAILURE_THRESHOLD = float(CONFIG.get("CIRCUIT_BREAKER_FAILURE_THRESHOLD", 0.6) or 0.6)
_MIN_REQUESTS = int(CONFIG.get("CIRCUIT_BREAKER_MIN_REQUESTS", 5) or 5)
_RESET_WINDOW = int(CONFIG.get("CIRCUIT_BREAKER_RESET_TIME", 300) or 300)


def _mark_api_result(provider: str, *, success: bool) -> None:
    """Update health counters for ``provider`` and open the circuit when needed."""

    global LITE_MODE_UNTIL

    stats = API_PROVIDERS.setdefault(provider, _provider_state())
    now = time.time()

    if success:
        stats["success"] = stats.get("success", 0) + 1
        stats["last_success"] = now
        # A success is a natural probe – relax the breaker when the cool-down
        # window expired.
        if stats.get("circuit_open") and (now - stats.get("opened_at", 0)) > _RESET_WINDOW:
            stats["circuit_open"] = False
            stats["failure"] = max(0, int(stats.get("failure", 0) * 0.5))
        return

    stats["failure"] = stats.get("failure", 0) + 1
    total = stats.get("success", 0) + stats.get("failure", 0)
    if total < max(1, _MIN_REQUESTS):
        return

    ratio = stats["failure"] / max(total, 1)
    if ratio >= _FAILURE_THRESHOLD:
        if not stats.get("circuit_open"):
            log.warning(
                "Circuit breaker tripped for %s (failure ratio %.2f)", provider, ratio
            )
        stats["circuit_open"] = True
        stats["opened_at"] = now
        # Keep the lite-mode window reasonably short; the reset worker will
        # gradually decay the failure counter.
        LITE_MODE_UNTIL = max(LITE_MODE_UNTIL, now + 180)


def _provider_available(provider: str) -> bool:
    """Return ``True`` if the provider circuit is open for business."""

    stats = API_PROVIDERS.get(provider)
    if not stats or not stats.get("circuit_open"):
        return True

    # Allow opportunistic probes once the cool-down window elapsed.
    opened = stats.get("opened_at", 0)
    if opened and (time.time() - opened) > _RESET_WINDOW:
        stats["circuit_open"] = False
        stats["failure"] = max(0, int(stats.get("failure", 0) * 0.5))
        return True
    return False


# --------------------------------------------------------------------------------------
# Small utility helpers
# --------------------------------------------------------------------------------------


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str) and value.strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _default_timeout(timeout: Optional[float]) -> httpx.Timeout:
    base = float(CONFIG.get("HTTP_TIMEOUT", 10.0) or 10.0)
    if timeout is not None:
        try:
            base = float(timeout)
        except Exception:
            pass
    return httpx.Timeout(base)


async def _fetch(
    client: httpx.AsyncClient,
    url: str,
    *,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json: Any = None,
    data: Any = None,
    timeout: Optional[float] = None,
) -> Optional[Any]:
    """Perform a best-effort HTTP request and return JSON/text payloads.

    The helper hides HTTP exceptions and simply returns ``None`` when the
    request fails.  This keeps the rest of the code base tidy: API helpers only
    need to update the provider health counters and decide on fallbacks.
    """

    try:
        response = await client.request(
            method.upper(),
            url,
            headers=headers,
            params=params,
            json=json,
            data=data,
            timeout=_default_timeout(timeout),
        )
        response.raise_for_status()
    except Exception as exc:
        log.debug("HTTP request failed for %s: %s", url, exc)
        return None

    ctype = response.headers.get("content-type", "").lower()
    if "application/json" in ctype or "text/json" in ctype:
        try:
            return response.json()
        except json.JSONDecodeError:
            return None
    try:
        return response.json()
    except Exception:
        return response.text


def _is_ipfs_uri(uri: str) -> bool:
    uri = (uri or "").strip().lower()
    return uri.startswith("ipfs://") or uri.startswith("ipns://")


def _build_ipfs_url(uri: str, gateway: str) -> str:
    if not _is_ipfs_uri(uri):
        return uri
    content = uri.split("://", 1)[1]
    return gateway.rstrip("/") + "/" + content.lstrip("/")


_IPFS_CACHE = TTLCache(maxsize=512, ttl=float(CONFIG.get("FETCH_CACHE_TTL", 300) or 300))


async def fetch_ipfs_json(client: httpx.AsyncClient, uri: str) -> Optional[Dict[str, Any]]:
    """Resolve an ``ipfs://`` URI using the configured gateways."""

    if not uri:
        return None

    if not _is_ipfs_uri(uri):
        res = await _fetch(client, uri)
        return res if isinstance(res, dict) else None

    if uri in _IPFS_CACHE:
        return _IPFS_CACHE[uri]

    gateways = get_ipfs_gateways()
    hedge_delay = float(CONFIG.get("IPFS_HEDGE_MS", 0) or 0) / 1000.0
    provider = "ipfs"

    if not _provider_available(provider):
        return None

    async def _resolve(gateway: str) -> Optional[Dict[str, Any]]:
        url = _build_ipfs_url(uri, gateway)
        result = await _fetch(client, url, timeout=CONFIG.get("IPFS_FETCH_TIMEOUT_SECONDS", 5))
        if isinstance(result, dict):
            return result
        return None

    tasks = []
    for idx, gateway in enumerate(gateways):
        if idx == 0 or hedge_delay <= 0:
            tasks.append(_resolve(gateway))
        else:
            await asyncio.sleep(hedge_delay)
            tasks.append(_resolve(gateway))

    for task in tasks:
        try:
            res = await task
            if res is not None:
                _mark_api_result(provider, success=True)
                _IPFS_CACHE[uri] = res
                return res
        except Exception:
            continue

    _mark_api_result(provider, success=False)
    return None


# --------------------------------------------------------------------------------------
# BirdEye helpers
# --------------------------------------------------------------------------------------


async def fetch_birdeye(client: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    if not mint or not BIRDEYE_API_KEY:
        return None
    provider = "birdeye"
    if not _provider_available(provider):
        return None

    headers = {
        "accept": "application/json",
        "X-API-KEY": BIRDEYE_API_KEY,
        "User-Agent": "TokenTonyBot/1.0",
    }
    params = {"address": mint, "chain": "solana"}
    url = "https://public-api.birdeye.so/public/token/market-data"

    data = await _fetch(client, url, headers=headers, params=params)
    if isinstance(data, dict):
        _mark_api_result(provider, success=True)
        return data

    _mark_api_result(provider, success=False)
    return None


# --------------------------------------------------------------------------------------
# DexScreener helpers
# --------------------------------------------------------------------------------------


def _normalise_pair(pair: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(pair, dict):
        return out

    out["pair_address"] = (
        pair.get("pairAddress")
        or pair.get("pair_address")
        or pair.get("address")
    )
    out["dex"] = pair.get("dexId") or pair.get("dex" or "dexId")
    out["price_usd"] = _as_float(pair.get("priceUsd"))
    liq = pair.get("liquidity") or {}
    out["liquidity_usd"] = _as_float(liq.get("usd") or liq.get("value"))
    vol = pair.get("volume") or {}
    out["volume_24h_usd"] = _as_float(vol.get("h24") or vol.get("24h"))
    out["price_change_24h"] = _as_float((pair.get("priceChange") or {}).get("h24"))
    out["market_cap_usd"] = _as_float(pair.get("fdv") or pair.get("marketCap"))
    out["pair_created_ms"] = (
        pair.get("pairCreatedAt")
        or pair.get("pairCreatedTime")
        or pair.get("createdAt")
    )
    out["pool_created_at"] = pair.get("poolCreatedAt")

    base_addr = (pair.get("baseToken") or {}).get("address")
    quote_addr = (pair.get("quoteToken") or {}).get("address")
    if base_addr and base_addr not in KNOWN_QUOTE_MINTS:
        out["base_mint"] = base_addr
    if quote_addr and quote_addr not in KNOWN_QUOTE_MINTS:
        out["quote_mint"] = quote_addr

    return out


async def _fetch_dexscreener(
    client: httpx.AsyncClient,
    url: str,
    *,
    expect_key: str,
) -> Optional[Dict[str, Any]]:
    provider = "dexscreener"
    if not _provider_available(provider):
        return None

    headers = {
        "accept": "application/json",
        "User-Agent": "TokenTonyBot/1.0",
        "Referer": "https://dexscreener.com/solana",
    }

    data = await _fetch(client, url, headers=headers)
    if isinstance(data, dict) and data.get(expect_key):
        _mark_api_result(provider, success=True)
        return data

    _mark_api_result(provider, success=False)
    return None


async def fetch_dexscreener_by_mint(
    client: httpx.AsyncClient, mint: str
) -> Optional[Dict[str, Any]]:
    if not mint:
        return None

    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    data = await _fetch_dexscreener(client, url, expect_key="pairs")
    if not data:
        return None

    pairs = data.get("pairs") or []
    # Prefer Solana pools with the highest liquidity.
    sol_pairs = [p for p in pairs if p.get("chainId") in {"solana", "sol"}]
    pairs = sol_pairs or pairs

    def _liq(pair: Dict[str, Any]) -> float:
        return float((pair.get("liquidity") or {}).get("usd") or 0.0)

    best = max(pairs, key=_liq, default=None)
    if not best:
        return None

    info = _normalise_pair(best)
    info.setdefault("mint", mint)
    return info


async def fetch_dexscreener_pair(
    client: httpx.AsyncClient, pair_address: str
) -> Optional[Dict[str, Any]]:
    if not pair_address:
        return None
    url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{pair_address}"
    data = await _fetch_dexscreener(client, url, expect_key="pair")
    if not data:
        return None
    info = _normalise_pair(data.get("pair") or {})
    info.setdefault("pair_address", pair_address)
    return info


async def fetch_dexscreener_chart(
    pair_address: Optional[str], *, timeframe: str = "1h"
) -> Optional[bytes]:
    if not pair_address:
        return None

    provider = "dexscreener"
    if not _provider_available(provider):
        return None

    url = (
        f"https://image.dexscreener.com/dexcharts/solana/{pair_address}?timeframe={timeframe}"
        "&width=800&height=450&format=png"
    )
    try:
        async with httpx.AsyncClient(timeout=_default_timeout(None)) as client:
            response = await client.get(
                url,
                headers={
                    "User-Agent": "TokenTonyBot/1.0",
                    "Referer": "https://dexscreener.com/solana",
                    "Accept": "image/png",
                },
            )
            response.raise_for_status()
            _mark_api_result(provider, success=True)
            return response.content
    except Exception as exc:
        log.debug("DexScreener chart fetch failed: %s", exc)
        _mark_api_result(provider, success=False)
        return None


# --------------------------------------------------------------------------------------
# GeckoTerminal helpers
# --------------------------------------------------------------------------------------


async def fetch_gecko_market_data(
    client: httpx.AsyncClient, mint: str
) -> Optional[Dict[str, Any]]:
    if not mint:
        return None
    provider = "gecko"
    if not _provider_available(provider):
        return None

    headers = {
        "accept": "application/json;version=20230302",
        "User-Agent": "TokenTonyBot/1.0",
    }
    url = f"{GECKO_API_URL}/networks/solana/tokens/{mint}"
    data = await _fetch(client, url, headers=headers)
    if not isinstance(data, dict):
        _mark_api_result(provider, success=False)
        return None

    attr = (data.get("data") or {}).get("attributes") or {}
    market = {
        "price_usd": _as_float(attr.get("price_usd")),
        "liquidity_usd": _as_float(attr.get("liquidity_usd")),
        "volume_24h_usd": _as_float(attr.get("volume_usd_24h")),
        "market_cap_usd": _as_float(attr.get("fdv_usd")),
        "price_change_24h": _as_float(attr.get("price_change_percentage_24h")),
    }
    if any(v is not None for v in market.values()):
        _mark_api_result(provider, success=True)
        return market

    _mark_api_result(provider, success=False)
    return None


# --------------------------------------------------------------------------------------
# Jupiter helpers
# --------------------------------------------------------------------------------------


async def fetch_jupiter_has_route(
    client: httpx.AsyncClient, mint: str
) -> Optional[bool]:
    if not mint:
        return None

    # Quick short-circuit for common quote assets.
    if mint in {SOL_MINT, USDC_MINT, USDT_MINT}:
        return True

    provider = "jupiter"
    if not _provider_available(provider):
        return None

    params = {
        "inputMint": mint,
        "outputMint": USDC_MINT,
        "amount": 1_000_000,  # 1 token with 6 decimals – enough for a route probe
        "slippageBps": int(CONFIG.get("JUP_SLIPPAGE_BPS", 300) or 300),
        "swapMode": "ExactIn",
    }
    headers = {"accept": "application/json", "User-Agent": "TokenTonyBot/1.0"}
    data = await _fetch(client, JUP_QUOTE_URL, headers=headers, params=params)

    if isinstance(data, dict):
        if data.get("error"):
            _mark_api_result(provider, success=True)
            return False
        routes = data.get("routePlan") or data.get("data") or []
        _mark_api_result(provider, success=True)
        return bool(routes)

    _mark_api_result(provider, success=False)
    return None


# --------------------------------------------------------------------------------------
# Helius RPC helpers
# --------------------------------------------------------------------------------------


def _rpc_endpoint() -> str:
    if HELIUS_API_KEY:
        return HELIUS_RPC_URL
    return "https://api.mainnet-beta.solana.com"


async def fetch_helius_asset(
    client: httpx.AsyncClient, mint: str
) -> Optional[Dict[str, Any]]:
    if not mint:
        return None
    provider = "helius"
    if not _provider_available(provider):
        return None

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAsset",
        "params": {"id": mint},
    }
    data = await _fetch(client, _rpc_endpoint(), method="POST", json=payload)
    if isinstance(data, dict) and data.get("result"):
        _mark_api_result(provider, success=True)
        return data

    _mark_api_result(provider, success=False)
    return None


async def fetch_top10_via_rpc(
    client: httpx.AsyncClient, mint: str
) -> Optional[Dict[str, Any]]:
    if not mint:
        return None
    provider = "helius"
    if not _provider_available(provider):
        return None

    largest_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [mint, {"commitment": "confirmed"}],
    }
    largest = await _fetch(client, _rpc_endpoint(), method="POST", json=largest_payload)
    accounts = ((largest or {}).get("result") or {}).get("value") or []
    if not accounts:
        _mark_api_result(provider, success=False)
        return None

    supply_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenSupply",
        "params": [mint],
    }
    supply_res = await _fetch(client, _rpc_endpoint(), method="POST", json=supply_payload)
    supply_info = (supply_res or {}).get("result", {}).get("value", {})
    try:
        supply_raw = int(supply_info.get("amount"))
        decimals = int(supply_info.get("decimals", 0))
        divisor = 10 ** decimals if decimals >= 0 else 1
    except Exception:
        supply_raw = 0
        divisor = 1

    try:
        top10 = sum(int(acc.get("amount", "0")) for acc in accounts[:10])
    except Exception:
        top10 = 0

    pct = None
    if supply_raw > 0:
        pct = (top10 / supply_raw) * 100.0

    out = {"top10_holder_percentage": round(pct, 2) if pct is not None else None}
    _mark_api_result(provider, success=True)
    return out


async def fetch_holders_count_via_rpc(
    client: httpx.AsyncClient, mint: str
) -> Optional[int]:
    if not mint:
        return None
    provider = "helius"
    if not _provider_available(provider):
        return None

    # ``getTokenLargestAccounts`` is cheap and quickly tells us if a token has
    # at least ``n`` holders.  Helius does not expose a free aggregate holders
    # count endpoint so we report the number of distinct accounts returned by
    # this call.  The list contains up to 20 holders which is sufficient to
    # signal "more than a handful" to the rest of the pipeline.
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [mint, {"commitment": "confirmed"}],
    }
    res = await _fetch(client, _rpc_endpoint(), method="POST", json=payload)
    accounts = ((res or {}).get("result") or {}).get("value") or []
    if accounts:
        _mark_api_result(provider, success=True)
        return len(accounts)

    _mark_api_result(provider, success=False)
    return None


# --------------------------------------------------------------------------------------
# RugCheck helpers
# --------------------------------------------------------------------------------------


async def fetch_rugcheck_score(
    client: httpx.AsyncClient, mint: str
) -> Optional[str]:
    if not mint:
        return None
    provider = "rugcheck"
    if not _provider_available(provider):
        return None

    headers = {
        "accept": "application/json",
        "User-Agent": "TokenTonyBot/1.0",
    }
    if RUGCHECK_JWT:
        headers["Authorization"] = f"Bearer {RUGCHECK_JWT}"

    url = f"{RUGCHECK_API_URL.rstrip('/')}/tokens/{mint}"
    data = await _fetch(client, url, headers=headers)
    if isinstance(data, dict):
        label = (
            data.get("label")
            or data.get("risk")
            or data.get("riskLevel")
            or data.get("risk_label")
        )
        _mark_api_result(provider, success=True)
        return label

    _mark_api_result(provider, success=False)
    return None


# --------------------------------------------------------------------------------------
# Bitquery helpers
# --------------------------------------------------------------------------------------


async def fetch_creator_dossier_bitquery(
    client: httpx.AsyncClient, creator: str
) -> Optional[int]:
    if not creator or not BITQUERY_API_KEY:
        return None
    provider = "bitquery"
    if not _provider_available(provider):
        return None

    query = {
        "query": """
            query($creator: String!) {
              solana(network: solana) {
                mintAccounts(
                  where: { mintAccount: { mintAuthority: { is: $creator } } }
                ) {
                  count
                }
              }
            }
        """,
        "variables": {"creator": creator},
    }
    headers = {
        "X-API-KEY": BITQUERY_API_KEY,
        "content-type": "application/json",
    }
    data = await _fetch(client, BITQUERY_URL, method="POST", headers=headers, json=query)
    try:
        count = (
            (data or {})
            .get("data", {})
            .get("solana", {})
            .get("mintAccounts", [{}])[0]
            .get("count")
        )
    except Exception:
        count = None

    if isinstance(count, int):
        _mark_api_result(provider, success=True)
        return count

    _mark_api_result(provider, success=False)
    return None


# --------------------------------------------------------------------------------------
# Twitter helpers
# --------------------------------------------------------------------------------------


_TWITTER_CACHE = TTLCache(maxsize=256, ttl=900)
_TWITTER_RE = re.compile(r"twitter\.com/([^/?#]+)", re.IGNORECASE)


async def fetch_twitter_stats(
    client: httpx.AsyncClient, handle_or_url: str
) -> Optional[Dict[str, Any]]:
    if not handle_or_url or not X_BEARER_TOKEN:
        return None
    provider = "twitter"
    if not _provider_available(provider):
        return None

    match = _TWITTER_RE.search(handle_or_url or "")
    handle = match.group(1) if match else handle_or_url.strip().lstrip("@")
    if not handle:
        return None

    if handle in _TWITTER_CACHE:
        return _TWITTER_CACHE[handle]

    url = f"https://api.twitter.com/2/users/by/username/{handle}"
    params = {"user.fields": "public_metrics"}
    headers = {
        "Authorization": f"Bearer {X_BEARER_TOKEN}",
        "User-Agent": "TokenTonyBot/1.0",
    }
    data = await _fetch(client, url, headers=headers, params=params)
    metrics = ((data or {}).get("data") or {}).get("public_metrics") or {}

    if metrics:
        out = {
            "followers": metrics.get("followers_count"),
            "following": metrics.get("following_count"),
            "tweet_count": metrics.get("tweet_count"),
        }
        _TWITTER_CACHE[handle] = out
        _mark_api_result(provider, success=True)
        return out

    _mark_api_result(provider, success=False)
    return None


# --------------------------------------------------------------------------------------
# Market snapshot orchestration
# --------------------------------------------------------------------------------------


async def fetch_market_snapshot(
    client: httpx.AsyncClient, mint: str
) -> Optional[Dict[str, Any]]:
    if not mint:
        return None

    ds = await fetch_dexscreener_by_mint(client, mint)
    if ds:
        snapshot = {
            "mint": mint,
            "source": "dexscreener",
            "price_usd": ds.get("price_usd"),
            "liquidity_usd": ds.get("liquidity_usd"),
            "volume_24h_usd": ds.get("volume_24h_usd"),
            "market_cap_usd": ds.get("market_cap_usd"),
            "price_change_24h": ds.get("price_change_24h"),
            "pair_address": ds.get("pair_address"),
            "pair_created_ms": ds.get("pair_created_ms"),
            "pool_created_at": ds.get("pool_created_at"),
            "dex": ds.get("dex"),
        }
        return snapshot

    birdeye = await fetch_birdeye(client, mint)
    if isinstance(birdeye, dict):
        payload = birdeye.get("data") or {}
        snapshot = {
            "mint": mint,
            "source": "birdeye",
            "price_usd": _as_float(payload.get("price")),
            "liquidity_usd": _as_float(payload.get("liquidity")),
            "volume_24h_usd": _as_float(payload.get("v24h")),
            "market_cap_usd": _as_float(payload.get("mc")),
            "price_change_24h": _as_float(payload.get("priceChange24h")),
        }
        return snapshot

    gecko = await fetch_gecko_market_data(client, mint)
    if gecko:
        gecko["mint"] = mint
        gecko["source"] = "geckoterminal"
        return gecko

    return None


# --------------------------------------------------------------------------------------
# Snapshot + chart helpers used by the Telegram bot
# --------------------------------------------------------------------------------------


async def extract_mint_from_check_text(
    client: httpx.AsyncClient, text: str
) -> Optional[str]:
    """Extract a mint address from free-form ``/check`` input."""

    if not text:
        return None

    text = text.strip()

    # 1) Direct base58 addresses.
    base58_re = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")
    for candidate in base58_re.findall(text):
        if is_valid_solana_address(candidate):
            return candidate

    # 2) Solscan, BirdEye, pump.fun direct token links.
    simple_patterns = (
        re.compile(r"solscan\.io/(?:token|account)/([1-9A-HJ-NP-Za-km-z]{32,44})", re.IGNORECASE),
        re.compile(r"birdeye\.so/(?:token|account)/([1-9A-HJ-NP-Za-km-z]{32,44})", re.IGNORECASE),
        re.compile(r"pump\.fun/(?:coin|board)/([1-9A-HJ-NP-Za-km-z]{32,44})", re.IGNORECASE),
    )
    for pattern in simple_patterns:
        match = pattern.search(text)
        if match and is_valid_solana_address(match.group(1)):
            return match.group(1)

    # 3) DexScreener pair links (require an API call to resolve the base mint).
    ds_match = re.search(r"dexscreener\.com/[^/]+/([A-Za-z0-9]+)", text, re.IGNORECASE)
    if ds_match:
        info = await fetch_dexscreener_pair(client, ds_match.group(1))
        if info:
            for key in ("base_mint", "mint", "quote_mint"):
                cand = info.get(key)
                if cand and is_valid_solana_address(cand):
                    return cand

    # 4) Fallback: try to fetch the URL contents when it looks like a JSON blob
    #    containing a ``mint`` field (common for third-party scanners).
    url_match = re.search(r"https?://[\w\-._~:/?#[\]@!$&'()*+,;=%]+", text)
    if url_match:
        try:
            res = await _fetch(client, url_match.group(0))
            if isinstance(res, dict):
                for key in ("mint", "tokenMint", "token"):
                    cand = res.get(key)
                    if cand and is_valid_solana_address(str(cand)):
                        return str(cand)
        except Exception:
            pass

    return None


# --------------------------------------------------------------------------------------
# Convenience structures for diagnostics
# --------------------------------------------------------------------------------------


@dataclass
class ProviderStatus:
    name: str
    success: int
    failure: int
    circuit_open: bool
    last_success: Optional[datetime]


def iter_provider_status() -> Iterable[ProviderStatus]:
    for name, stats in API_PROVIDERS.items():
        yield ProviderStatus(
            name=name,
            success=int(stats.get("success", 0)),
            failure=int(stats.get("failure", 0)),
            circuit_open=bool(stats.get("circuit_open")),
            last_success=(
                datetime.fromtimestamp(stats.get("last_success", 0), tz=timezone.utc)
                if stats.get("last_success")
                else None
            ),
        )


__all__ = [
    "API_HEALTH",
    "API_PROVIDERS",
    "LITE_MODE_UNTIL",
    "fetch_birdeye",
    "fetch_creator_dossier_bitquery",
    "fetch_dexscreener_by_mint",
    "fetch_dexscreener_chart",
    "fetch_gecko_market_data",
    "fetch_helius_asset",
    "fetch_holders_count_via_rpc",
    "fetch_ipfs_json",
    "fetch_jupiter_has_route",
    "fetch_market_snapshot",
    "fetch_rugcheck_score",
    "fetch_twitter_stats",
    "fetch_top10_via_rpc",
    "extract_mint_from_check_text",
    "iter_provider_status",
    "_fetch",
    "_is_ipfs_uri",
]

