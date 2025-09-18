# -*- coding: utf-8 -*-
"""Shared httpx client for Token Tony."""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from threading import Lock
from typing import Any, Dict, Iterable, Optional

import httpx

from config import CONFIG
from utils import HTTP_LIMITER

log = logging.getLogger("tony_helpers.http_client")

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


_HTTP_CLIENT: Optional[httpx.AsyncClient] = None
_HTTP_CLIENT_DS: Optional[httpx.AsyncClient] = None  # DexScreener prefers HTTP/1.1 in practice

async def get_http_client(*, ds: bool = False) -> httpx.AsyncClient:
    """Get the shared httpx client."""
    global _HTTP_CLIENT, _HTTP_CLIENT_DS
    if ds:
        if _HTTP_CLIENT_DS is None:
            # Use HTTP/1.1 for DexScreener endpoints to avoid edge-caching oddities
            _HTTP_CLIENT_DS = httpx.AsyncClient(http2=False, timeout=CONFIG["HTTP_TIMEOUT"])  # re-used across tasks
        return _HTTP_CLIENT_DS
    if _HTTP_CLIENT is None:
        _HTTP_CLIENT = httpx.AsyncClient(http2=True, timeout=CONFIG["HTTP_TIMEOUT"])  # re-used across tasks
    return _HTTP_CLIENT

async def close_http_clients():
    """Close the shared httpx clients."""
    global _HTTP_CLIENT, _HTTP_CLIENT_DS
    if _HTTP_CLIENT is not None:
        await _HTTP_CLIENT.aclose()
        _HTTP_CLIENT = None
    if _HTTP_CLIENT_DS is not None:
        await _HTTP_CLIENT_DS.aclose()
        _HTTP_CLIENT_DS = None


async def fetch(
    url: str,
    *,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    json_data: Optional[Any] = None,
    data: Optional[Any] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
    provider: Optional[str] = None,
    allow_status: Iterable[int] = (200,),
    retries: Optional[int] = None,
    ds: bool = False,
) -> Optional[Any]:
    """Generic HTTP helper with retries and circuit breaker integration."""

    provider_name = provider or _infer_provider_from_url(url) or "generic"
    stats = _ensure_provider(provider_name)
    if stats.get("circuit_open") and time.time() < stats.get("circuit_expires", 0.0):
        log.debug("Skipping %s request to %s (circuit open)", provider_name, url)
        return None

    client = await get_http_client(ds=ds)
    attempts = (int(CONFIG.get("HTTP_RETRIES", 2) or 2) + 1) if retries is None else max(1, retries + 1)
    timeout_val = timeout if timeout is not None else float(CONFIG.get("HTTP_TIMEOUT", 15.0) or 15.0)
    last_error: Optional[Exception] = None

    # Apply rate limiting
    await HTTP_LIMITER.limit(provider_name)

    for attempt in range(attempts):
        start = time.perf_counter()
        try:
            response = await client.request(
                method,
                url,
                params=params,
                json=json_data,
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