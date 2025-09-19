# -*- coding: utf-8 -*-
"""Shared httpx client for Token Tony."""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any, Dict, Iterable, Optional

import httpx

from config import CONFIG, API_RATE_LIMITS
from token_tony.services.health import _ensure_provider, _record_failure, _record_success, _infer_provider_from_url

log = logging.getLogger("tony_helpers.http_client")

class TokenBucket:
    def __init__(self, capacity: int, refill_amount: int, interval_seconds: float) -> None:
        self.capacity = max(1, capacity)
        self.tokens = float(capacity)
        self.refill_amount = float(refill_amount)
        self.interval = float(interval_seconds)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, amount: float = 1.0) -> None:
        amount = float(amount)
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._last)
                if elapsed >= self.interval:
                    # Add whole-interval refills for stability under load
                    intervals = int(elapsed // self.interval)
                    self.tokens = min(self.capacity, self.tokens + intervals * self.refill_amount)
                    self._last = now if intervals > 0 else self._last
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
                # Compute time until next token becomes available
                needed = amount - self.tokens
                rate_per_sec = (self.refill_amount / self.interval) if self.interval > 0 else self.refill_amount
                wait = max(0.01, needed / max(1e-6, rate_per_sec))
            # jitter to avoid thundering herd
            await asyncio.sleep(min(2.0, wait + random.uniform(0, 0.05)))


class HttpRateLimiter:
    """Endpoint/host aware limiters.
    Define buckets by string keys; call await limit('key') before HTTP calls.
    """

    def __init__(self) -> None:
        self._buckets: Dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()
        for provider, limits in API_RATE_LIMITS.items():
            self._buckets[provider] = TokenBucket(
                capacity=limits["capacity"],
                refill_amount=limits["refill"],
                interval_seconds=limits["interval"],
            )

    async def ensure_bucket(self, key: str, capacity: int, refill: int, interval: float) -> TokenBucket:
        async with self._lock:
            if key not in self._buckets:
                self._buckets[key] = TokenBucket(capacity, refill, interval)
            return self._buckets[key]

    async def limit(self, key: str) -> None:
        bucket = self._buckets.get(key)
        if bucket is None:
            # Default conservative bucket if unknown
            bucket = await self.ensure_bucket("generic", capacity=10, refill=10, interval=1.0)
        await bucket.acquire(1.0)

HTTP_LIMITER = HttpRateLimiter()

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
