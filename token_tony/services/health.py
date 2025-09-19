# -*- coding: utf-8 -*-
"""Provider health tracking and circuit breaker state for Token Tony."""
from __future__ import annotations

import logging
import time
from threading import Lock
from typing import Any, Dict

from config import CONFIG

log = logging.getLogger(__name__)

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
