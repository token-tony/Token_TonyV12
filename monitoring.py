# -*- coding: utf-8 -*-
"""Provider reliability monitoring helpers."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict


@dataclass
class ProviderMetrics:
    """Runtime metrics collected for upstream providers."""

    failures: int = 0
    successes: int = 0
    last_error: float = 0.0
    last_success: float = 0.0
    last_mint: str = ""
    last_exception: str = ""


_PROVIDER_METRICS: Dict[str, ProviderMetrics] = {}


def record_provider_failure(provider: str, mint: str, exc: Exception) -> None:
    """Record a failure for the given provider and mint."""
    stats = _PROVIDER_METRICS.setdefault(provider, ProviderMetrics())
    stats.failures += 1
    stats.last_error = time.time()
    stats.last_mint = mint
    stats.last_exception = str(exc)[:300]


def record_provider_success(provider: str, mint: str = "") -> None:
    """Record a success for the given provider."""
    stats = _PROVIDER_METRICS.setdefault(provider, ProviderMetrics())
    stats.successes += 1
    stats.last_success = time.time()
    if mint:
        stats.last_mint = mint


def get_provider_metrics() -> Dict[str, ProviderMetrics]:
    """Return a shallow copy of the current provider metrics."""
    return dict(_PROVIDER_METRICS)
