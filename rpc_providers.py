# -*- coding: utf-8 -*-
"""Shared Solana RPC provider helpers.

This module centralizes the list of HTTP RPC endpoints that Token Tony can
use and exposes a helper for sending JSON-RPC POST requests with automatic
failover between the configured providers.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

from http_client import fetch
from config import (
    ALCHEMY_RPC_URL,
    CONFIG,
    HELIUS_RPC_URL,
    SYNDICA_RPC_URL,
)

log = logging.getLogger("token_tony.rpc")


def _build_providers() -> List[Tuple[str, str]]:
    providers: List[Tuple[str, str]] = []
    for name, url in (
        ("Syndica", (SYNDICA_RPC_URL or "").strip()),
        ("Alchemy", (ALCHEMY_RPC_URL or "").strip()),
        ("Helius", (HELIUS_RPC_URL or "").strip()),
    ):
        if url:
            providers.append((name, url))
    return providers


RPC_PROVIDERS: List[Tuple[str, str]] = _build_providers()


async def rpc_post(payload: Dict) -> Dict:
    """Send a JSON-RPC POST request with provider failover."""
    if not RPC_PROVIDERS:
        raise RuntimeError("No RPC providers configured.")

    errors: List[str] = []

    for name, url in RPC_PROVIDERS:
        try:
            res = await fetch(url, method="POST", json_data=payload, timeout=10.0, provider=name.lower())
        except Exception as exc:  # noqa: BLE001
            msg = f"RPC provider {name} failed: {exc}"
            log.warning(msg)
            errors.append(msg)
            continue

        if res:
            if errors:
                log.info("RPC provider %s succeeded after %d fallback(s).", name, len(errors))
            return res

        errors.append(f"RPC provider {name} returned no data.")

    raise RuntimeError("; ".join(errors) if errors else "All RPC providers failed.")
