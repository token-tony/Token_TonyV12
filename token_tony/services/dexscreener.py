# -*- coding: utf-8 -*-
"""DexScreener API integration for Token Tony."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx

from http_client import _fetch


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
