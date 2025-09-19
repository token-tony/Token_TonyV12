# -*- coding: utf-8 -*-
"""GeckoTerminal API integration for Token Tony."""
from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from config import GECKO_API_URL
from http_client import _fetch


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
