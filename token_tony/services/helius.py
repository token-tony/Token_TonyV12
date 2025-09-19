# -*- coding: utf-8 -*-
"""Helius API integration for Token Tony."""
from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from config import HELIUS_API_KEY, HELIUS_RPC_URL
from http_client import _fetch


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
