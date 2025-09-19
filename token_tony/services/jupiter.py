# -*- coding: utf-8 -*-
"""Jupiter API integration for Token Tony."""
from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from config import JUP_QUOTE_URL, USDC_MINT
from http_client import _fetch


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
