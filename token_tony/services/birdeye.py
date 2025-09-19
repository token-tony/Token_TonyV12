# -*- coding: utf-8 -*-
"""BirdEye API integration for Token Tony."""
from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from config import BIRDEYE_API_KEY
from http_client import _fetch
from utils import HTTP_LIMITER


async def fetch_birdeye(client: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    if not BIRDEYE_API_KEY:
        return None
    url = f"https://public-api.birdeye.so/public/marketstat/solana/{mint}"
    headers = {"X-API-KEY": BIRDEYE_API_KEY, "Accept": "application/json"}
    # Strict global Birdeye rate: 1 request/second
    try:
        await HTTP_LIMITER.ensure_bucket("birdeye", capacity=1, refill=1, interval=1.0)
        await HTTP_LIMITER.limit("birdeye")
    except Exception:
        # If limiter unavailable for any reason, proceed but rely on circuit breaker
        pass
    result = await _fetch(client, url, headers=headers, provider="birdeye")
    return result if isinstance(result, dict) else None
