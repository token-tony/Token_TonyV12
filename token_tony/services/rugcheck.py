# -*- coding: utf-8 -*-
"""RugCheck API integration for Token Tony."""
from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from config import RUGCHECK_API_URL, RUGCHECK_JWT
from http_client import _fetch


async def fetch_rugcheck_score(client: httpx.AsyncClient, mint: str) -> Optional[str]:
    url = f"{RUGCHECK_API_URL.rstrip('/')}/token/{mint}"
    headers = {"Accept": "application/json"}
    if RUGCHECK_JWT:
        headers["Authorization"] = f"Bearer {RUGCHECK_JWT}"
    result = await _fetch(client, url, headers=headers, provider="rugcheck")
    if not isinstance(result, dict):
        return None
    summary = result.get("summary") or {}
    label = summary.get("risk") or summary.get("label")
    if label:
        return str(label)
    if result.get("risk"):
        return str(result["risk"])
    return None
