# -*- coding: utf-8 -*-
"""Solana-related functions for Token Tony."""
from __future__ import annotations

from typing import Optional

import httpx

from config import HELIUS_RPC_URL, TOKEN_PROGRAM_ID, TOKEN2022_PROGRAM_ID
from http_client import _fetch


async def fetch_holders_via_program_accounts(
    client: httpx.AsyncClient, mint: str
) -> Optional[int]:
    """Approximate holders via programAccounts scanning (fallback)."""
    if not HELIUS_RPC_URL:
        return None
    payload = {
        "jsonrpc": "2.0",
        "id": "token-tony",
        "method": "getProgramAccounts",
        "params": [
            TOKEN_PROGRAM_ID,
            {
                "encoding": "base64",
                "filters": [
                    {"dataSize": 165},
                    {
                        "memcmp": {
                            "offset": 0,
                            "bytes": mint,
                        }
                    },
                ],
            },
        ],
    }
    result = await _fetch(client, HELIUS_RPC_URL, method="POST", json=payload, provider="helius")
    if not isinstance(result, dict):
        # Try Token2022 program as fallback
        payload["params"][0] = TOKEN2022_PROGRAM_ID
        result = await _fetch(client, HELIUS_RPC_URL, method="POST", json=payload, provider="helius")
    if not isinstance(result, dict):
        return None
    try:
        return len(result.get("result", []) or [])
    except Exception:
        return None
