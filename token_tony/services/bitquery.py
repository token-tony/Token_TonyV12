# -*- coding: utf-8 -*-
"""BitQuery API integration for Token Tony."""
from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from config import BITQUERY_API_KEY
from http_client import _fetch


async def fetch_creator_dossier_bitquery(client: httpx.AsyncClient, creator: str) -> Optional[int]:
    if not BITQUERY_API_KEY:
        return None
    query = """
    query ($creator: String!) {
      solana {
        minting: transfers(
          transferType: {is: Mint}
          receiverAddress: {is: $creator}
        ) {
          count
        }
      }
    }
    """
    variables = {"creator": creator}
    headers = {
        "X-API-KEY": BITQUERY_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {"query": query, "variables": variables}
    result = await _fetch(
        client,
        "https://graphql.bitquery.io",
        method="POST",
        json=payload,
        headers=headers,
        provider="bitquery",
    )
    if not isinstance(result, dict):
        return None
    try:
        transfers = (
            result.get("data", {})
            .get("solana", {})
            .get("minting", [])
        )
        if transfers:
            return int(transfers[0].get("count") or 0)
    except Exception:
        pass
    return None
