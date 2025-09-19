# -*- coding: utf-8 -*-
"""IPFS integration for Token Tony."""
from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict, Optional

import httpx

from config import CONFIG
from http_client import _fetch

_IPFS_PREFIX = re.compile(r"^ipfs://", re.IGNORECASE)


def _is_ipfs_uri(uri: str) -> bool:
    return bool(uri and _IPFS_PREFIX.match(uri))


async def fetch_ipfs_json(client: httpx.AsyncClient, uri: str) -> Optional[Dict[str, Any]]:
    """Resolve an IPFS URI using a set of HTTP gateways."""
    if not _is_ipfs_uri(uri):
        result = await _fetch(client, uri, provider="ipfs")
        return result if isinstance(result, dict) else None

    cid_path = uri[7:]
    if "/" in cid_path:
        cid, path = cid_path.split("/", 1)
        suffix = "/" + path
    else:
        cid, suffix = cid_path, ""

    gateways = [
        f"https://cloudflare-ipfs.com/ipfs/{cid}{suffix}",
        f"https://ipfs.io/ipfs/{cid}{suffix}",
        f"https://gateway.pinata.cloud/ipfs/{cid}{suffix}",
    ]
    timeout_s = float(CONFIG.get("IPFS_FETCH_TIMEOUT_SECONDS", 5.0) or 5.0)

    for idx, url in enumerate(gateways):
        result = await _fetch(client, url, timeout=timeout_s, provider="ipfs")
        if isinstance(result, (dict, list)):
            return result  # type: ignore[return-value]
        if isinstance(result, (bytes, bytearray)):
            try:
                return json.loads(result.decode("utf-8"))
            except Exception:
                pass
        if isinstance(result, str):
            try:
                return json.loads(result)
            except json.JSONDecodeError:
                pass
        # Hedge after the first gateway if configured
        hedge_ms = int(CONFIG.get("IPFS_HEDGE_MS", 0) or 0)
        if idx == 0 and hedge_ms > 0:
            await asyncio.sleep(hedge_ms / 1000.0)
    return None
