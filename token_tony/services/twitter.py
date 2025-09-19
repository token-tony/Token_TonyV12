# -*- coding: utf-8 -*-
"""Twitter API integration for Token Tony."""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

import httpx

from config import X_BEARER_TOKEN
from http_client import _fetch


async def fetch_twitter_stats(client: httpx.AsyncClient, url_or_handle: str) -> Optional[Dict[str, Any]]:
    if not X_BEARER_TOKEN:
        return None
    handle = url_or_handle.strip()
    if handle.startswith("http"):
        match = re.search(r"twitter\.com/(?:#!\/)?([^/?#]+)", handle)
        if not match:
            match = re.search(r"x\.com/(?:#!\/)?([^/?#]+)", handle)
        handle = match.group(1) if match else handle
    handle = handle.lstrip("@")
    if not handle:
        return None
    url = f"https://api.twitter.com/2/users/by/username/{handle}"
    params = {"user.fields": "public_metrics,created_at"}
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
    user = await _fetch(client, url, params=params, headers=headers, provider="twitter")
    if not isinstance(user, dict):
        return None
    data = user.get("data") or {}
    metrics = data.get("public_metrics") or {}
    try:
        return {
            "username": data.get("username"),
            "name": data.get("name"),
            "created_at": data.get("created_at"),
            "followers": int(metrics.get("followers_count", 0) or 0),
            "following": int(metrics.get("following_count", 0) or 0),
            "tweet_count": int(metrics.get("tweet_count", 0) or 0),
            "listed_count": int(metrics.get("listed_count", 0) or 0),
        }
    except Exception:
        return None
