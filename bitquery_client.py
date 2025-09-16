# -*- coding: utf-8 -*-
"""Bitquery GraphQL client and Solana-specific helper queries."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

import httpx

from config import BITQUERY_API_KEY, BITQUERY_URL, CONFIG

log = logging.getLogger("token_tony.bitquery")


async def run_query(query: str, variables: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Execute a Bitquery GraphQL query.

    Parameters
    ----------
    query:
        The GraphQL query string to execute.
    variables:
        Optional variables dict passed to the GraphQL query.

    Returns
    -------
    Optional[Dict[str, Any]]
        The ``data`` section of the GraphQL response when successful. ``None``
        is returned when Bitquery rejects the request, the API key is missing,
        or the response contains errors.
    """
    if not BITQUERY_API_KEY:
        log.debug("Bitquery API key missing – skipping query execution.")
        return None

    payload = {"query": query, "variables": variables or {}}
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": BITQUERY_API_KEY,
    }
    timeout = httpx.Timeout(CONFIG.get("HTTP_TIMEOUT", 10.0))

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(BITQUERY_URL, json=payload, headers=headers)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        log.warning("Bitquery request failed with status %s: %s", exc.response.status_code, exc)
        return None
    except httpx.HTTPError as exc:
        log.warning("Bitquery request error: %s", exc)
        return None

    try:
        payload = response.json()
    except ValueError:
        log.warning("Bitquery returned a non-JSON payload.")
        return None

    errors = payload.get("errors")
    if errors:
        log.warning("Bitquery returned errors: %s", errors)
        return None

    return payload.get("data")


# --- Creator history -----------------------------------------------------

_CREATOR_HISTORY_QUERY = """
query CreatorHistory($creator: String!, $since: ISO8601DateTime!, $limit: Int!) {
  solana(network: solana) {
    instructions(
      options: {desc: "block.timestamp.iso8601", limit: $limit}
      date: {since: $since}
      account: {is: $creator}
      programId: {is: "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"}
    ) {
      block {
        timestamp {
          iso8601
        }
      }
      instruction {
        accounts {
          address
        }
      }
    }
  }
}
"""

_CREATOR_DEX_QUERY = """
query CreatorDexSummary($mints: [String!], $since: ISO8601DateTime!) {
  solana(network: solana) {
    dexTrades(
      options: {limit: 2000}
      baseCurrency: {in: $mints}
      date: {since: $since}
    ) {
      baseCurrency {
        address
      }
      tradeAmount(in: USD)
      tradeAmountRaw: tradeAmount
      tradePrice: quotePrice
      block {
        timestamp {
          iso8601
        }
      }
      transaction {
        signature
      }
    }
  }
}
"""


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _infer_mint_from_accounts(accounts: Sequence[Dict[str, Any]]) -> Optional[str]:
    """Best-effort extraction of the mint account from an instruction."""
    for candidate in accounts:
        addr = candidate.get("address")
        if _looks_like_address(addr):
            return addr
    return None


def _looks_like_address(value: Optional[str]) -> bool:
    return bool(value and 30 <= len(value) <= 44)


def _classify_creator_token(summary: Dict[str, Any]) -> bool:
    """Determine whether the creator's token appears rugged.

    The heuristic considers the amount of trading activity and how long the
    token traded before activity stopped. The function returns ``True`` when
    it looks like a rug; otherwise ``False``.
    """
    minted_at: Optional[datetime] = summary.get("minted_at")
    last_trade: Optional[datetime] = summary.get("last_trade_at")
    volume = float(summary.get("volume_usd") or 0.0)
    trades = int(summary.get("trade_count") or 0)

    if trades == 0:
        return True

    now = datetime.now(timezone.utc)
    active_hours = None
    if minted_at and last_trade:
        active_hours = max(0.0, (last_trade - minted_at).total_seconds() / 3600.0)

    # Sudden stop shortly after launch with minimal volume: likely rugged.
    if volume < 500 and (active_hours is not None and active_hours < 4):
        return True

    # Very low volume relative to the time since mint is suspicious.
    if minted_at:
        age_days = (now - minted_at).total_seconds() / 86400.0
        if volume < 1000 and age_days > 1.5 and (not last_trade or (now - last_trade).total_seconds() > 3600 * 12):
            return True

    # No trades in the past week combined with tiny lifetime volume.
    if last_trade and (now - last_trade).total_seconds() > 3600 * 24 * 7 and volume < 5000:
        return True

    return False


def _summarize_creator_tokens(tokens: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(tokens)
    if total == 0:
        return {"tokens": [], "total_mints": 0, "rugged_tokens": 0, "recent_rugs": 0, "avg_volume_usd": 0.0}

    rugged = [t for t in tokens if t.get("rugged")]
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    recent_rugs = [t for t in rugged if t.get("minted_at") and t["minted_at"] >= thirty_days_ago]

    total_volume = sum(float(t.get("volume_usd") or 0.0) for t in tokens)
    avg_volume = total_volume / total if total else 0.0

    return {
        "tokens": tokens,
        "total_mints": total,
        "rugged_tokens": len(rugged),
        "recent_rugs": len(recent_rugs),
        "avg_volume_usd": avg_volume,
    }


async def fetch_creator_history(creator: str, *, lookback_days: int = 120, limit: int = 40) -> Optional[Dict[str, Any]]:
    """Fetch historic mint activity for a creator address.

    Parameters
    ----------
    creator:
        Solana address of the verified metadata creator.
    lookback_days:
        Range of history (in days) to inspect for prior launches.
    limit:
        Maximum number of mint events to inspect.
    """
    creator = (creator or "").strip()
    if not creator:
        return None

    since = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
    variables = {"creator": creator, "since": since, "limit": limit}
    data = await run_query(_CREATOR_HISTORY_QUERY, variables)
    if not data:
        return None

    instructions = ((data.get("solana") or {}).get("instructions") or [])
    minted_tokens: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for entry in instructions:
        inst = entry.get("instruction") or {}
        accounts = inst.get("accounts") or []
        mint = _infer_mint_from_accounts(accounts)
        if not mint or mint in seen:
            continue
        seen.add(mint)
        minted_at = _parse_ts(((entry.get("block") or {}).get("timestamp") or {}).get("iso8601"))
        minted_tokens.append({
            "mint": mint,
            "minted_at": minted_at,
            "volume_usd": 0.0,
            "trade_count": 0,
            "last_trade_at": None,
        })
        if len(minted_tokens) >= limit:
            break

    if not minted_tokens:
        return {"tokens": [], "total_mints": 0, "rugged_tokens": 0, "recent_rugs": 0, "avg_volume_usd": 0.0}

    mint_addresses = [t["mint"] for t in minted_tokens]
    dex_data = await run_query(_CREATOR_DEX_QUERY, {"mints": mint_addresses, "since": since})
    trade_rows = ((dex_data or {}).get("solana") or {}).get("dexTrades") or []

    trade_map: Dict[str, Dict[str, Any]] = {}
    for row in trade_rows:
        base = (row.get("baseCurrency") or {}).get("address")
        if not base:
            continue
        summary = trade_map.setdefault(base, {"volume_usd": 0.0, "trade_count": 0, "last_trade_at": None})
        volume = row.get("tradeAmount") or row.get("tradeAmountRaw")
        try:
            summary["volume_usd"] += float(volume or 0.0)
        except Exception:
            pass
        summary["trade_count"] += 1
        ts = _parse_ts(((row.get("block") or {}).get("timestamp") or {}).get("iso8601"))
        if ts and (summary["last_trade_at"] is None or ts > summary["last_trade_at"]):
            summary["last_trade_at"] = ts

    enriched_tokens: List[Dict[str, Any]] = []
    for token in minted_tokens:
        stats = trade_map.get(token["mint"]) or {}
        token["volume_usd"] = float(stats.get("volume_usd") or 0.0)
        token["trade_count"] = int(stats.get("trade_count") or 0)
        token["last_trade_at"] = stats.get("last_trade_at")
        token["rugged"] = _classify_creator_token(token)
        enriched_tokens.append(token)

    summary = _summarize_creator_tokens(enriched_tokens)
    summary["creator"] = creator
    summary["lookback_since"] = since
    return summary


# --- Holder snapshot -----------------------------------------------------

_HOLDER_SNAPSHOT_QUERY = """
query HolderSnapshot($mint: String!, $limit: Int!) {
  solana(network: solana) {
    balances: tokenHolders(
      options: {desc: "amount", limit: $limit}
      tokenAddress: {is: $mint}
    ) {
      account {
        address
      }
      owner {
        address
      }
      amount
      amountInUSD: amount(in: USD)
      amountPercentage
    }
    aggregate: tokenHoldersAggregate(
      tokenAddress: {is: $mint}
    ) {
      count
    }
    token: tokens(
      options: {limit: 1}
      address: {is: $mint}
    ) {
      address
      symbol
      decimals
      supply
    }
  }
}
"""


def _normalize_addresses(values: Optional[Iterable[str]]) -> List[str]:
    out: List[str] = []
    if not values:
        return out
    for value in values:
        if not value:
            continue
        v = value.strip()
        if not v:
            continue
        if v not in out:
            out.append(v)
    return out


async def fetch_holder_snapshot(
    mint: str,
    *,
    limit: int = 25,
    total_supply: Optional[float] = None,
    insider_addresses: Optional[Iterable[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Return a snapshot of the top holders for a mint."""
    mint = (mint or "").strip()
    if not mint:
        return None

    variables = {"mint": mint, "limit": limit}
    data = await run_query(_HOLDER_SNAPSHOT_QUERY, variables)
    if not data:
        return None

    solana_data = data.get("solana") or {}
    holders_raw = solana_data.get("balances") or solana_data.get("tokenHolders") or []
    aggregate = solana_data.get("aggregate") or {}
    holder_count = aggregate.get("count")

    token_meta = None
    token_meta_list = solana_data.get("token") or solana_data.get("tokens") or []
    if token_meta_list:
        token_meta = token_meta_list[0]

    supply = total_supply
    try:
        if supply is None and token_meta is not None:
            raw_supply = token_meta.get("supply")
            if raw_supply is not None:
                supply = float(raw_supply)
            decimals = token_meta.get("decimals")
            if supply is not None and decimals is not None:
                supply = supply / (10 ** int(decimals))
    except Exception:
        pass

    holders: List[Dict[str, Any]] = []
    for row in holders_raw:
        address = ((row.get("account") or {}).get("address")
                   or (row.get("owner") or {}).get("address")
                   or row.get("address"))
        if not address:
            continue
        try:
            amount = float(row.get("amount") or row.get("balance") or 0.0)
        except Exception:
            amount = 0.0
        try:
            amount_usd = float(row.get("amountInUSD") or row.get("value") or 0.0)
        except Exception:
            amount_usd = 0.0
        try:
            share = float(row.get("amountPercentage") or row.get("share") or row.get("percentage") or 0.0)
        except Exception:
            share = 0.0
        if share == 0.0 and supply:
            try:
                share = (amount / supply) * 100 if supply else 0.0
            except Exception:
                share = 0.0

        holders.append({
            "address": address,
            "amount": amount,
            "amount_usd": amount_usd,
            "share": share,
        })

    insider_set = {addr.lower() for addr in _normalize_addresses(insider_addresses)}

    whales = [h for h in holders if h.get("share") and h["share"] >= 5.0]
    sharks = [h for h in holders if h.get("share") and 2.0 <= h["share"] < 5.0]
    retail = [h for h in holders if h.get("share") and h["share"] < 1.0]
    insider_share = sum(h["share"] for h in holders if h.get("share") and h["address"].lower() in insider_set)
    whale_share = sum(h["share"] for h in whales if h.get("share"))
    top10_share = sum(h["share"] for h in holders[:10] if h.get("share"))
    retail_share = sum(h["share"] for h in retail if h.get("share"))

    snapshot = {
        "mint": mint,
        "holders": holders,
        "holder_count": holder_count or len(holders),
        "supply": supply,
        "whale_share": whale_share,
        "insider_share": insider_share,
        "retail_share": retail_share,
        "top10_share": top10_share,
        "segments": {
            "whales": len(whales),
            "sharks": len(sharks),
            "retail": len(retail),
        },
    }
    return snapshot


__all__ = [
    "run_query",
    "fetch_creator_history",
    "fetch_holder_snapshot",
]
