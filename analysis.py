# -*- coding: utf-8 -*-
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from cachetools import TTLCache

from config import CONFIG, HELIUS_API_KEY
from tony_helpers.api import (_is_ipfs_uri, fetch_birdeye,
                              fetch_creator_dossier_bitquery,
                              fetch_dexscreener_by_mint,
                              fetch_gecko_market_data, fetch_helius_asset,
                              fetch_holders_count_via_rpc, fetch_ipfs_json,
                              fetch_jupiter_has_route, fetch_rugcheck_score,
                              fetch_top10_via_rpc, fetch_twitter_stats)
from tony_helpers.db import _execute_db

log = logging.getLogger("token_tony.analysis")

# --- Caches ---
_intel_cache: TTLCache = TTLCache(maxsize=200, ttl=120)
POOL_BIRTH_CACHE: TTLCache = TTLCache(maxsize=1000, ttl=3600)
GECKO_SEARCH_CACHE: TTLCache = TTLCache(maxsize=500, ttl=600)
# Cache for DexScreener /latest/dex/pairs/solana/new endpoint results
DS_NEW_CACHE: TTLCache = TTLCache(maxsize=200, ttl=180)

def _compute_sss(i: Dict[str, Any]) -> int:
    """Calculates a score based on immediate, on-chain rugpull risks."""
    cfg = CONFIG.get("SSS_SCORING", {})
    score = float(cfg.get("base_score", 80))

    # Strong penalty for active authorities (but not hard zero)
    if i.get("mint_authority") or i.get("freeze_authority"):
        score -= float(cfg.get("authority_penalty", 60))

    pct = i.get("top10_holder_percentage")
    if pct is not None:
        try:
            pct_val = float(pct)
        except (TypeError, ValueError):
            pct_val = None
        if pct_val is not None:
            thresholds = cfg.get("top_holder_thresholds", [80, 60, 40])
            penalties = cfg.get("top_holder_penalties", [40, 25, 10])
            bucketed: List[tuple[float, float]] = []
            for threshold, penalty in zip(thresholds, penalties):
                try:
                    bucketed.append((float(threshold), float(penalty)))
                except (TypeError, ValueError):
                    continue
            for threshold, penalty in sorted(bucketed, key=lambda x: x[0], reverse=True):
                if pct_val >= threshold:
                    score -= penalty
                    break

    rug_score = i.get("rugcheck_score", "")
    if isinstance(rug_score, str) and "high risk" in rug_score.lower():
        score -= float(cfg.get("rug_high_risk_penalty", 30))

    count = i.get("creator_token_count", 0)
    try:
        count_val = int(count)
    except (TypeError, ValueError):
        count_val = 0
    start = int(cfg.get("creator_penalty_start", 5))
    if count_val > start:
        per_token = float(cfg.get("creator_penalty_per_token", 3))
        cap = float(cfg.get("creator_penalty_cap", 25))
        penalty = min((count_val - start) * per_token, cap)
        score -= penalty

    return max(0, int(score))

def _compute_mms(i: Dict[str, Any]) -> int:
    """Market health with age-aware expectations."""
    cfg = CONFIG.get("MMS_SCORING", {})
    liq = float(i.get("liquidity_usd") or 0)
    vol = float(i.get("volume_24h_usd") or 0)
    mc = float(i.get("market_cap_usd") or 0)
    age_m = float(i.get("age_minutes") or 0)

    brackets = cfg.get("age_brackets") or []
    bracket: Dict[str, Any] = {}
    for candidate in brackets:
        max_age = candidate.get("max_age_minutes")
        try:
            max_age_val = float(max_age) if max_age is not None else None
        except (TypeError, ValueError):
            max_age_val = None
        if max_age_val is None or age_m < max_age_val:
            bracket = candidate
            break
    if not bracket and brackets:
        bracket = brackets[-1]

    weights = bracket.get("weights", {})
    norms = bracket.get("norms", {})
    liq_weight = float(weights.get("liquidity", 0.35))
    vol_weight = float(weights.get("volume", 0.35))
    mc_weight = float(weights.get("market_cap", 0.2))
    liq_norm = float(norms.get("liquidity", 5_000))
    vol_norm = float(norms.get("volume", 25_000))
    mc_norm = float(norms.get("market_cap", 50_000))
    cap = int(bracket.get("cap", 90))

    def norm(x: float, k: float) -> float:
        try:
            k_val = float(k)
        except (TypeError, ValueError):
            k_val = 0.0
        if x < 0:
            return 0.0
        if k_val <= 0:
            return 1.0 if x > 0 else 0.0
        return x / (x + k_val)

    score = 0.0
    score += liq_weight * 100 * norm(liq, liq_norm)
    score += vol_weight * 100 * norm(vol, vol_norm)
    score += mc_weight * 100 * norm(mc, mc_norm)

    follower_cfg = cfg.get("twitter_followers", {})
    if (stats := i.get("twitter_stats")):
        followers = int(stats.get("followers", 0) or 0)
        weight = float(follower_cfg.get("weight", 10.0))
        follower_norm = float(follower_cfg.get("norm", 10_000))
        score += weight * norm(followers, follower_norm)

    volume_clamps = cfg.get("volume_clamp_rules") or [
        {"min_age_minutes": 1440, "max_volume": 1_000, "cap": 20},
        {"min_age_minutes": 360, "max_volume": 500, "cap": 25},
        {"min_age_minutes": 0, "max_volume": 100, "cap": 15},
    ]
    for clamp in volume_clamps:
        try:
            min_age = float(clamp.get("min_age_minutes", 0) or 0)
            max_volume = float(clamp.get("max_volume", 0) or 0)
            clamp_cap = float(clamp.get("cap", cap))
        except (TypeError, ValueError):
            continue
        if age_m >= min_age and vol < max_volume:
            score = min(score, clamp_cap)
            break

    try:
        pchg = abs(float(i.get("price_change_24h") or 0.0))
    except Exception:
        pchg = 0.0
    price_cap_cfg = cfg.get("price_change_cap") or {"max_volume": 100, "max_price_change": 0.1, "cap": 10}
    try:
        price_cap_volume = float(price_cap_cfg.get("max_volume", 100) or 0)
        price_cap_delta = float(price_cap_cfg.get("max_price_change", 0.1) or 0)
        price_cap_value = float(price_cap_cfg.get("cap", 10))
    except (TypeError, ValueError):
        price_cap_volume = 100.0
        price_cap_delta = 0.1
        price_cap_value = 10.0
    if vol < price_cap_volume and pchg < price_cap_delta:
        score = min(score, price_cap_value)

    lv_cfg = cfg.get("liquidity_volume_cap") or {"min_liquidity": 100_000, "max_volume": 1_000, "cap": 20}
    try:
        min_liq = float(lv_cfg.get("min_liquidity", 100_000) or 0)
        lv_max_vol = float(lv_cfg.get("max_volume", 1_000) or 0)
        lv_cap = float(lv_cfg.get("cap", 20))
    except (TypeError, ValueError):
        min_liq = 100_000.0
        lv_max_vol = 1_000.0
        lv_cap = 20.0
    if liq > min_liq and vol < lv_max_vol:
        score = min(score, lv_cap)

    return max(0, min(int(score), cap))

def _compute_score(intel: Dict[str, Any]) -> int:
    """Blends the SSS and MMS based on the token's age."""
    sss = intel.get("sss_score", 0)
    mms = intel.get("mms_score", 0)
    age_days = (intel.get("age_minutes") or 0) / 1440

    if age_days < 7: final_score = (sss * 0.5) + (mms * 0.5)
    elif age_days <= 30: final_score = (sss * 0.35) + (mms * 0.65)
    else: final_score = (sss * 0.25) + (mms * 0.75)

    # Apply uncertainty drag so incomplete data can't produce extreme scores
    q = _score_confidence(intel)  # 0.3 .. 1.0
    final_score = final_score * q
    return max(0, min(int(final_score), 100))

def _score_confidence(i: Dict[str, Any]) -> float:
    """Estimate data quality/recency confidence for scoring.
    Returns a factor in [0.3, 1.0] used to temper extremes when data is sparse.
    """
    signals = 0
    present = 0
    for k in ("liquidity_usd", "market_cap_usd", "volume_24h_usd", "age_minutes"):
        signals += 1
        v = i.get(k)
        if v is not None:
            present += 1
    # Rugcheck presence counts as a signal (we just need the field to exist)
    signals += 1
    if i.get("rugcheck_score") is not None:
        present += 1
    # Bound confidence
    base = 0.3 + 0.7 * (present / max(1, signals))
    # If age is missing entirely, cap lower
    if i.get("age_minutes") is None:
        base = min(base, 0.6)
    return float(max(0.3, min(1.0, base)))

async def enrich_token_intel(c: httpx.AsyncClient, mint: str, deep_dive: bool = False) -> Optional[Dict[str, Any]]:
    """The heart of the analysis pipeline. Gathers all data and calculates scores."""
    from tony_helpers.api import _fetch
    cache_key = f"{mint}:{deep_dive}";
    if cache_key in _intel_cache: return _intel_cache[cache_key]
    
    # Step 1: Gather all primary data sources concurrently for efficiency
    helius_task = fetch_helius_asset(c, mint)
    rugcheck_task = fetch_rugcheck_score(c, mint)
    # Fetch BirdEye in parallel (may be stale), but we will prefer DexScreener below
    birdeye_task = fetch_birdeye(c, mint)
    results = await asyncio.gather(helius_task, rugcheck_task, birdeye_task, return_exceptions=True)

    helius_data = results[0] if not isinstance(results[0], Exception) else None
    rugcheck_score = results[1] if not isinstance(results[1], Exception) else "N/A"
    birdeye_raw = results[2] if not isinstance(results[2], Exception) else None
    market_data = birdeye_raw

    # Normalize BirdEye response if present; otherwise trigger fallbacks
    if market_data and isinstance(market_data.get("data"), dict):
        be = market_data["data"]
        market_data = {
            "liquidity_usd": float(be.get("liquidity", 0.0)),
            "market_cap_usd": float(be.get("mc", 0.0)),
            "volume_24h_usd": float(be.get("v24h", 0.0)),
            "price_change_24h": float(be.get("priceChange24h", 0.0)),
            # Note: pair_address not available from BirdEye; may be filled by fallbacks
        }
        # If BirdEye provides holders, capture it
        try:
            int(be.get("holders")) if be.get("holders") is not None else None
        except Exception:
            pass
    elif market_data:
        # BirdEye returned but with no usable data
        market_data = None

    # Step 2: Prefer DexScreener live data; if unavailable, use BirdEye (normalized above) or GeckoTerminal
    try:
        ds_now = await fetch_dexscreener_by_mint(c, mint)
        if ds_now:
            market_data = ds_now
    except Exception:
        pass
    if not market_data:
        log.warning(f"No DexScreener for {mint}, trying GeckoTerminal.")
        market_data = await fetch_gecko_market_data(c, mint)
    
    # Step 3: Build the intel object from whatever data we have
    # Do NOT drop the token on upstream failures; proceed with minimal intel so it can be tagged/bucketed.
    if not helius_data and not market_data:
        log.warning(f"Sparse intel for {mint}: core and market data unavailable. Proceeding with minimal fields.")
    
    intel = {"mint": mint, "rugcheck_score": rugcheck_score, "socials": {}}

    if helius_data and (core := helius_data.get("result")):
        creation_dt = None
        if created_at_ts := core.get("created_at"):
            try:
                # Helius provides a Unix timestamp, not an ISO string.
                creation_dt = datetime.fromtimestamp(int(created_at_ts), tz=timezone.utc)
            except (ValueError, TypeError):
                log.warning(f"Could not parse creation_dt timestamp for {mint}: {created_at_ts}")

        content = core.get("content", {})
        meta_blk = (content.get("metadata") or {})
        intel["name"] = meta_blk.get("name", "Unnamed")
        intel["symbol"] = meta_blk.get("symbol", "N/A")
        # Metadata mutability/update authority indicator
        intel["metadata_mutable"] = bool(content.get("mutable") if content.get("mutable") is not None else meta_blk.get("mutable", False))
        intel["metadata_update_authority"] = meta_blk.get("updateAuthority") or meta_blk.get("update_authority")
        intel["creator_address"] = next((cr.get("address") for cr in core.get("creators", []) if cr.get("verified")), None)
        intel["mint_authority"] = core.get("mint_info", {}).get("mint_authority") # This is often None for SPL tokens, which is correct.
        intel["freeze_authority"] = core.get("mint_info", {}).get("freeze_authority")
        if creation_dt:
            intel["created_at"] = creation_dt.isoformat()
            intel["age_minutes"] = (datetime.now(timezone.utc) - creation_dt).total_seconds() / 60

        if token_info := core.get("token_info"):
            try:
                supply = int(token_info.get("supply", "0"))
                holders_list = token_info.get("holders") or []
                intel["holders_count"] = len(holders_list) if isinstance(holders_list, list) else None
                if supply > 0 and holders_list:
                    top10_sum = sum(int(acc.get("amount", "0")) for acc in holders_list[:10])
                    intel["top10_holder_percentage"] = round((top10_sum / supply) * 100.0, 1)
            except (ValueError, TypeError, ZeroDivisionError) as e:
                log.warning(f"Could not calculate top 10 holders for {mint}: {e}")

        # If Helius didn't include holders, fall back to direct RPC to compute top10 concentration
        if intel.get("top10_holder_percentage") is None and HELIUS_API_KEY:
            try:
                top10_res = await fetch_top10_via_rpc(c, mint)
                if top10_res:
                    intel.update(top10_res)
            except Exception:
                pass

        if metadata_uri := core.get("content", {}).get("json_uri"):
            # Prefer robust IPFS resolution with gateway fallback
            if _is_ipfs_uri(metadata_uri):
                meta_res = await fetch_ipfs_json(c, metadata_uri)
            else:
                meta_res = await _fetch(c, metadata_uri)
            if meta_res and isinstance(meta_res, dict):
                socials = {}
                if url := meta_res.get("external_url"): socials["Website"] = url
                if url := meta_res.get("telegram"): socials["Telegram"] = url
                if url := meta_res.get("twitter", meta_res.get("extensions", {}).get("twitter")):
                    socials["Twitter"] = url if "twitter.com" in url else f"https://twitter.com/{url}"
                intel["socials"] = socials

    if market_data:
        intel.update(market_data)
    # Fill holders from BirdEye if we saw it
    if intel.get("holders_count") in (None, 0):
        try:
            if birdeye_raw and isinstance(birdeye_raw.get("data"), dict):
                hv = birdeye_raw["data"].get("holders")
                if hv is not None:
                    intel["holders_count"] = int(hv)
        except Exception:
            pass

    # Prefer pool creation time for age if available
    if market_data and isinstance(market_data, dict):
        created_ms = market_data.get("pair_created_ms")
        created_iso = market_data.get("pool_created_at")
        pool_created_dt = None
        try:
            if created_ms:
                pool_created_dt = datetime.fromtimestamp(int(created_ms) / 1000, tz=timezone.utc)
            elif created_iso:
                pool_created_dt = datetime.fromisoformat(str(created_iso).replace("Z", "+00:00"))
        except Exception:
            pool_created_dt = None
        if pool_created_dt:
            intel["created_at_pool"] = pool_created_dt.isoformat()
            intel["age_minutes"] = (datetime.now(timezone.utc) - pool_created_dt).total_seconds() / 60
    # Also prefer our own logs birth cache when available
    try:
        if mint in POOL_BIRTH_CACHE and not intel.get("created_at_pool"):
            bt = int(POOL_BIRTH_CACHE[mint])
            dt = datetime.fromtimestamp(bt, tz=timezone.utc)
            intel["created_at_pool"] = dt.isoformat()
            intel["age_minutes"] = (datetime.now(timezone.utc) - dt).total_seconds() / 60
    except Exception:
        pass

    # Ensure we always have an age estimate even without deep dive
    if "age_minutes" not in intel:
        discovered_row = await _execute_db("SELECT discovered_at FROM TokenLog WHERE mint_address=?", (mint,), fetch='one')
        if discovered_row and discovered_row[0]:
            try:
                discovered_dt = datetime.fromisoformat(discovered_row[0]).replace(tzinfo=timezone.utc)
                age_delta = datetime.now(timezone.utc) - discovered_dt
                intel["age_minutes"] = age_delta.total_seconds() / 60
                log.info(f"[{mint}] Age not in APIs. Using DB discovery time. Fallback age: {intel['age_minutes']:.1f}m")
            except (ValueError, TypeError):
                pass
    # Do NOT set a short default age; leaving it unset prevents misclassifying old tokens as fresh

    # Post-age Jupiter sanity check: only clamp if clearly untradable and not a newborn
    try:
        jup_ok = await fetch_jupiter_has_route(c, mint)
        # Respect grace window for very young tokens to avoid prematurely classifying as illiquid
        min_age = float(CONFIG.get("JUP_CLAMP_MIN_AGE_MINUTES", 180) or 180)
        age_m = float(intel.get("age_minutes") or 1e9)
        if jup_ok is False and age_m >= min_age:
            intel["liquidity_usd"] = 0.0
            intel["volume_24h_usd"] = 0.0
    except Exception:
        pass

    # If still missing holders, try an RPC count (approximate)
    if intel.get("holders_count") in (None, 0) and HELIUS_API_KEY:
        hc = await fetch_holders_count_via_rpc(c, mint)
        if isinstance(hc, int) and hc > 0:
            intel["holders_count"] = hc

    # Step 5: Deep dive if requested
    if deep_dive:
        log.info(f"[{mint}] Performing deep dive analysis...")
        tasks_deep_dive = {}
        if intel["creator_address"]:
            tasks_deep_dive["creator"] = fetch_creator_dossier_bitquery(c, intel["creator_address"])
        if intel["socials"].get("Twitter"):
            tasks_deep_dive["twitter"] = fetch_twitter_stats(c, intel["socials"]["Twitter"])
        
        # (age fallback done above for all paths)

        if tasks_deep_dive:
            deep_dive_results = await asyncio.gather(*tasks_deep_dive.values(), return_exceptions=True)
            results_map = dict(zip(tasks_deep_dive.keys(), deep_dive_results))
            if (res := results_map.get("creator")) and not isinstance(res, Exception): intel["creator_token_count"] = res
            if (res := results_map.get("twitter")) and not isinstance(res, Exception): intel["twitter_stats"] = res

    # Step 6: Compute scores. These functions are robust to missing data.
    intel["sss_score"] = _compute_sss(intel)
    intel["mms_score"] = _compute_mms(intel)
    intel["score"] = _compute_score(intel)
    intel["score_confidence"] = _score_confidence(intel)
    
    _intel_cache[cache_key] = intel
    return intel
