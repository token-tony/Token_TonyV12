# -*- coding: utf-8 -*-
"""
Tony's AI Brain - Gemini Flash Lite Integration
Alpha Dad explanations with wit, wisdom, and zero financial advice.
"""
import logging
import os
import time
from typing import Dict, Any, Iterable, Optional, Set

import httpx
from cachetools import TTLCache
from voice import (
    VOICE_PRESETS,
    get_current_voice,
    get_voice_label,
    get_voice_prompt_instructions,
)

log = logging.getLogger("token_tony.ai_router")

# Tony's memory - he remembers what he's already explained
EXPLANATION_CACHE = TTLCache(maxsize=200, ttl=1800)  # 30min cache

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
# Prefer stable flash models; keep experimental as last fallback
GEMINI_MODELS = (
    "gemini-1.5-flash-latest",
    "gemini-1.5-flash-8b-latest",
    "gemini-2.0-flash-exp",
)
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"

# Tony's fallback wisdom when AI is down, keyed by voice preset
TONY_FALLBACKS: Dict[str, Dict[str, Any]] = {
    key: profile.get("fallbacks", {}) for key, profile in VOICE_PRESETS.items()
}

DEFAULT_VOICE_KEY = "protective_dad" if "protective_dad" in TONY_FALLBACKS else next(
    iter(TONY_FALLBACKS), "protective_dad"
)

TAG_PRIORITY: Iterable[str] = (
    "bleeding",
    "surging",
    "illiquid",
    "deep_liquidity",
    "top",
    "cooking",
    "fresh",
    "hatching",
    "volatility_high",
)

TAG_DISPLAY_PRIORITY: Iterable[str] = (
    "top",
    "cooking",
    "fresh",
    "hatching",
    "surging",
    "bleeding",
    "deep_liquidity",
    "illiquid",
    "volatility_high",
)

TAG_DISPLAY_ALIASES = {
    "deep_liquidity": "deep-liquidity",
    "volatility_high": "volatility-spike",
}


def _current_voice_fallbacks() -> Dict[str, Any]:
    voice_key = get_current_voice()
    return TONY_FALLBACKS.get(voice_key, TONY_FALLBACKS.get(DEFAULT_VOICE_KEY, {}))


def _format_tag(tag: str) -> str:
    return TAG_DISPLAY_ALIASES.get(tag, tag)


def _ordered_tags(tags: Set[str]) -> list[str]:
    ordered = [t for t in TAG_DISPLAY_PRIORITY if t in tags]
    for tag in sorted(tags):
        if tag not in ordered:
            ordered.append(tag)
    return ordered


def _derive_context_tags(intel: Dict[str, Any], context: str) -> Set[str]:
    tags: Set[str] = set()
    if context:
        for raw in str(context).replace(",", "|").split("|"):
            clean = raw.strip().lower()
            if clean and clean not in {"general", "none"}:
                tags.add(clean)

    bucket = str(intel.get("enhanced_bucket") or "").strip().lower()
    if bucket:
        tags.add(bucket)

    age_minutes = intel.get("age_minutes")
    try:
        age_val = float(age_minutes) if age_minutes is not None else None
    except (TypeError, ValueError):
        age_val = None
    if age_val is not None:
        if age_val < 30:
            tags.add("hatching")
        if age_val < 1440:
            tags.add("fresh")

    liquidity = intel.get("liquidity_usd")
    try:
        liq_val = float(liquidity) if liquidity is not None else None
    except (TypeError, ValueError):
        liq_val = None
    if liq_val is not None:
        if liq_val < 10_000:
            tags.add("illiquid")
        elif liq_val >= 75_000:
            tags.add("deep_liquidity")

    price_change = intel.get("price_change_24h")
    try:
        change_val = float(price_change) if price_change is not None else None
    except (TypeError, ValueError):
        change_val = None
    if change_val is not None:
        if change_val >= 35:
            tags.add("surging")
        if change_val <= -20:
            tags.add("bleeding")
        if abs(change_val) >= 45:
            tags.add("volatility_high")

    volume = intel.get("volume_24h_usd")
    try:
        vol_val = float(volume) if volume is not None else None
    except (TypeError, ValueError):
        vol_val = None
    if vol_val is not None and liq_val and liq_val > 0:
        if vol_val / max(liq_val, 1.0) >= 6:
            tags.add("volatility_high")

    return tags


async def explain_token_score(intel: Dict[str, Any], context: str = "general") -> str:
    """
    Tony's AI-powered explanations - witty, wise, and never financial advice.
    """
    tags = _derive_context_tags(intel, context)
    tag_signature = "|".join(sorted(tags)) or "none"

    if not GEMINI_API_KEY:
        log.debug("🤖 No Gemini key - Tony's using his backup wisdom")
        return _get_tony_fallback(intel, context, tags)

    # Tony's memory check (bucketed by tags to avoid stale tone)
    cache_key = (
        f"{intel.get('mint', 'unknown')}_{intel.get('score', 0)}_{context}_{tag_signature}_"
        f"{int(time.time() / 300)}"
    )
    if cache_key in EXPLANATION_CACHE:
        return EXPLANATION_CACHE[cache_key]

    try:
        explanation = await _call_gemini_api(intel, context, tags)
        if explanation and len(explanation.strip()) > 10:
            EXPLANATION_CACHE[cache_key] = explanation
            return explanation
    except Exception as e:
        log.warning(f"🤖 Tony's AI brain hiccupped: {e}")

    # Fallback to Tony's built-in wisdom
    fallback = _get_tony_fallback(intel, context, tags)
    EXPLANATION_CACHE[cache_key] = fallback
    return fallback

async def _call_gemini_api(intel: Dict[str, Any], context: str, context_tags: Set[str]) -> Optional[str]:
    """Tony's direct line to Gemini - optimized for cost and personality."""

    # Tony's data summary - concise but complete
    score = intel.get('score', 0)
    symbol = intel.get('symbol', 'Unknown')
    liquidity = intel.get('liquidity_usd', 0)
    volume_24h = intel.get('volume_24h_usd', 0)
    age_minutes = intel.get('age_minutes', 0)
    rugcheck = intel.get('rugcheck_score', 'N/A')
    price_change = intel.get('price_change_24h', 0)
    
    ordered_tags = _ordered_tags(context_tags)
    tag_display = ", ".join(_format_tag(tag) for tag in ordered_tags) if ordered_tags else "general"
    voice_label = get_voice_label()
    voice_prompt = get_voice_prompt_instructions()

    # Tony's personality prompt - efficient but on-brand
    prompt = f"""You are Tony, the "Alpha Dad" of crypto - protective, witty, data-driven, never gives financial advice.

Current voice preset: {voice_label}.
Voice instructions: {voice_prompt}

Token: {symbol} | Score: {score}/100
Requested context: {context}
Context tags: {tag_display}
Data: Liq=${liquidity:,.0f}, Vol=${volume_24h:,.0f}, Age={age_minutes}min, Risk={rugcheck}, Change={price_change:+.1f}%

Explain the score in 1-2 sentences. Reference the most relevant context tags, stay in character, and never say "buy", "sell", or "invest".
"""

    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }],
        "generationConfig": {
            "maxOutputTokens": 80,  # Keep it brief for cost
            "temperature": 0.8,     # Tony's got personality
            "topP": 0.9,
            "stopSequences": ["\n\n"]  # Stop at double newline
        }
    }
    
    timeout = httpx.Timeout(float(os.getenv("GEMINI_TIMEOUT", "10.0") or 10.0))
    
    async with httpx.AsyncClient(timeout=timeout) as client:
        last_error: Optional[Exception] = None
        for model in GEMINI_MODELS:
            url = f"{GEMINI_API_BASE}/models/{model}:generateContent?key={GEMINI_API_KEY}"
            try:
                response = await client.post(url, json=payload, headers={"Content-Type": "application/json"})
                response.raise_for_status()
                data = response.json()
                candidates = data.get("candidates", [])
                if candidates and candidates[0].get("content", {}).get("parts"):
                    text = candidates[0]["content"]["parts"][0].get("text", "").strip()
                    if text and not text.lower().startswith("i "):
                        return text
            except Exception as e:
                last_error = e
                continue
        if last_error:
            raise last_error
    
    return None

def _get_tony_fallback(
    intel: Dict[str, Any], context: str, context_tags: Optional[Set[str]] = None
) -> str:
    """Tony's backup wisdom when AI is unavailable."""

    tags = set(context_tags or _derive_context_tags(intel, context))
    fallback_map = _current_voice_fallbacks()

    tag_map: Dict[str, str] = fallback_map.get("tags", {}) if isinstance(fallback_map, dict) else {}
    for priority in TAG_PRIORITY:
        if priority in tags and priority in tag_map:
            return tag_map[priority]

    for tag in _ordered_tags(tags):
        if tag in tag_map:
            return tag_map[tag]

    score_val = intel.get("score", 0)
    try:
        score_float = float(score_val)
    except (TypeError, ValueError):
        score_float = 0.0

    score_map: Dict[str, str] = fallback_map.get("score", {}) if isinstance(fallback_map, dict) else {}
    if score_float >= 70 and "high" in score_map:
        return score_map["high"]
    if score_float >= 40 and "medium" in score_map:
        return score_map["medium"]
    if "low" in score_map:
        return score_map["low"]

    default_voice = TONY_FALLBACKS.get(DEFAULT_VOICE_KEY, {})
    if isinstance(fallback_map, dict) and "default" in fallback_map:
        return fallback_map["default"]
    if isinstance(default_voice, dict):
        if "default" in default_voice:
            return default_voice["default"]
        default_score_map = default_voice.get("score", {})
        for key in ("medium", "low", "high"):
            if key in default_score_map:
                return default_score_map[key]

    return "Tony's double-checking the data. Give it a beat."

def get_ai_health_status() -> Dict[str, Any]:
    """Tony's AI health check for diagnostics."""
    return {
        "gemini_configured": bool(GEMINI_API_KEY),
        "cache_size": len(EXPLANATION_CACHE),
        "cache_hits": getattr(EXPLANATION_CACHE, 'hits', 0),
        "cache_misses": getattr(EXPLANATION_CACHE, 'misses', 0),
    }


