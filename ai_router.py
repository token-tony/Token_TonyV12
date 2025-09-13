# -*- coding: utf-8 -*-
"""
Tony's AI Brain - Gemini Flash Lite Integration
Alpha Dad explanations with wit, wisdom, and zero financial advice.
"""
import logging
import os
import time
from typing import Dict, Any, Optional

import httpx
from cachetools import TTLCache

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

# Tony's fallback wisdom when AI is down
TONY_FALLBACKS = {
    "high_score": "Solid fundamentals, kid. This one's got legs and liquidity to back it up. 💪",
    "medium_score": "Decent play but keep your eyes open. Tony's seen better, seen worse. ⚖️",
    "low_score": "Red flags everywhere. Tony wouldn't touch this with a ten-foot pole. 🚩",
    "fresh": "Brand spanking new. High risk, high reward - like dating in your 20s. 🌱",
    "cooking": "This baby's heating up! Volume's spiking, momentum's building. 🔥",
    "hatching": "Just hatched from the blockchain egg. Pure speculation territory, sport. 🥚",
    "top": "Cream of the crop. Tony's impressed, and that doesn't happen often. 👑",
    "error": "Tony's brain is taking a coffee break. Check back in a minute. ☕"
}

async def explain_token_score(intel: Dict[str, Any], context: str = "general") -> str:
    """
    Tony's AI-powered explanations - witty, wise, and never financial advice.
    """
    if not GEMINI_API_KEY:
        log.debug("🤖 No Gemini key - Tony's using his backup wisdom")
        return _get_tony_fallback(intel, context)
    
    # Tony's memory check
    cache_key = f"{intel.get('mint', 'unknown')}_{intel.get('score', 0)}_{context}_{int(time.time() / 300)}"
    if cache_key in EXPLANATION_CACHE:
        return EXPLANATION_CACHE[cache_key]
    
    try:
        explanation = await _call_gemini_api(intel, context)
        if explanation and len(explanation.strip()) > 10:
            EXPLANATION_CACHE[cache_key] = explanation
            return explanation
    except Exception as e:
        log.warning(f"🤖 Tony's AI brain hiccupped: {e}")
    
    # Fallback to Tony's built-in wisdom
    fallback = _get_tony_fallback(intel, context)
    EXPLANATION_CACHE[cache_key] = fallback
    return fallback

async def _call_gemini_api(intel: Dict[str, Any], context: str) -> Optional[str]:
    """Tony's direct line to Gemini - optimized for cost and personality."""
    
    # Tony's data summary - concise but complete
    score = intel.get('score', 0)
    symbol = intel.get('symbol', 'Unknown')
    liquidity = intel.get('liquidity_usd', 0)
    volume_24h = intel.get('volume_24h_usd', 0)
    age_minutes = intel.get('age_minutes', 0)
    rugcheck = intel.get('rugcheck_score', 'N/A')
    price_change = intel.get('price_change_24h', 0)
    
    # Tony's personality prompt - efficient but on-brand
    prompt = f"""You are Tony, the "Alpha Dad" of crypto - protective, witty, data-driven, never gives financial advice.

Token: {symbol} | Score: {score}/100 | Context: {context}
Data: Liq=${liquidity:,.0f}, Vol=${volume_24h:,.0f}, Age={age_minutes}min, Risk={rugcheck}, Change={price_change:+.1f}%

Explain the score in 1-2 sentences. Be Tony: direct, protective, use relevant emoji, mention key factors. Never say "buy/sell/invest".

Examples of Tony's voice:
- "Solid fundamentals, kid. Liquidity's there and volume's backing it up. 💪"
- "Red flags everywhere. Tony wouldn't touch this with a ten-foot pole. 🚩"
- "Fresh out the gate with decent backing. High risk, high reward territory. 🌱"
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

def _get_tony_fallback(intel: Dict[str, Any], context: str) -> str:
    """Tony's backup wisdom when AI is unavailable."""
    score = intel.get('score', 0)
    age_minutes = intel.get('age_minutes', 0)
    
    # Context-specific Tony wisdom
    if context == "fresh" or age_minutes < 1440:
        return TONY_FALLBACKS["fresh"]
    elif context == "cooking":
        return TONY_FALLBACKS["cooking"]
    elif context == "hatching" or age_minutes < 30:
        return TONY_FALLBACKS["hatching"]
    elif context == "top":
        return TONY_FALLBACKS["top"]
    
    # Score-based Tony wisdom
    if score >= 70:
        return TONY_FALLBACKS["high_score"]
    elif score >= 40:
        return TONY_FALLBACKS["medium_score"]
    else:
        return TONY_FALLBACKS["low_score"]

def get_ai_health_status() -> Dict[str, Any]:
    """Tony's AI health check for diagnostics."""
    return {
        "gemini_configured": bool(GEMINI_API_KEY),
        "cache_size": len(EXPLANATION_CACHE),
        "cache_hits": getattr(EXPLANATION_CACHE, 'hits', 0),
        "cache_misses": getattr(EXPLANATION_CACHE, 'misses', 0),
    }


