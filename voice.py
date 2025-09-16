"""Voice preset management for Token Tony.

Provides runtime-configurable tone presets that influence AI prompts and
fallback quips. Presets can be toggled via the /voice admin command.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

VOICE_PRESETS: Dict[str, Dict[str, Any]] = {
    "protective_dad": {
        "label": "Protective Dad",
        "description": "Guardrails up, fatherly caution with quick risk call-outs.",
        "prompt": (
            "Lean into protective-dad energy: keep rookies safe, highlight red flags, "
            "sound confident without giving financial advice."
        ),
        "fallbacks": {
            "score": {
                "high": "Solid fundamentals, kid. Liquidity's there but the seatbelt stays buckled. 🛡️",
                "medium": "Decent play but keep your eyes open. Tony's seen better, seen worse. ⚖️",
                "low": "Too many red flags. Tony's locking the door on this one. 🚫",
            },
            "tags": {
                "top": "Cream of the crop, but we still check the safety harness before liftoff. 👑",
                "cooking": "Momentum's heating up—ride it, but keep stop-loss discipline tight. 🍳",
                "fresh": "Brand-new listing—training wheels stay on until liquidity proves itself. 🌱",
                "hatching": "Just cracked from the shell. Treat it like a newborn: gentle and suspicious. 🥚",
                "surging": "It's ripping right now. Don't chase without a plan and a helmet. 🚀",
                "bleeding": "Momentum's bleeding out—protect capital first, questions later. 🩸",
                "deep_liquidity": "Liquidity's a full-on moat—Tony approves but still guards the drawbridge. 💧",
                "illiquid": "Order book's paper-thin. Tony keeps the rookies on the sidelines. 🧊",
                "volatility_high": "Volatility's spiking like a sugar rush; keep size tiny and exits ready. ⚡",
            },
            "default": "Tony's watching from the porch—discipline first, hype second. 🧱",
            "error": "Tony's brain is taking a coffee break. Check back in a minute. ☕",
        },
    },
    "hype_mode": {
        "label": "Hype Mode",
        "description": "High-energy hype man vibes while still pointing out risks.",
        "prompt": (
            "Dial the energy up: celebrate momentum, sprinkle swagger, but always "
            "flag risks and avoid investment advice."
        ),
        "fallbacks": {
            "score": {
                "high": "Charts are singing and the crowd's roaring—Tony's hype meter is maxed. 🔥",
                "medium": "Decent rhythm building—stay nimble and ride the groove. 🎶",
                "low": "Energy's off. Tony's not feeling this track—skip before it drops. ⛔",
            },
            "tags": {
                "top": "Winners' circle vibes—laser lights on this one. 👑",
                "cooking": "Pan's sizzling—keep the fire hot but don't burn the bankroll. 🍳",
                "fresh": "Fresh drop on stage—warm-up set with upside if it holds pitch. 🌶️",
                "hatching": "Newborn beat—tiny but feisty. Let it prove itself before you crowd the floor. 🐣",
                "surging": "It's ripping the speakers—ride the wave but watch your stops. 🚀",
                "bleeding": "Beat just skipped—tone down size until momentum snaps back. 🩸",
                "deep_liquidity": "Pool's deep enough for a cannonball—splashes welcome. 🌊",
                "illiquid": "Liquidity's whisper quiet—the DJ can't spin with an empty floor. 🤫",
                "volatility_high": "Strobe lights on max—expect wild drops and sharp rebounds. ⚡",
            },
            "default": "Tony's warming up the crowd—play it smart but keep the vibe high. 🎤",
            "error": "Soundboard glitched—Tony's hype man is grabbing a reboot. 🔌",
        },
    },
}

_DEFAULT_VOICE = os.getenv("TONY_VOICE_PRESET", "protective_dad").strip().lower() or "protective_dad"
if _DEFAULT_VOICE not in VOICE_PRESETS:
    _DEFAULT_VOICE = "protective_dad"

_current_voice = _DEFAULT_VOICE


def get_current_voice() -> str:
    """Return the key for the current voice preset."""
    return _current_voice


def get_voice_profile(preset: Optional[str] = None) -> Dict[str, Any]:
    """Return the profile for the requested (or current) voice preset."""
    name = (preset or _current_voice).lower()
    if name not in VOICE_PRESETS:
        raise KeyError(f"Unknown voice preset: {preset}")
    return VOICE_PRESETS[name]


def get_voice_label(preset: Optional[str] = None) -> str:
    """Human-readable label for the requested (or current) voice preset."""
    return get_voice_profile(preset)["label"]


def get_voice_prompt_instructions(preset: Optional[str] = None) -> str:
    """Prompt instructions for the requested (or current) voice preset."""
    return get_voice_profile(preset)["prompt"]


def get_voice_fallbacks(preset: Optional[str] = None) -> Dict[str, Any]:
    """Structured fallback strings for the requested (or current) voice preset."""
    return get_voice_profile(preset).get("fallbacks", {})


def list_voice_presets() -> Dict[str, str]:
    """Return available presets mapped to user-facing descriptions."""
    return {
        key: f"{profile['label']} — {profile['description']}"
        for key, profile in VOICE_PRESETS.items()
    }


def set_voice_preset(name: str) -> Dict[str, Any]:
    """Set the current voice preset and return the profile."""
    global _current_voice
    key = (name or "").strip().lower()
    if key not in VOICE_PRESETS:
        raise KeyError(f"Unknown voice preset: {name}")
    _current_voice = key
    return VOICE_PRESETS[key]


def cycle_voice_preset(step: int = 1) -> Dict[str, Any]:
    """Advance the current preset index and return the new profile."""
    global _current_voice
    keys = list(VOICE_PRESETS.keys())
    try:
        idx = keys.index(_current_voice)
    except ValueError:
        idx = 0
    new_key = keys[(idx + step) % len(keys)]
    _current_voice = new_key
    return VOICE_PRESETS[new_key]
