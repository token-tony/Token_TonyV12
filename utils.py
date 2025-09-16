# -*- coding: utf-8 -*-
import asyncio
import html as _html
import math
import random
import re
import time
from typing import Any, Dict, Mapping, Optional

from telegram.constants import ParseMode

from config import CONFIG, OWNER_ID

# --------------------------------------------------------------------------------------
# Rate limiting primitives (token buckets) and Telegram outbox gating
# --------------------------------------------------------------------------------------

class TokenBucket:
    def __init__(self, capacity: int, refill_amount: int, interval_seconds: float) -> None:
        self.capacity = max(1, capacity)
        self.tokens = float(capacity)
        self.refill_amount = float(refill_amount)
        self.interval = float(interval_seconds)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, amount: float = 1.0) -> None:
        amount = float(amount)
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._last)
                if elapsed >= self.interval:
                    # Add whole-interval refills for stability under load
                    intervals = int(elapsed // self.interval)
                    self.tokens = min(self.capacity, self.tokens + intervals * self.refill_amount)
                    self._last = now if intervals > 0 else self._last
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
                # Compute time until next token becomes available
                needed = amount - self.tokens
                rate_per_sec = (self.refill_amount / self.interval) if self.interval > 0 else self.refill_amount
                wait = max(0.01, needed / max(1e-6, rate_per_sec))
            # jitter to avoid thundering herd
            await asyncio.sleep(min(2.0, wait + random.uniform(0, 0.05)))


class HttpRateLimiter:
    """Endpoint/host aware limiter with per-provider buckets.

    Limits are configured via requests-per-second (``rps``), burst capacity
    (maximum queued tokens) and an optional interval length in seconds. Each
    provider is keyed by a short string (for example ``"dexscreener"`` or
    ``"gecko"``) that should be supplied to :meth:`limit` before issuing an
    outbound HTTP request.
    """

    def __init__(
        self,
        *,
        default_rps: float = 10.0,
        default_burst: Optional[int] = None,
        default_interval: float = 1.0,
        provider_limits: Optional[Mapping[str, Mapping[str, Any]]] = None,
    ) -> None:
        self._buckets: Dict[str, TokenBucket] = {}
        self._active_configs: Dict[str, tuple[int, float, float]] = {}
        self._lock = asyncio.Lock()
        self._provider_limits: Dict[str, Dict[str, float]] = {}

        self._default_rps = self._coerce_float(default_rps, 10.0, minimum=0.01)
        self._default_interval = self._coerce_float(default_interval, 1.0, minimum=0.001)
        fallback_burst = self._default_rps * self._default_interval * 2.0
        self._default_burst = self._coerce_int(default_burst, fallback_burst)
        self._default_refill = self._default_rps * self._default_interval
        self._active_configs["default"] = (
            self._default_burst,
            self._default_refill,
            self._default_interval,
        )

        if provider_limits:
            self.configure(provider_limits)

    @staticmethod
    def _coerce_float(value: Any, fallback: float, *, minimum: Optional[float] = None) -> float:
        try:
            if value is None:
                raise ValueError
            result = float(value)
        except (TypeError, ValueError):
            result = float(fallback)
        if minimum is not None:
            result = max(minimum, result)
        return result

    @staticmethod
    def _coerce_int(value: Any, fallback: float) -> int:
        try:
            if value is None:
                raise ValueError
            result = int(value)
        except (TypeError, ValueError):
            result = int(math.ceil(float(fallback)))
        return max(1, result)

    def _parse_limit_config(self, cfg: Any, *, fallback_rps: float, fallback_interval: float) -> Dict[str, float]:
        if isinstance(cfg, Mapping):
            data = dict(cfg)
        else:
            data = {"rps": cfg}

        interval = self._coerce_float(data.get("interval"), fallback_interval, minimum=0.001)
        if "refill" in data:
            refill = self._coerce_float(data["refill"], fallback_rps * interval, minimum=0.0001)
            rps_value = refill / interval if interval else fallback_rps
        else:
            rps_value = self._coerce_float(data.get("rps"), fallback_rps, minimum=0.0001)
            refill = rps_value * interval

        capacity_hint = data.get("burst", data.get("capacity", data.get("max_tokens")))
        burst = self._coerce_int(capacity_hint, max(refill * 2.0, 1.0))
        return {
            "rps": rps_value,
            "burst": burst,
            "interval": interval,
            "refill": refill,
        }

    def configure(self, provider_limits: Mapping[str, Any]) -> None:
        if not provider_limits:
            return

        for raw_key, cfg in provider_limits.items():
            if raw_key is None:
                continue
            key = str(raw_key).strip().lower()
            if not key:
                continue

            normalized = self._parse_limit_config(
                cfg, fallback_rps=self._default_rps, fallback_interval=self._default_interval
            )

            if key == "default":
                self._default_rps = normalized["rps"]
                self._default_interval = normalized["interval"]
                self._default_burst = int(normalized["burst"])
                self._default_refill = normalized["refill"]
                self._active_configs["default"] = (
                    self._default_burst,
                    self._default_refill,
                    self._default_interval,
                )
                if "default" in self._buckets:
                    self._buckets["default"] = TokenBucket(
                        self._default_burst, self._default_refill, self._default_interval
                    )
                continue

            self._provider_limits[key] = normalized
            self._active_configs[key] = (
                int(normalized["burst"]),
                float(normalized["refill"]),
                float(normalized["interval"]),
            )
            if key in self._buckets:
                self._buckets[key] = TokenBucket(*self._active_configs[key])

    def _settings_for_key(self, key: str) -> tuple[int, float, float]:
        cfg = self._provider_limits.get(key)
        if cfg:
            return int(cfg["burst"]), float(cfg["refill"]), float(cfg["interval"])
        return (
            int(self._default_burst),
            float(self._default_refill),
            float(self._default_interval),
        )

    async def ensure_bucket(self, key: str, capacity: int, refill: float, interval: float) -> TokenBucket:
        capacity = self._coerce_int(capacity, capacity)
        refill = float(refill)
        interval = self._coerce_float(interval, self._default_interval, minimum=0.001)
        async with self._lock:
            bucket = self._buckets.get(key)
            config = (capacity, refill, interval)
            if bucket is None or self._active_configs.get(key) != config:
                bucket = TokenBucket(capacity, refill, interval)
                self._buckets[key] = bucket
                self._active_configs[key] = config
            return bucket

    async def limit(self, key: str) -> None:
        normalized = "default"
        if key is not None:
            try:
                normalized = str(key).strip().lower() or "default"
            except Exception:
                normalized = "default"

        target = self._settings_for_key(normalized)
        bucket = self._buckets.get(normalized)
        if bucket is None or self._active_configs.get(normalized) != target:
            bucket = await self.ensure_bucket(normalized, *target)
        await bucket.acquire(1.0)

    def get_config(self, key: str) -> Dict[str, float]:
        normalized = "default"
        if key is not None:
            try:
                normalized = str(key).strip().lower() or "default"
            except Exception:
                normalized = "default"

        if normalized == "default":
            return {
                "rps": float(self._default_rps),
                "burst": int(self._default_burst),
                "interval": float(self._default_interval),
            }

        cfg = self._provider_limits.get(normalized)
        if cfg:
            return {
                "rps": float(cfg["rps"]),
                "burst": int(cfg["burst"]),
                "interval": float(cfg["interval"]),
            }
        return {
            "rps": float(self._default_rps),
            "burst": int(self._default_burst),
            "interval": float(self._default_interval),
        }


class TelegramOutbox:
    """Global + per-chat + per-group token buckets for Telegram sends."""

    def __init__(self) -> None:
        # Global: ~30 msgs/sec
        self.global_bucket = TokenBucket(capacity=30, refill_amount=30, interval_seconds=1.0)
        self.per_chat: Dict[int, TokenBucket] = {}
        self.per_group: Dict[int, TokenBucket] = {}
        self._lock = asyncio.Lock()

    async def _chat_bucket(self, chat_id: int) -> TokenBucket:
        async with self._lock:
            if chat_id not in self.per_chat:
                # 1 msg/sec sustained per chat
                self.per_chat[chat_id] = TokenBucket(capacity=1, refill_amount=1, interval_seconds=1.0)
            return self.per_chat[chat_id]

    async def _group_bucket(self, chat_id: int) -> TokenBucket:
        async with self._lock:
            if chat_id not in self.per_group:
                # 20 msgs/min per group
                self.per_group[chat_id] = TokenBucket(capacity=20, refill_amount=20, interval_seconds=60.0)
            return self.per_group[chat_id]

    async def send_text(self, bot, chat_id: int, text: str, is_group: bool, **kwargs):
        await self.global_bucket.acquire(1)
        if is_group:
            await (await self._group_bucket(chat_id)).acquire(1)
        await (await self._chat_bucket(chat_id)).acquire(1)
        # Retry on 429 with jitter
        for attempt in range(5):
            try:
                # Map PTB convenience arg 'quote' to reply_to_message_id if present
                quote = bool(kwargs.pop("quote", False))
                if quote and hasattr(bot, "_Application"):  # not reliable; instead use context from kwargs if provided
                    pass
                reply_to_message_id = kwargs.pop("reply_to_message_id", None)
                if quote and reply_to_message_id is None:
                    # Try to infer from potential Update passed via 'update' kw (if any)
                    # If not present, just send without quoting (PTB's bot API doesn't support 'quote')
                    pass
                return await bot.send_message(chat_id=chat_id, text=text, reply_to_message_id=reply_to_message_id, **kwargs)
            except Exception as e:
                # Telegram RetryAfter or generic 429/420 errors
                msg = str(e)
                if ("Too Many Requests" in msg or "RetryAfter" in msg or "429" in msg) and attempt < 4:
                    await asyncio.sleep(1.5 + random.uniform(0, 0.6))
                    continue
                # Network flaps on underlying httpx/httpcore: retry lightly
                if any(s in msg for s in ("ReadError", "Timeout", "timed out", "Server disconnected", "reset by peer", "RemoteProtocolError", "ClientOSError")) and attempt < 4:
                    await asyncio.sleep(0.8 + 0.4 * attempt + random.uniform(0, 0.3))
                    continue
                raise

    async def send_photo(self, bot, chat_id: int, photo: bytes, is_group: bool, **kwargs):
        await self.global_bucket.acquire(1)
        if is_group:
            await (await self._group_bucket(chat_id)).acquire(1)
        await (await self._chat_bucket(chat_id)).acquire(1)
        for attempt in range(5):
            try:
                quote = bool(kwargs.pop("quote", False))
                reply_to_message_id = kwargs.pop("reply_to_message_id", None)
                if quote and reply_to_message_id is None:
                    pass
                return await bot.send_photo(chat_id=chat_id, photo=photo, reply_to_message_id=reply_to_message_id, **kwargs)
            except Exception as e:
                msg = str(e)
                if ("Too Many Requests" in msg or "RetryAfter" in msg or "429" in msg) and attempt < 4:
                    await asyncio.sleep(1.5 + random.uniform(0, 0.6))
                    continue
                if any(s in msg for s in ("ReadError", "Timeout", "timed out", "Server disconnected", "reset by peer", "RemoteProtocolError", "ClientOSError")) and attempt < 4:
                    await asyncio.sleep(0.8 + 0.4 * attempt + random.uniform(0, 0.3))
                    continue
                raise


OUTBOX = TelegramOutbox()

_HTTP_LIMIT_CONFIG = CONFIG.get("HTTP_PROVIDER_LIMITS", {}) or {}
_DEFAULT_HTTP_LIMIT = _HTTP_LIMIT_CONFIG.get("default", {})
HTTP_LIMITER = HttpRateLimiter(
    default_rps=_DEFAULT_HTTP_LIMIT.get("rps", 10.0),
    default_burst=_DEFAULT_HTTP_LIMIT.get("burst"),
    default_interval=_DEFAULT_HTTP_LIMIT.get("interval", 1.0),
)
HTTP_LIMITER.configure(_HTTP_LIMIT_CONFIG)

# --- Telegram helpers for channel access checks ---
async def _notify_owner(bot, text: str) -> None:
    try:
        if OWNER_ID:
            await OUTBOX.send_text(bot, OWNER_ID, text, is_group=False, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception:
        pass

async def _can_post_to_chat(bot, chat_id: int) -> tuple[bool, str]:
    """Check if the bot can post to the given chat (channel/group).
    Returns (ok, reason). ok=True when bot is admin (channels) or member with send rights (groups).
    """
    try:
        me = await bot.get_me()
        my_id = getattr(me, 'id', None)
        if not my_id:
            return False, "get_me returned no id"
    except Exception as e:
        return False, f"get_me failed: {e}"
    try:
        chat = await bot.get_chat(chat_id)
    except Exception as e:
        return False, f"get_chat failed: {e}"
    try:
        m = await bot.get_chat_member(chat_id, my_id)
        status = getattr(m, 'status', '')
        chat_type = getattr(chat, 'type', '') or ''
        is_channel = (chat_type == 'channel')
        if status in ("administrator", "creator"):
            # Admin of channel/group: check explicit permissions when available
            can_post = True
            # For channels, ensure can_post_messages if attribute exists
            if is_channel:
                can_post = bool(getattr(m, 'can_post_messages', True))
            # For groups, ensure can_send_messages if attribute exists (PTB uses ChatMemberAdministrator without this flag sometimes)
            if not is_channel:
                can_post = bool(getattr(m, 'can_send_messages', True))
            if can_post:
                return True, "ok"
            return False, f"admin but posting disabled (type={chat_type})"
        # Non-admin path: allow member in groups/supergroups if can_send_messages
        if not is_channel and status in ("member", "restricted"):
            can_send = getattr(m, 'can_send_messages', None)
            if can_send is None:
                # Assume allowed when flag missing
                return True, "ok"
            if bool(can_send):
                return True, "ok"
            return False, "member but cannot send messages"
        # For channels, non-admin cannot post
        return False, f"insufficient rights (type={chat_type}, status={status})"
    except Exception as e:
        return False, f"get_chat_member failed: {e}"

def is_valid_solana_address(address: str) -> bool:
    """Validate a Solana address (base58-encoded 32-byte public key).
    Accept 43–44 base58 chars (leading zeros can yield 43).
    """
    return bool(re.match(r"^[1-9A-HJ-NP-Za-km-z]{43,44}$", address or ""))

def _parse_typed_value(v: str) -> Any:
    s = v.strip()
    low = s.lower()
    if low in {"true", "yes", "on"}: return True
    if low in {"false", "no", "off"}: return False
    try:
        if "." in s: return float(s)
        return int(s)
    except ValueError:
        return s
