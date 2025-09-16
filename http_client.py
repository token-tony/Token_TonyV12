# -*- coding: utf-8 -*-
"""Shared HTTP helpers with rate limiting and resilient retries."""

import asyncio
import logging
import random
from typing import Any, Iterable, Mapping, Optional

import httpx

from config import CONFIG
from utils import HTTP_LIMITER

log = logging.getLogger("token_tony.http_client")

_RETRYABLE_STATUSES = {408, 425, 429, 500, 502, 503, 504}


def _coerce_timeout(value: Any, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(fallback)


def _compute_backoff(attempt: int, *, base: float = 0.5, cap: float = 8.0) -> float:
    """Exponential backoff with jitter."""
    return min(cap, base * (2 ** attempt)) + random.uniform(0, base)


def _decode_response(
    response: httpx.Response,
    *,
    parse_json: bool,
    raw_response: bool,
) -> Any:
    if raw_response:
        return response

    if parse_json:
        if not response.content:
            return None
        try:
            return response.json()
        except ValueError:
            log.debug("Failed to decode JSON from %s", response.url, exc_info=True)
            return None

    content_type = (response.headers.get("Content-Type") or "").lower()
    if "json" in content_type:
        try:
            return response.json()
        except ValueError:
            return None

    if not response.content:
        return None
    return response.text


async def _fetch(
    client: httpx.AsyncClient,
    url: str,
    *,
    method: str = "GET",
    headers: Optional[Mapping[str, str]] = None,
    params: Optional[Mapping[str, Any]] = None,
    data: Any = None,
    json: Any = None,
    timeout: Optional[float | httpx.Timeout] = None,
    expected_status: Optional[Iterable[int]] = None,
    retry_on: Optional[Iterable[int]] = None,
    rate_limit_key: Optional[str] = "default",
    parse_json: bool = True,
    raw_response: bool = False,
    **kwargs: Any,
) -> Any:
    """Perform an HTTP request with rate limiting and exponential backoff."""

    retries = max(0, int(CONFIG.get("HTTP_RETRIES", 2) or 0))
    configured_timeout = _coerce_timeout(CONFIG.get("HTTP_TIMEOUT", 10.0), 10.0)

    if isinstance(timeout, httpx.Timeout):
        request_timeout: float | httpx.Timeout = timeout
    else:
        request_timeout = _coerce_timeout(timeout, configured_timeout)

    expected_set = set(expected_status) if expected_status is not None else None
    retry_statuses = set(retry_on) if retry_on is not None else _RETRYABLE_STATUSES

    limiter_key: Optional[str]
    if rate_limit_key is None:
        limiter_key = None
    else:
        try:
            limiter_key = str(rate_limit_key).strip().lower() or "default"
        except Exception:
            limiter_key = "default"

    request_method = method or "GET"
    last_error: Optional[Exception] = None

    for attempt in range(retries + 1):
        if limiter_key:
            await HTTP_LIMITER.limit(limiter_key)
        try:
            response = await client.request(
                request_method,
                url,
                headers=headers,
                params=params,
                data=data,
                json=json,
                timeout=request_timeout,
                **kwargs,
            )
            if expected_set is not None:
                if response.status_code not in expected_set:
                    response.raise_for_status()
            else:
                response.raise_for_status()
            return _decode_response(response, parse_json=parse_json, raw_response=raw_response)
        except httpx.HTTPStatusError as exc:
            last_error = exc
            status = exc.response.status_code
            if status not in retry_statuses or attempt == retries:
                raise
            delay = _compute_backoff(attempt)
        except httpx.RequestError as exc:
            last_error = exc
            if attempt == retries:
                raise
            delay = _compute_backoff(attempt)
        else:
            continue

        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                "HTTP %s %s failed (%s). Retrying in %.2fs (%d/%d)",
                request_method,
                url,
                last_error,
                delay,
                attempt + 1,
                retries + 1,
            )
        await asyncio.sleep(delay)

    if last_error:
        raise last_error
    return None


__all__ = ["_fetch"]
