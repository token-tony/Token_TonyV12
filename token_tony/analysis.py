# -*- coding: utf-8 -*-
"""Analysis compatibility layer for Token Tony.

Bridges older imports (token_tony.analysis) to the new services/storage layout and
provides helper utilities used by handlers and workers.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from http_client import get_http_client
from token_tony.services import (
    fetch_market_snapshot,
    _compute_mms,
    _compute_score,
    _compute_sss,
    enrich_token_intel,
)
from token_tony.services.health import LITE_MODE_UNTIL
from token_tony.storage import load_latest_snapshot

log = logging.getLogger(__name__)


async def _refresh_reports_with_latest(
    reports: List[Dict[str, Any]], allow_missing: bool = False
) -> List[Dict[str, Any]]:
    """Refresh market data for reports and recompute scores."""
    if not reports:
        return []

    refreshed: List[Dict[str, Any]] = []
    client = await get_http_client()
    for report in reports:
        mint = report.get("mint")
        if not mint:
            if allow_missing:
                refreshed.append(report)
            continue

        try:
            # Fetch fresh market snapshot
            snapshot = await fetch_market_snapshot(client, mint)
            if snapshot:
                # Update report with fresh data
                updated_report = report.copy()
                updated_report.update(
                    {
                        "liquidity_usd": snapshot.get("liquidity_usd"),
                        "volume_24h_usd": snapshot.get("volume_24h_usd"),
                        "market_cap_usd": snapshot.get("market_cap_usd"),
                        "price_change_24h": snapshot.get("price_change_24h"),
                        "price_usd": snapshot.get("price_usd"),
                    }
                )

                # Recompute scores with fresh data
                sss_score = _compute_sss(updated_report)
                mms_score = _compute_mms(updated_report)
                updated_report["sss_score"] = sss_score
                updated_report["mms_score"] = mms_score
                final_score = _compute_score(updated_report)
                updated_report["score"] = final_score

                refreshed.append(updated_report)
            else:
                if allow_missing:
                    refreshed.append(report)
        except Exception as e:
            log.warning(f"Failed to refresh report for {mint}: {e}")
            if allow_missing:
                refreshed.append(report)

    return refreshed


def _filter_items_for_command(items: List[Dict[str, Any]], command: str) -> List[Dict[str, Any]]:
    """Apply global filtering rules for command outputs (e.g., no-zero-liquidity rule)."""
    if not items:
        return items

    filtered: List[Dict[str, Any]] = []
    for item in items:
        # Global no-zero-liquidity rule for lists
        liq_raw = item.get("liquidity_usd", None)
        if liq_raw is not None:
            try:
                liq = float(liq_raw)
                if liq <= 0:
                    continue  # Skip zero liquidity items
            except (ValueError, TypeError):
                # Keep items with non-numeric liquidity (treated as unknown)
                pass

        filtered.append(item)

    return filtered


__all__ = [
    "LITE_MODE_UNTIL",
    "load_latest_snapshot",
    "_refresh_reports_with_latest",
    "_filter_items_for_command",
    "enrich_token_intel",
]
