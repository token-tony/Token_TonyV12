# -*- coding: utf-8 -*- 
"""/check command for Token Tony."""
from __future__ import annotations

import asyncio
import logging
import random
import re
from typing import Optional

import httpx
from telegram import Update, ReplyKeyboardRemove
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from config import OWNER_ID
from http_client import get_http_client
from token_tony.analysis import enrich_token_intel
from token_tony.reports import build_full_report2, pick_header_label
from token_tony.services.dexscreener import fetch_dexscreener_chart
from token_tony.utils.telegram import safe_reply_text, _maybe_send_typing, _safe_is_group, safe_reply_photo
from token_tony.utils.solana import is_valid_solana_address


log = logging.getLogger(__name__)

_BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")


async def extract_mint_from_check_text(client: httpx.AsyncClient, text: str) -> Optional[str]:
    if not text:
        return None
    cleaned = text.strip()
    cleaned = re.sub(r"^/[A-Za-z0-9_]+\s*", "", cleaned)

    # Direct base58 candidates
    for candidate in _BASE58_RE.findall(cleaned):
        if is_valid_solana_address(candidate) and candidate not in KNOWN_QUOTE_MINTS:
            return candidate

    # Known URL patterns carrying the mint directly
    url_patterns = [
        r"birdeye\.so/(?:token|coin)/([1-9A-HJ-NP-Za-km-z]{32,44})",
        r"solscan\.io/token/([1-9A-HJ-NP-Za-km-z]{32,44})",
        r"pump\.fun/coin/([1-9A-HJ-NP-Za-km-z]{32,44})",
        r"dexscreener\.com/(?:solana|pump|raydium)/token/([1-9A-HJ-NP-Za-km-z]{32,44})",
    ]
    for pattern in url_patterns:
        match = re.search(pattern, cleaned, re.IGNORECASE)
        if match:
            cand = match.group(1)
            if is_valid_solana_address(cand) and cand not in KNOWN_QUOTE_MINTS:
                return cand

    # Query parameter extraction (e.g., token=)
    q_match = re.search(r"token=([1-9A-HJ-NP-Za-km-z]{32,44})", cleaned)
    if q_match:
        cand = q_match.group(1)
        if is_valid_solana_address(cand) and cand not in KNOWN_QUOTE_MINTS:
            return cand

    # DexScreener pair link fallback -> fetch pair details
    pair_match = re.search(r"dexscreener\.com/[^\s]+/([A-Za-z0-9]{20,})", cleaned)
    if pair_match:
        pair = pair_match.group(1).split("?")[0]
        ds_pair = await _fetch_dexscreener_pair(client, pair)
        if ds_pair:
            base = ds_pair.get("baseToken", {}).get("address")
            if base and is_valid_solana_address(base) and base not in KNOWN_QUOTE_MINTS:
                return base

    return None


async def check(u: Update, c: ContextTypes.DEFAULT_TYPE):
    # Robustly extract text from any update type (DM, group, channel)
    try:
        text = (getattr(getattr(u, 'effective_message', None), 'text', '') or '').strip()
    except Exception:
        text = ''
    # Encourage DM-only deep checks to avoid exposing details in groups
    try:
        if await _safe_is_group(u) and u.effective_user.id != OWNER_ID:
            return await safe_reply_text(u, "For privacy, run /check in DM with me.")
    except Exception:
        pass
    # Ensure any old ReplyKeyboard is removed (Telegram persists it otherwise)
    await safe_reply_text(u, "Running a quick scan... I’ll follow up with extras.", quote=True, reply_markup=ReplyKeyboardRemove())
    await _maybe_send_typing(u)
    try:
        client = await get_http_client()
        mint_address = await extract_mint_from_check_text(client, text)
        if not mint_address:
            return await safe_reply_text(u, "Give me a Solana token mint, pair link, or token URL, boss!")
        intel = await enrich_token_intel(client, mint_address, deep_dive=False)
        
        if not intel: return await safe_reply_text(u, "Couldn't find hide nor hair of that one. Bad address or no data.")
        
        # Header line like other commands
        check_quips = [
            "🔍 Tony put this one on the bench — full breakdown",
            "🔍 Here’s the inspection report",
            "🔍 Tony pulled it apart — no shortcuts",
            "🔍 Token double-checked the details",
            "🔍 Rugcheck complete — truth below",
            "🔍 Tony says: under the hood now",
            "🔍 Every gauge read — log below",
            "🔍 Inspection done — nothing hidden",
            "🔍 Tony left no gaps — all here",
            "🔍 Report delivered — raw and clear",
        ]
        header_line = f"{pick_header_label('/check')} — {random.choice(check_quips)}"
        report_text = build_full_report2(intel, include_links=True)
        final_text = header_line + "\n\n" + report_text
        # Send initial response quickly
        sent_msg = await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

        # Follow-up background enrichment (Bitquery/Twitter + chart)
        async def _follow_up_enrichment():
            try:
                deep = await enrich_token_intel(client, mint_address, deep_dive=True)
                if not deep:
                    return
                new_text = header_line + "\n\n" + build_full_report2(deep, include_links=True)
                try:
                    await u.get_bot().edit_message_text(chat_id=sent_msg.chat_id, message_id=sent_msg.message_id, text=new_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                except Exception as e_edit:
                    log.debug(f"/check edit fallback: {e_edit}")
                    await safe_reply_text(u, new_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                try:
                    photo_content2 = await fetch_dexscreener_chart(deep.get('pair_address'))
                    if photo_content2:
                        await safe_reply_photo(u, photo=photo_content2)
                except Exception as e_photo:
                    log.debug(f"/check chart send failed: {e_photo}")
            except Exception as e2:
                log.debug(f"/check follow-up enrichment failed: {e2}")

        try:
            asyncio.create_task(_follow_up_enrichment())
        except Exception as e_bg:
            log.debug(f"/check: could not schedule follow-up: {e_bg}")
    except Exception as e:
        log.error(f"Error in /check for text '{text}': {e}", exc_info=True)
        await safe_reply_text(u, "💀 Tony’s tools are jammed. Can't get a read on that one right now.")