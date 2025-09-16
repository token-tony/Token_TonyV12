# -*- coding: utf-8 -*-
"""SQLite helpers used by Token Tony.

Historically these utilities lived in the ``tony_helpers.db`` package.  The
original repository relied on it to abstract all database access behind a small
async API.  To keep the public interface intact we vendor the functionality
directly in the repository.  The helpers use ``aiosqlite`` so that callers can
share the same async event loop as the rest of the bot.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence

import aiosqlite

from config import CONFIG

import logging

log = logging.getLogger("token_tony.db")

DB_PATH = Path(CONFIG.get("DB_FILE", "data/tony_memory.db"))


# --------------------------------------------------------------------------------------
# Connection helpers
# --------------------------------------------------------------------------------------


async def _ensure_db_directory() -> None:
    if not DB_PATH.parent.exists():
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)


async def _prepare_connection(conn: aiosqlite.Connection) -> None:
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = aiosqlite.Row


async def _execute_db(
    query: str,
    params: Sequence[Any] | None = None,
    *,
    fetch: Optional[str] = None,
    commit: bool = False,
) -> Any:
    """Execute ``query`` against the SQLite database.

    Parameters mirror the original helper from ``tony_helpers``.  ``fetch`` may
    be ``"one"``/``"all"`` to retrieve rows; otherwise the function returns
    ``None``.  The helper is intentionally lightweight – connections are opened
    per call.  The workload consists of short writes and small reads so the
    overhead is negligible and greatly simplifies concurrency guarantees.
    """

    await _ensure_db_directory()
    params = tuple(params or ())

    async with aiosqlite.connect(DB_PATH) as conn:
        await _prepare_connection(conn)
        cursor = await conn.execute(query, params)
        result = None
        if fetch == "one":
            result = await cursor.fetchone()
        elif fetch == "all":
            result = await cursor.fetchall()
        if commit:
            await conn.commit()
        await cursor.close()
        return result


# --------------------------------------------------------------------------------------
# Schema management
# --------------------------------------------------------------------------------------


async def setup_database() -> None:
    """Create the SQLite schema if it does not exist yet."""

    await _ensure_db_directory()
    async with aiosqlite.connect(DB_PATH) as conn:
        await _prepare_connection(conn)

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS TokenLog (
                mint_address TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                intel_json TEXT,
                discovered_at TEXT DEFAULT (datetime('now')),
                last_analyzed_time TEXT,
                last_snapshot_time TEXT,
                last_served_time TEXT,
                final_score REAL,
                age_minutes REAL,
                is_hatching_candidate INTEGER DEFAULT 0,
                is_cooking_candidate INTEGER DEFAULT 0,
                is_fresh_candidate INTEGER DEFAULT 0,
                enhanced_bucket TEXT,
                priority REAL
            )
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS TokenSnapshots (
                mint_address TEXT NOT NULL,
                snapshot_time TEXT NOT NULL,
                liquidity_usd REAL,
                volume_24h_usd REAL,
                market_cap_usd REAL,
                price_usd REAL,
                price_change_24h REAL,
                PRIMARY KEY (mint_address, snapshot_time)
            )
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS PushMessages (
                chat_id INTEGER NOT NULL,
                segment TEXT NOT NULL,
                message_id INTEGER,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, segment)
            )
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS KeyValueStore (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )

        await conn.execute(
            """CREATE INDEX IF NOT EXISTS idx_tokenlog_status ON TokenLog(status)"""
        )
        await conn.execute(
            """CREATE INDEX IF NOT EXISTS idx_tokenlog_bucket ON TokenLog(enhanced_bucket)"""
        )
        await conn.execute(
            """CREATE INDEX IF NOT EXISTS idx_tokenlog_last_snapshot ON TokenLog(last_snapshot_time)"""
        )

        await conn.commit()


# --------------------------------------------------------------------------------------
# Persistence helpers used by the bot
# --------------------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


async def upsert_token_intel(mint: str, intel: Dict[str, Any]) -> None:
    if not mint:
        return

    now = _now_iso()
    intel_json = _json_dumps(intel)
    final_score = int(intel.get("score", 0) or 0)
    age_minutes = intel.get("age_minutes")

    await _execute_db(
        """
        INSERT INTO TokenLog (
            mint_address, status, intel_json, last_analyzed_time,
            final_score, age_minutes
        ) VALUES (?, 'analyzed', ?, ?, ?, ?)
        ON CONFLICT(mint_address) DO UPDATE SET
            status=excluded.status,
            intel_json=excluded.intel_json,
            last_analyzed_time=excluded.last_analyzed_time,
            final_score=excluded.final_score,
            age_minutes=excluded.age_minutes
        """,
        (mint, intel_json, now, final_score, age_minutes),
        commit=True,
    )


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str) and value.strip() == "":
            return None
        return float(value)
    except Exception:
        return None


async def save_snapshot(mint: str, intel: Dict[str, Any]) -> None:
    if not mint:
        return
    now = _now_iso()
    values = (
        mint,
        now,
        _float_or_none(intel.get("liquidity_usd")),
        _float_or_none(intel.get("volume_24h_usd")),
        _float_or_none(intel.get("market_cap_usd")),
        _float_or_none(intel.get("price_usd")),
        _float_or_none(intel.get("price_change_24h")),
    )
    await _execute_db(
        """
        INSERT OR REPLACE INTO TokenSnapshots (
            mint_address, snapshot_time, liquidity_usd, volume_24h_usd,
            market_cap_usd, price_usd, price_change_24h
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        values,
        commit=True,
    )
    await _execute_db(
        "UPDATE TokenLog SET last_snapshot_time = ? WHERE mint_address = ?",
        (now, mint),
        commit=True,
    )


async def load_latest_snapshot(mint: str) -> Optional[Dict[str, Any]]:
    if not mint:
        return None
    row = await _execute_db(
        """
        SELECT snapshot_time, liquidity_usd, volume_24h_usd, market_cap_usd,
               price_usd, price_change_24h
        FROM TokenSnapshots
        WHERE mint_address = ?
        ORDER BY snapshot_time DESC
        LIMIT 1
        """,
        (mint,),
        fetch="one",
    )
    if not row:
        return None

    snapshot_time = row["snapshot_time"]
    try:
        dt = datetime.fromisoformat(str(snapshot_time).replace("Z", "+00:00"))
        age_sec = (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        dt = None
        age_sec = None

    return {
        "snapshot_time": snapshot_time,
        "snapshot_age_sec": age_sec,
        "liquidity_usd": row["liquidity_usd"],
        "volume_24h_usd": row["volume_24h_usd"],
        "market_cap_usd": row["market_cap_usd"],
        "price_usd": row["price_usd"],
        "price_change_24h": row["price_change_24h"],
    }


async def mark_as_served(mints: Iterable[str]) -> None:
    items = [m for m in mints if m]
    if not items:
        return

    now = _now_iso()
    await _ensure_db_directory()

    async with aiosqlite.connect(DB_PATH) as conn:
        await _prepare_connection(conn)
        await conn.executemany(
            "UPDATE TokenLog SET status = 'served', last_served_time = ? WHERE mint_address = ?",
            [(now, mint) for mint in items],
        )
        await conn.commit()


async def get_recently_served_mints(cooldown_hours: float) -> set[str]:
    if cooldown_hours <= 0:
        return set()
    window = f"-{float(cooldown_hours)} hours"
    rows = await _execute_db(
        """
        SELECT mint_address FROM TokenLog
        WHERE status = 'served' AND last_served_time >= datetime('now', ?)
        """,
        (window,),
        fetch="all",
    )
    return {row[0] for row in rows or []}


async def get_push_message_id(chat_id: int, segment: str) -> Optional[int]:
    row = await _execute_db(
        "SELECT message_id FROM PushMessages WHERE chat_id = ? AND segment = ?",
        (int(chat_id), str(segment)),
        fetch="one",
    )
    if not row:
        return None
    try:
        return int(row[0]) if row[0] is not None else None
    except Exception:
        return None


async def set_push_message_id(
    chat_id: int, segment: str, message_id: Optional[int]
) -> None:
    if message_id is None:
        return
    await _execute_db(
        """
        INSERT OR REPLACE INTO PushMessages (chat_id, segment, message_id, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (int(chat_id), str(segment), int(message_id), _now_iso()),
        commit=True,
    )


__all__ = [
    "_execute_db",
    "get_push_message_id",
    "get_recently_served_mints",
    "load_latest_snapshot",
    "mark_as_served",
    "save_snapshot",
    "set_push_message_id",
    "setup_database",
    "upsert_token_intel",
]

