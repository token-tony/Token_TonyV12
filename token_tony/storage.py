# -*- coding: utf-8 -*-
"""Async SQLite helpers and persistence utilities for Token Tony."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, List, Set

import aiosqlite

from config import CONFIG

log = logging.getLogger("token_tony.storage")

_DB: Optional[aiosqlite.Connection] = None
_DB_LOCK = asyncio.Lock()


async def _get_db() -> aiosqlite.Connection:
    global _DB
    if _DB is None:
        async with _DB_LOCK:
            if _DB is None:
                db_path = Path(CONFIG.get("DB_FILE", "data/tony_memory.db"))
                db_path.parent.mkdir(parents=True, exist_ok=True)
                _DB = await aiosqlite.connect(db_path)
                await _DB.execute("PRAGMA journal_mode=WAL")
                await _DB.execute("PRAGMA synchronous=NORMAL")
                await _DB.execute("PRAGMA foreign_keys=ON")
    return _DB


async def setup_database() -> None:
    """Initialise tables and indexes used by the bot."""
    db = await _get_db()
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS TokenLog (
            mint_address TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            intel_json TEXT,
            discovered_at TEXT DEFAULT (datetime('now')),
            last_analyzed_time TEXT,
            last_snapshot_time TEXT,
            final_score REAL,
            score REAL,
            sss_score REAL,
            mms_score REAL,
            age_minutes REAL,
            is_hatching_candidate INTEGER DEFAULT 0,
            is_cooking_candidate INTEGER DEFAULT 0,
            is_fresh_candidate INTEGER DEFAULT 0,
            enhanced_bucket TEXT,
            priority INTEGER DEFAULT 0,
            served_at TEXT
        );

        CREATE TABLE IF NOT EXISTS TokenSnapshots (
            mint_address TEXT NOT NULL,
            snapshot_time TEXT NOT NULL,
            liquidity_usd REAL,
            volume_24h_usd REAL,
            market_cap_usd REAL,
            price_change_24h REAL,
            price_usd REAL,
            PRIMARY KEY (mint_address, snapshot_time)
        );

        CREATE TABLE IF NOT EXISTS KeyValueStore (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS PushMessages (
            chat_id INTEGER NOT NULL,
            segment TEXT NOT NULL,
            message_id INTEGER,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (chat_id, segment)
        );

        CREATE TABLE IF NOT EXISTS ServedHistory (
            mint_address TEXT NOT NULL,
            served_at TEXT NOT NULL,
            PRIMARY KEY (mint_address, served_at)
        );

        CREATE INDEX IF NOT EXISTS idx_tokenlog_status ON TokenLog(status);
        CREATE INDEX IF NOT EXISTS idx_tokenlog_bucket ON TokenLog(enhanced_bucket);
        CREATE INDEX IF NOT EXISTS idx_servedhistory_time ON ServedHistory(served_at);
        CREATE INDEX IF NOT EXISTS idx_snapshots_mint_time ON TokenSnapshots(mint_address, snapshot_time DESC);
        """
    )
    await db.commit()


async def _execute_db(
    query: str,
    params: Optional[Sequence[Any]] = None,
    *,
    fetch: Optional[str] = None,
    commit: bool = False,
) -> Any:
    db = await _get_db()
    params = params or ()
    cursor = await db.execute(query, params)
    try:
        result: Any = None
        if fetch == "one":
            result = await cursor.fetchone()
        elif fetch == "all":
            result = await cursor.fetchall()
        elif fetch == "val":
            row = await cursor.fetchone()
            result = row[0] if row else None
        if commit:
            await db.commit()
        return result
    finally:
        await cursor.close()


async def get_push_message_id(chat_id: int, segment: str) -> Optional[int]:
    row = await _execute_db(
        "SELECT message_id FROM PushMessages WHERE chat_id=? AND segment=?",
        (chat_id, segment),
        fetch="one",
    )
    if row and row[0] is not None:
        try:
            return int(row[0])
        except Exception:
            return None
    return None


async def set_push_message_id(chat_id: int, segment: str, message_id: int) -> None:
    if message_id is None:
        return
    now = datetime.now(timezone.utc).isoformat()
    await _execute_db(
        """
        INSERT OR REPLACE INTO PushMessages (chat_id, segment, message_id, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (chat_id, segment, int(message_id), now),
        commit=True,
    )


async def mark_as_served(mints: Iterable[str]) -> None:
    unique = [m for m in dict.fromkeys(mints) if m]
    if not unique:
        return
    db = await _get_db()
    base_time = datetime.now(timezone.utc)
    stamps = [
        (mint, (base_time + timedelta(microseconds=index)).isoformat())
        for index, mint in enumerate(unique)
    ]
    await db.executemany(
        "UPDATE TokenLog SET status='served', served_at=? WHERE mint_address=?",
        [(ts, mint) for mint, ts in stamps],
    )
    await db.executemany(
        "INSERT INTO ServedHistory (mint_address, served_at) VALUES (?, ?)",
        stamps,
    )
    await db.commit()


async def get_recently_served_mints(hours: int) -> list[str]:
    if hours <= 0:
        return []
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    rows = await _execute_db(
        "SELECT DISTINCT mint_address FROM ServedHistory WHERE served_at >= ?",
        (cutoff,),
        fetch="all",
    )
    return [row[0] for row in rows or [] if row and row[0]]


async def save_snapshot(mint: str, intel: Dict[str, Any]) -> None:
    db = await _get_db()
    now = datetime.now(timezone.utc).isoformat()
    def _to_float(key: str) -> Optional[float]:
        try:
            value = intel.get(key)
            return float(value) if value is not None else None
        except Exception:
            return None
    values = (
        mint,
        now,
        _to_float("liquidity_usd"),
        _to_float("volume_24h_usd"),
        _to_float("market_cap_usd"),
        _to_float("price_change_24h"),
        _to_float("price_usd"),
    )
    await db.execute(
        """
        INSERT INTO TokenSnapshots (
            mint_address, snapshot_time, liquidity_usd, volume_24h_usd,
            market_cap_usd, price_change_24h, price_usd
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        values,
    )
    await db.execute(
        "UPDATE TokenLog SET last_snapshot_time=? WHERE mint_address=?",
        (now, mint),
    )
    await db.commit()


async def load_latest_snapshot(mint: str) -> Optional[Dict[str, Any]]:
    row = await _execute_db(
        """
        SELECT liquidity_usd, volume_24h_usd, market_cap_usd, price_change_24h,
               price_usd, snapshot_time
        FROM TokenSnapshots
        WHERE mint_address=?
        ORDER BY snapshot_time DESC
        LIMIT 1
        """,
        (mint,),
        fetch="one",
    )
    if not row:
        return None
    liquidity, volume, market_cap, price_change, price, snapshot_time = row
    try:
        snapshot_dt = datetime.fromisoformat(str(snapshot_time))
    except Exception:
        snapshot_dt = datetime.now(timezone.utc)
    age_sec = (datetime.now(timezone.utc) - snapshot_dt).total_seconds()
    return {
        "liquidity_usd": liquidity,
        "volume_24h_usd": volume,
        "market_cap_usd": market_cap,
        "price_change_24h": price_change,
        "price_usd": price,
        "snapshot_time": snapshot_time,
        "snapshot_age_sec": age_sec,
    }


async def upsert_token_intel(mint: str, intel: Dict[str, Any]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    intel_json = json.dumps(intel, ensure_ascii=False)
    score = intel.get("score")
    try:
        score_val = int(float(score or 0))
    except Exception:
        score_val = 0
    try:
        sss = int(float(intel.get("sss_score", 0) or 0))
    except Exception:
        sss = 0
    try:
        mms = int(float(intel.get("mms_score", 0) or 0))
    except Exception:
        mms = 0
    try:
        age = float(intel.get("age_minutes")) if intel.get("age_minutes") is not None else None
    except Exception:
        age = None

    row = await _execute_db(
        "SELECT status FROM TokenLog WHERE mint_address=?",
        (mint,),
        fetch="one",
    )
    status = "analyzed"
    if row:
        current = (row[0] or "").lower()
        if current == "served":
            status = "served"
    await _execute_db(
        """
        INSERT INTO TokenLog (
            mint_address, status, intel_json, last_analyzed_time,
            final_score, score, sss_score, mms_score, age_minutes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mint_address) DO UPDATE SET
            status=excluded.status,
            intel_json=excluded.intel_json,
            last_analyzed_time=excluded.last_analyzed_time,
            final_score=excluded.final_score,
            score=excluded.score,
            sss_score=excluded.sss_score,
            mms_score=excluded.mms_score,
            age_minutes=excluded.age_minutes
        """,
        (
            mint,
            status,
            intel_json,
            now,
            score_val,
            score_val,
            sss,
            mms,
            age,
        ),
        commit=True,
    )

async def get_reports_by_tag(tag: str, limit: int, cooldown: set, min_score: int = 0) -> List[Dict[str, Any]]:
    """Get reports from TokenLog by tag (is_hatching_candidate, is_cooking_candidate, is_fresh_candidate)."""
    exclude_placeholders = ",".join("?" for _ in cooldown) if cooldown else "''"

    # Map tag names to column names
    tag_column_map = {
        "is_hatching_candidate": "is_hatching_candidate",
        "is_cooking_candidate": "is_cooking_candidate",
        "is_fresh_candidate": "is_fresh_candidate"
    }

    if tag not in tag_column_map:
        return []

    column = tag_column_map[tag]

    if cooldown:
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND {column} = 1
            AND final_score >= ?
            AND mint_address NOT IN ({exclude_placeholders})
            ORDER BY last_analyzed_time DESC, final_score DESC
            LIMIT ?
        """
        params = (min_score, *cooldown, limit)
    else:
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND {column} = 1
            AND final_score >= ?
            ORDER BY last_analyzed_time DESC, final_score DESC
            LIMIT ?
        """
        params = (min_score, limit)

    rows = await _execute_db(query, params, fetch='all')
    return [json.loads(row[0]) for row in rows] if rows else []

async def _db_prune(days_snap: int, days_rej: int) -> bool:
    """Placeholder for DB pruning logic."""
    log.warning("DB prune logic is not implemented yet.")
    # Here you would implement the logic to delete old snapshots and rejected tokens.
    # For example:
    # snap_cutoff = (datetime.now(timezone.utc) - timedelta(days=days_snap)).isoformat()
    # rej_cutoff = (datetime.now(timezone.utc) - timedelta(days=days_rej)).isoformat()
    # await _execute_db("DELETE FROM TokenSnapshots WHERE snapshot_time < ?", (snap_cutoff,), commit=True)
    # await _execute_db("DELETE FROM TokenLog WHERE status = 'rejected' AND discovered_at < ?", (rej_cutoff,), commit=True)
    return True

async def _db_purge_all() -> None:
    """Placeholder for DB purge logic."""
    log.warning("DB purge logic is not implemented yet.")
    # Here you would implement the logic to delete all data from the tables.
    # For example:
    # await _execute_db("DELETE FROM TokenLog", commit=True)
    # await _execute_db("DELETE FROM TokenSnapshots", commit=True)
    # await _execute_db("DELETE FROM KeyValueStore", commit=True)
    # await _execute_db("DELETE FROM PushMessages", commit=True)
    # await _execute_db("DELETE FROM ServedHistory", commit=True)
    # await _execute_db("VACUUM", commit=True)
    pass


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
    "get_reports_by_tag",
    "_db_prune",
    "_db_purge_all",
]
