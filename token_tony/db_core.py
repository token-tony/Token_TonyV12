# -*- coding: utf-8 -*-
"""Compatibility shim: token_tony.db_core -> token_tony.storage.

Keeps older imports working while the codebase migrates to the new storage module.
"""
from __future__ import annotations

from token_tony.storage import (
    _execute_db,
    get_push_message_id,
    get_recently_served_mints,
    load_latest_snapshot,
    mark_as_served,
    save_snapshot,
    setup_database,
    set_push_message_id,
    upsert_token_intel,
    get_reports_by_tag,
    _db_prune,
    _db_purge_all,
)

__all__ = [
    "_execute_db",
    "get_push_message_id",
    "get_recently_served_mints",
    "load_latest_snapshot",
    "mark_as_served",
    "save_snapshot",
    "setup_database",
    "set_push_message_id",
    "upsert_token_intel",
    "get_reports_by_tag",
    "_db_prune",
    "_db_purge_all",
]
