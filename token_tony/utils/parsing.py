# -*- coding: utf-8 -*-
"""Parsing utilities for Token Tony."""

from typing import Any

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
