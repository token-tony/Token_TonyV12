# -*- coding: utf-8 -*-
"""Solana utilities for Token Tony."""

import re

def is_valid_solana_address(address: str) -> bool:
    """Validate a Solana address (base58-encoded 32-byte public key).
    Accept 43–44 base58 chars (leading zeros can yield 43).
    """
    return bool(re.match(r"^[1-9A-HJ-NP-Za-km-z]{43,44}$", address or ""))
