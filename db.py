# -*- coding: utf-8 -*-
"""Compatibility shim exposing :mod:`db_core` under the legacy name."""

from .db_core import *  # noqa: F401,F403
from .db_core import __all__  # noqa: F401
