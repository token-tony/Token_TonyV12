import copy
import sys
import types

import pytest

# Provide lightweight stubs so `analysis` can import optional helpers during tests.
_helpers_pkg = types.ModuleType("tony_helpers")
_helpers_api = types.ModuleType("tony_helpers.api")
_helpers_db = types.ModuleType("tony_helpers.db")

for name in (
    "_is_ipfs_uri",
    "fetch_birdeye",
    "fetch_creator_dossier_bitquery",
    "fetch_dexscreener_by_mint",
    "fetch_gecko_market_data",
    "fetch_helius_asset",
    "fetch_holders_count_via_rpc",
    "fetch_ipfs_json",
    "fetch_jupiter_has_route",
    "fetch_rugcheck_score",
    "fetch_top10_via_rpc",
    "fetch_twitter_stats",
):
    setattr(_helpers_api, name, lambda *args, **kwargs: None)

setattr(_helpers_db, "_execute_db", lambda *args, **kwargs: None)

_helpers_pkg.api = _helpers_api
_helpers_pkg.db = _helpers_db

sys.modules.setdefault("tony_helpers", _helpers_pkg)
sys.modules.setdefault("tony_helpers.api", _helpers_api)
sys.modules.setdefault("tony_helpers.db", _helpers_db)

from analysis import _compute_mms, _compute_sss
from config import CONFIG


@pytest.fixture(autouse=True)
def restore_scoring_config():
    original_sss = copy.deepcopy(CONFIG.get("SSS_SCORING", {}))
    original_mms = copy.deepcopy(CONFIG.get("MMS_SCORING", {}))
    yield
    CONFIG["SSS_SCORING"] = original_sss
    CONFIG["MMS_SCORING"] = original_mms


def test_sss_score_reflects_authority_penalty():
    sample = {
        "mint_authority": "SomeAuthority",
        "freeze_authority": None,
        "top10_holder_percentage": 10,
        "rugcheck_score": "Low",
        "creator_token_count": 1,
    }

    baseline = _compute_sss(sample)

    updated = copy.deepcopy(CONFIG["SSS_SCORING"])
    updated["authority_penalty"] = updated.get("authority_penalty", 60) + 10
    CONFIG["SSS_SCORING"] = updated

    adjusted = _compute_sss(sample)
    assert adjusted == max(0, baseline - 10)


def test_mms_score_changes_with_norm_adjustment():
    sample = {
        "liquidity_usd": 25_000,
        "volume_24h_usd": 15_000,
        "market_cap_usd": 75_000,
        "age_minutes": 120,
        "twitter_stats": {"followers": 5000},
        "price_change_24h": 5.0,
    }

    baseline = _compute_mms(sample)

    updated = copy.deepcopy(CONFIG["MMS_SCORING"])
    first_bracket = updated["age_brackets"][0]
    for metric in ("liquidity", "volume", "market_cap"):
        first_bracket["norms"][metric] = 1_000_000
    CONFIG["MMS_SCORING"] = updated

    adjusted = _compute_mms(sample)
    assert adjusted < baseline
