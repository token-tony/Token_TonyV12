# -*- coding: utf-8 -*-
import os
import re
from typing import List

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- Environment / API Keys ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
SYNDICA_API_KEY = os.getenv("SYNDICA_API_KEY", "")
BITQUERY_API_KEY = os.getenv("BITQUERY_API_KEY", "")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "")

# Scheduled push targets (optional). Set via environment.
PUBLIC_CHAT_ID = int(os.getenv("PUBLIC_CHAT_ID", "0") or 0)
VIP_CHAT_ID = int(os.getenv("VIP_CHAT_ID", "0") or 0)

# Optional alternative providers for logs firehose
ALCHEMY_RPC_URL = os.getenv("ALCHEMY_RPC_URL", "").strip()
ALCHEMY_WS_URL = os.getenv("ALCHEMY_WS_URL", "").strip()
SYNDICA_RPC_URL = os.getenv("SYNDICA_RPC_URL", "").strip()
SYNDICA_WS_URL = os.getenv("SYNDICA_WS_URL", "").strip()

# --- API Endpoints ---
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
BITQUERY_URL = "https://graphql.bitquery.io/"
GECKO_API_URL = "https://api.geckoterminal.com/api/v2"
JUP_QUOTE_URL = "https://quote-api.jup.ag/v6/quote"
SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhJHfY6kAj2bDqQvZ2Wn9isqo7uis"

# Canonical stables on Solana (core quote assets)
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wQ1Y1AoG6CwY"
USDT_MINT = "Es9vMFrzaCERzsiDMHcRWNtNeBNZ6qKqc7C6dQY9jz4"

# Known quote mints used to filter base/quote pairs in discovery and parsing
# Include at least SOL to avoid treating it as a new token; extend as needed with USDC/USDT.
def _env_bool(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "y"}

# Base known quote mints: SOL + canonical stables for efficiency and reliability
KNOWN_QUOTE_MINTS = {SOL_MINT, USDC_MINT, USDT_MINT}

# Optional: comma-separated extra quote mints from env (e.g., LSDs/blue chips)
_extra_quotes = os.getenv("KNOWN_QUOTE_MINTS_EXTRA", "").strip()
if _extra_quotes:
    for m in re.split(r"[,\s]+", _extra_quotes):
        if m:
            KNOWN_QUOTE_MINTS.add(m)

# Optional: toggle extended quotes via flag + optional list (env-driven). Default empty.
EXTENDED_QUOTES = _env_bool("EXTENDED_QUOTES", "0")
_extended_list = os.getenv("EXTENDED_QUOTES_LIST", "").strip()
if EXTENDED_QUOTES and _extended_list:
    for m in re.split(r"[,\s]+", _extended_list):
        if m:
            KNOWN_QUOTE_MINTS.add(m)

# Rugcheck configuration
RUGCHECK_API_URL = os.getenv("RUGCHECK_API_URL", "https://api.rugcheck.xyz/v1").strip()
RUGCHECK_JWT = os.getenv("RUGCHECK_JWT", "").strip()

# Add Gemini AI configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()

# --- Rate Limits ---
API_RATE_LIMITS = {
    "birdeye": {"capacity": 1, "refill": 1, "interval": 1},
    "dexscreener": {"capacity": 300, "refill": 300, "interval": 60},
    "gecko": {"capacity": 30, "refill": 30, "interval": 60},
    "helius": {"capacity": 100, "refill": 100, "interval": 1},
    "jupiter": {"capacity": 100, "refill": 100, "interval": 1},
    "generic": {"capacity": 10, "refill": 10, "interval": 1},
}

# --- Configuration ---
CONFIG = {
    # Default DB path moved into 'data/' to keep project root tidy
    # DB path can be overridden via env var DB_FILE (recommended to place outside cloud-synced folders)
    "DB_FILE": os.getenv("DB_FILE", "data/tony_memory.db").strip() or "data/tony_memory.db",
    "QUIP_FILE": "Token_Tony_Advanced_Quips.txt",
    "HTTP_TIMEOUT": 10.0,
    "HTTP_RETRIES": 2,
    # Maintenance & retention
    "SNAPSHOT_RETENTION_DAYS": 14,
    "REJECTED_RETENTION_DAYS": 7,
    "MAINTENANCE_INTERVAL_HOURS": 24,
    # Optional full reset interval; 0 disables
    "FULL_PURGE_INTERVAL_DAYS": 0,

    # Discovery & Analysis Settings
    "AGGREGATOR_POLL_INTERVAL_MINUTES": 1,
    "ENABLE_BACKUP_STREAMS": True,
    # Cap discovery per poll to keep backlog manageable (0 = unlimited)
    "AGGREGATOR_MAX_NEW_PER_CYCLE": 30,
    # Global fallbacks; worker will use bucket cadences below
    "RE_ANALYZER_INTERVAL_MINUTES": 2,
    "FIREHOSE_SIGNATURE_CACHE": 8000,
    "MIN_LIQUIDITY_FOR_HATCHING": 300, # Lowered to admit more newborns; explicit zero still excluded later
    # Initial analyzer throughput
    "INITIAL_ANALYZER_BATCH_SIZE": 60,
    "INDEXING_WAIT_SECONDS": 30,
    # Triage: drop tokens with no Jupiter route after a grace period (minutes)
    "TRIAGE_ROUTE_GRACE_MINUTES": 10,

    # Command Settings
    "COMMAND_COOLDOWN_HOURS": 6,
    "MIN_SCORE_TO_SHOW": 15, # The absolute floor score for a token to appear in any command.
    "FRESH_COMMAND_LIMIT": 2, # Standardized to 2
    "FRESH_MAX_AGE_HOURS": 24,
    "HATCHING_COMMAND_LIMIT": 2, # Standardized to 2
    # Buckets & cadences
    # Hatching: newborn (≤30m)
    "HATCHING_MAX_AGE_MINUTES": 180,
    # Per-bucket re-analysis cadence (minutes)
    "HATCHING_REANALYZE_MINUTES": 2,
    "FRESH_REANALYZE_MINUTES": 12,
    "COOKING_REANALYZE_MINUTES": 5,
    "OTHER_REANALYZE_MINUTES": 45,
    "COOKING_COMMAND_LIMIT": 2, # Standardized to 2
    # When 'cooking' tag is empty, fall back to high-volume tokens
    "COOKING_FALLBACK_VOLUME_MIN_USD": 100,
    "TOP_COMMAND_LIMIT": 2, # Standardized to 2
    "COOKING_LOOKBACK_HOURS": 3,
    "COOKING_VOLUME_SPIKE_MULTIPLIER": 4.0,
    # Per-command visibility floors (young tokens often have active authorities)
    "FRESH_MIN_SCORE_TO_SHOW": 5,
    "HATCHING_MIN_SCORE_TO_SHOW": 0,
    # Analyzer behavior
    "DISCOVERED_RETRY_MINUTES": 5,
    # Freshness & caching
    "SNAPSHOT_STALENESS_SECONDS": 1200,
    # Re-analyzer throughput control
    "RE_ANALYZER_BATCH_LIMIT": 50,
    "RE_ANALYZER_FETCH_CONCURRENCY": 6,
    # Telegram HTTP client tuning
    "TELEGRAM_POOL_SIZE": 80,
    "TELEGRAM_POOL_TIMEOUT": 60.0,
    "TELEGRAM_CONNECT_TIMEOUT": 20.0,
    "TELEGRAM_READ_TIMEOUT": 30.0,
    "TELEGRAM_GLOBAL_RATE_LIMIT": 30,
    "TELEGRAM_GROUP_RATE_LIMIT": 20,
    "TELEGRAM_GROUP_RATE_LIMIT_INTERVAL": 60,
    "TELEGRAM_CHAT_RATE_LIMIT": 1,
    "TELEGRAM_CHAT_RATE_LIMIT_INTERVAL": 1,
    # Don’t clamp liq to 0 on missing Jupiter routes for very young tokens (minutes)
    "JUP_CLAMP_MIN_AGE_MINUTES": 180,
    # IPFS tuning
    "IPFS_GATEWAY_DNS_TTL_MINUTES": 5,
    "IPFS_FETCH_TIMEOUT_SECONDS": 5,
    # Hedge delay for secondary IPFS request (ms). 0 disables hedging.
    "IPFS_HEDGE_MS": 800,
    # Runtime-tweakable knobs
    "JUP_SLIPPAGE_BPS": 300,
    # Testing helper: when >0, schedule /fresh push every N seconds (Public/VIP)
    # Default 0 because /fresh now has a real 60s cadence below.
    "FRESH_TEST_INTERVAL_SECONDS": 0,
    # Retain raw 'discovered' items for at most this many hours before dropping to avoid permanent queue bloat
    "DISCOVERED_RETENTION_HOURS": 8,
    # Allow explicit zero-liquidity for very young tokens in /fresh (minutes)
    "FRESH_ZERO_LIQ_AGE_MINUTES": 15,
    # Cooldowns
    "PUSH_COOLDOWN_HOURS": 1,
    "COMMAND_COOLDOWN_HOURS_COMMANDS": 4,
}

# Enhanced optimizations from new script
CONFIG.update({
    "ADAPTIVE_BATCH_SIZE": True,
    "MIN_BATCH_SIZE": 5,
    "MAX_BATCH_SIZE": 16,
    "TARGET_PROCESSING_TIME": 25.0,
    "PERFORMANCE_MONITORING": True,
    "INITIAL_ANALYSIS_CONCURRENCY": 10,
})

# Tony's enhanced configuration - bulletproof and comprehensive
CONFIG.update({
    # API Reliability & Circuit Breakers
    "HTTP_RETRIES": int(os.getenv("HTTP_RETRIES", "3")),
    "HTTP_TIMEOUT": float(os.getenv("HTTP_TIMEOUT", "15.0")),
    # Circuit breaker defaults tuned for noisy third-party APIs
    "CIRCUIT_BREAKER_FAILURE_THRESHOLD": float(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "0.8")),
    "CIRCUIT_BREAKER_MIN_REQUESTS": int(os.getenv("CIRCUIT_BREAKER_MIN_REQUESTS", "15")),
    "CIRCUIT_BREAKER_RESET_TIME": int(os.getenv("CIRCUIT_BREAKER_RESET_TIME", "900")),
    
    # Push scheduling enhancements
    "PUSH_DUPLICATE_PREVENTION": bool(os.getenv("PUSH_DUPLICATE_PREVENTION", "1")),
    "PUSH_ERROR_RETRY_DELAY": int(os.getenv("PUSH_ERROR_RETRY_DELAY", "300")),
    "PUSH_MAX_BACKOFF": int(os.getenv("PUSH_MAX_BACKOFF", "300")),
    
    # AI Integration settings
    "GEMINI_MAX_TOKENS": int(os.getenv("GEMINI_MAX_TOKENS", "80")),
    "GEMINI_TIMEOUT": float(os.getenv("GEMINI_TIMEOUT", "10.0")),
    "AI_EXPLANATION_CACHE_TTL": int(os.getenv("AI_EXPLANATION_CACHE_TTL", "1800")),
    "AI_FALLBACK_ENABLED": bool(os.getenv("AI_FALLBACK_ENABLED", "1")),
    
    # Enhanced diagnostics
    "STARTUP_CONFIG_LOG": bool(os.getenv("STARTUP_CONFIG_LOG", "1")),
    "DETAILED_ERROR_LOGGING": bool(os.getenv("DETAILED_ERROR_LOGGING", "1")),
    "HEALTH_CHECK_INTERVAL": int(os.getenv("HEALTH_CHECK_INTERVAL", "300")),
    
    # Fast check optimization
    "QUICK_CHECK_TIMEOUT": float(os.getenv("QUICK_CHECK_TIMEOUT", "5.0")),
    "DEEP_DIVE_TIMEOUT": float(os.getenv("DEEP_DIVE_TIMEOUT", "30.0")),
    
    # Cache optimization
    "FETCH_CACHE_SIZE": int(os.getenv("FETCH_CACHE_SIZE", "1000")),
    "FETCH_CACHE_TTL": int(os.getenv("FETCH_CACHE_TTL", "300")),
    
    # Maintenance settings
    "LOG_KEEP_COUNT": int(os.getenv("LOG_KEEP_COUNT", "7")),
    "WEEKLY_MAINTENANCE_ENABLED": bool(os.getenv("WEEKLY_MAINTENANCE_ENABLED", "1")),
})

# Optional: Plain-text output mode to avoid emoji issues (e.g., Telegram showing "??")
# Set TONY_PLAIN=1 in the environment to enable.
PLAIN_TEXT_MODE = str(os.getenv("TONY_PLAIN", os.getenv("TT_PLAIN", "0"))).strip().lower() in {"1", "true", "yes", "y"}

# Preferred IPFS gateways (primary -> fallbacks)
# Cloudflare is primary for performance, ipfs.io as immediate fallback, others as safety net.
IPFS_GATEWAYS_DEFAULT = (
    "https://cloudflare-ipfs.com/ipfs/",    # primary
    "https://ipfs.io/ipfs/",                # fallback
    "https://gateway.pinata.cloud/ipfs/",
    "https://w3s.link/ipfs/",
    "https://nftstorage.link/ipfs/",
    "https://gateway.ipfscdn.io/ipfs/",
)

def _normalize_gateway_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return u
    # Ensure it ends with '/ipfs/' for consistent concatenation
    if not u.endswith("/ipfs/"):
        if u.endswith("/ipfs"):
            u = u + "/"
        elif "/ipfs/" not in u:
            u = u.rstrip("/") + "/ipfs/"
    return u

def get_ipfs_gateways() -> List[str]:
    env_primary = os.getenv("IPFS_PRIMARY_GATEWAY", "").strip() or os.getenv("IPFS_DEDICATED_GATEWAY", "").strip()
    env_list = os.getenv("IPFS_GATEWAYS", "").strip()
    out: List[str] = []
    defaults = list(IPFS_GATEWAYS_DEFAULT)

    for g in defaults:
        if g not in out:
            out.append(g)

    if env_primary:
        p = _normalize_gateway_url(env_primary)
        if p and p not in out:
            out.append(p)
    if env_list:
        for raw in re.split(r"[,\s]+", env_list):
            g = _normalize_gateway_url(raw)
            if g and g not in out:
                out.append(g)
    return out

# Allow overriding CONFIG values via environment variables with the same key name.
# Types are inferred from the default values in CONFIG.
def _coerce_type(val: str, default):
    if isinstance(default, bool):
        return str(val).strip().lower() in {"1","true","yes","y","on"}
    if isinstance(default, int):
        try:
            return int(val)
        except Exception:
            return default
    if isinstance(default, float):
        try:
            return float(val)
        except Exception:
            return default
    return val

for _k, _v in list(CONFIG.items()):
    _env = os.getenv(_k)
    if _env is not None:
        CONFIG[_k] = _coerce_type(_env, _v)

# Tony's startup validation
def validate_config():
    """Tony's config validation - catch issues early."""
    issues = []
    
    # Critical API keys
    if not TELEGRAM_TOKEN:
        issues.append("TELEGRAM_TOKEN is required")
    
    # File system checks
    import os
    db_dir = os.path.dirname(CONFIG.get('DB_FILE', 'data/tony_memory.db'))
    if not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
        except Exception as e:
            issues.append(f"Cannot create database directory {db_dir}: {e}")
    
    log_dir = os.path.dirname(CONFIG.get('TONY_LOG_FILE', 'data/tony_log.log'))
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except Exception as e:
            issues.append(f"Cannot create log directory {log_dir}: {e}")
    
    # Warn about missing optional keys
    warnings = []
    if not HELIUS_API_KEY:
        warnings.append("HELIUS_API_KEY not set - some features limited")
    if not BIRDEYE_API_KEY:
        warnings.append("BIRDEYE_API_KEY not set - reduced data sources")
    if not GEMINI_API_KEY:
        warnings.append("GEMINI_API_KEY not set - AI explanations disabled")
    
    return issues, warnings

