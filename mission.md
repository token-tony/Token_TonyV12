# Token Tony Universe — Mission & Operating Guide

Token Tony is the blue‑collar alpha dad of Solana: the grounded protector who hunts fresh plays, warns against rugs, and delivers punchy, data‑backed reports. Token the Owl is Tony’s sharp‑eyed sidekick — the scout with vision who spots danger and opportunity early. Together they’re a duo: protector + hype, signal + style.

## Mission
- Hunt fresh SOL tokens fast and reliably.
- Protect against rugs with real risk signals.
- Scout moonshots before they break out.
- Blend trust, memes, and alpha into a shareable brand.

## System Overview
- Pot System: a living “stew” of 300–500 active tokens — always full, never clogged. Losers get discarded to keep flow healthy.
- Buckets (dynamic, rotating):
  - Hatching: newborn (minutes old), limited intel, neutral quips.
  - Fresh: young (≤24h), compact reports with scores.
  - Cooking: momentum heating up (volume/price spikes, route sanity).
  - Top (Top Shelf): Tony’s best picks now, highest confidence and score.
  - Scrap Heap: rugs/dead/low‑score leftovers.
- Rotation: coins move buckets as they age or heat up; discards are pruned quickly.

## Re‑analysis Cadence (current defaults)
- Hatching: every 2 minutes (`HATCHING_REANALYZE_MINUTES=2`).
- Fresh: every ~12 minutes (`FRESH_REANALYZE_MINUTES=12`).
- Cooking: every 5 minutes (`COOKING_REANALYZE_MINUTES=5`).
- Others: every ~45 minutes (`OTHER_REANALYZE_MINUTES=45`).
- Discards/maintenance: periodic retention pruning and WAL checkpoint.

## Scoring Model
- SSS — Safety Score: mint/freeze authorities, top holders concentration, RugCheck, creator history.
- MMS — Momentum/Maturity Score: liquidity, volume, market cap, age, socials, price action.
- Final Score: age‑weighted blend with confidence drag for incomplete data.
  - Young (<7 days): roughly balanced SSS/MMS.
  - Mid (≤30 days): MMS weighted.
  - Older: MMS strongly weighted.
- Grades (report tone and icons):
  - Moonshot (≥85) — 🚀
  - Promising (≥65) — 📈/🔥
  - Risky (≥40) — ⚠️
  - Danger (<40) — 💀 / 🪤

## Commands (current script)
- `/start`: quick intro and menu.
- `/fresh`: compact list from the Fresh bucket with current scores and quips.
- `/hatching`: newborns (minutes old) with neutral tone and limited data.
- `/cooking`: heated momentum plays (volume/price spike logic with fallbacks).
- `/top`: “Top Shelf” picks — highest scoring and filtered for quality.
- `/check <mint>`: deep dive on a specific mint (intel + market snapshot + chart image when available).
- `/diag`: system diagnostics (providers, queues, buckets, scheduling, env hints).
- Owner/admin only: `/setpublic`, `/setvip` (schedule auto‑pushes), `/dbclean`, `/dbprune`, `/dbpurge confirm`, `/logclean`.
- Cooldowns: per‑coin/list cooldowns to ensure healthy rotation (`COMMAND_COOLDOWN_HOURS=12`).

## Operating Sources & Flow
Discovery (cheap, real‑time)
- PumpPortal WebSocket: new token/migration events.
- Logs Firehose over WebSocket (Helius; optional Alchemy/Syndica URLs): subscribe to key Raydium program logs to catch pool births.

Enrichment (tiered, cached)
- DexScreener: primary live market data and charts.
- BirdEye: market/holders snapshot; used when fast and available.
- GeckoTerminal: fallback market data when DexScreener lacks coverage.
- Helius: asset metadata, mints, creator wallet traces, top holders via RPC.
- RugCheck: risk labeling and score cues.
- Jupiter: route sanity to avoid untradable/walled tokens.

Caching & Efficiency
- Stale snapshot tolerance: `SNAPSHOT_STALENESS_SECONDS=1200` with just‑in‑time refresh before sending.
- Tiered fallbacks minimize network cost and surface useful intel even when partial.
- Batching and adaptive concurrency (`ADAPTIVE_BATCH_SIZE`, `INITIAL_ANALYSIS_CONCURRENCY`) keep latency stable.

## Maintenance & Reliability
- Auto‑maintenance worker: prunes snapshots and rejected rows by retention, drops stale “discovered” items, checkpoints WAL, and VACUUMs periodically.
- On‑demand admin: `/dbclean`, `/dbprune`, `/dbpurge confirm`, `/logclean`.
- Weekly housekeeping: WAL checkpoint, VACUUM, log rotation cleanup.

## Brand Voice & Quips
- Tone: conversational, confident, witty; alpha hunter meets protective dad.
- Reports: short, scannable, data‑first; emojis as visual cues.
- Samples:
  - “Tony smells alpha — and it’s not just his cologne.”
  - “You’re not early — you’re the exit liquidity.”
  - “Chart talkin’. Tony listenin’.”
  - “Data > drama. Always.”

## Configuration (essentials)
Environment
- `TELEGRAM_TOKEN` (required), `OWNER_ID`.
- `PUBLIC_CHAT_ID`, `VIP_CHAT_ID` (optional for auto‑pushes).
- `HELIUS_API_KEY`, `BIRDEYE_API_KEY` (improves discovery and analysis).
- Optional: `RUGCHECK_JWT`, `ALCHEMY_WS_URL`/`ALCHEMY_RPC_URL`, `SYNDICA_WS_URL`/`SYNDICA_RPC_URL`.
- Output mode: `TONY_PLAIN=1` for plain‑text if emoji rendering is problematic.

Behavioral knobs (selected)
- Discovery/throughput: `AGGREGATOR_MAX_NEW_PER_CYCLE=30`, `INITIAL_ANALYSIS_CONCURRENCY=10`, `RE_ANALYZER_BATCH_LIMIT=40`.
- Cadence: `HATCHING_REANALYZE_MINUTES=2`, `FRESH_REANALYZE_MINUTES=12`, `COOKING_REANALYZE_MINUTES=5`, `OTHER_REANALYZE_MINUTES=45`.
- Freshness: `SNAPSHOT_STALENESS_SECONDS=1200`.
- Visibility floors: `MIN_SCORE_TO_SHOW=20`, `FRESH_MIN_SCORE_TO_SHOW=5`, `HATCHING_MIN_SCORE_TO_SHOW=0`.
- Bucket sizing: `FRESH_MAX_AGE_HOURS=24`, `HATCHING_MAX_AGE_MINUTES=30`.
- Liquidity admission: `MIN_LIQUIDITY_FOR_HATCHING=25`, `FRESH_ZERO_LIQ_AGE_MINUTES=15` grace window.
- Cooldowns: `COMMAND_COOLDOWN_HOURS=12`, `PUSH_COOLDOWN_HOURS=1`.

## Runbook
1) Install deps: `pip install -r requirements.txt`.
2) Configure `.env` (token, IDs, API keys; see README.md for full list).
3) Start: `python Token_TonyV10.py`.
4) Optional: set channel auto‑push via `/setpublic` and `/setvip` after granting the bot post permissions.

## Files of Interest
- `Token_TonyV10.py`: Telegram bot, schedulers, commands, buckets, WS workers.
- `analysis.py`: SSS/MMS scoring, intel aggregation, fallbacks, final score.
- `api.py`: HTTP/RPC to DexScreener, BirdEye, GeckoTerminal, Helius, RugCheck, Jupiter.
- `config.py`: env, endpoints, behavior knobs, IPFS gateways.
- `reports.py` and `Token_Tony_Advanced_Quips.txt`: report formatting and Tony’s voice.

— Spot the moonshots. Dodge the rugs. Deliver alpha — the Token Tony way.
