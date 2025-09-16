# Token Tony - Telegram Bot

Token Tony is a Telegram bot that surfaces real-time Solana token intel with a blue‑collar, protective vibe — fast discovery, risk-aware scoring, and punchy reports.

See `mission.md` for the full Mission & Operating Guide.

## Features

- `/fresh`, `/hatching`, `/cooking`, `/top`: Curated token sets with live scores and rotation.
- `/check [mint]`: Deep scan for a specific token by mint (with chart when available).
- `/diag`: System diagnostics (providers, queues, buckets, scheduling, env hints).
- Auto-push cadence for public/VIP chats (optional, via `/setpublic` and `/setvip`).
- Maintenance (owner): `/dbclean`, `/dbprune`, `/dbpurge confirm`, `/logclean`.
- `/start`: Display a welcome message with instructions.

## Setup and Installation

### Prerequisites
- Python 3.8 or higher
- A Telegram Bot Token (get one from [BotFather](https://t.me/BotFather))

### Steps

1.  Clone the Repository
    ```bash
    git clone https://github.com/token-tony/sol-bot.git
    cd sol-bot
    ```

2.  Install Dependencies
    ```bash
    pip install -r requirements.txt
    ```

    The historical `tony_helpers` package that powered the API and database
    helpers is now vendored directly in this repository (see the `tony_helpers/`
    directory).  No extra dependency needs to be installed from PyPI – the local
    package re-exports the same helpers that older releases used.

3.  Configure Environment Variables
    Create a file named `.env` in the project root. Replace placeholders as needed.
    ```
    TELEGRAM_TOKEN=YOUR_TELEGRAM_TOKEN_HERE
    # Optional:
    # OWNER_ID=000000000              # Your Telegram user ID for admin commands
    # PUBLIC_CHAT_ID=-100123456789    # Channel/chat ID for public auto-pushes
    # VIP_CHAT_ID=-100987654321       # Channel/chat ID for VIP auto-pushes
    # HELIUS_API_KEY=...              # Improves discovery & analysis
    # BIRDEYE_API_KEY=...             # Improves pricing/market data
    # RUGCHECK_JWT=...                # Optional for RugCheck API
    # ALCHEMY_WS_URL=...              # Optional WS alternative for logs firehose
    # ALCHEMY_RPC_URL=...
    # SYNDICA_WS_URL=...
    # SYNDICA_RPC_URL=...
    # LOG_KEEP_COUNT=7                # How many rotated logs to keep
    # TONY_LOG_FILE=data/tony_log.log # Where logs are written
    # DB_FILE=data/tony_memory.db     # SQLite DB location
    # TONY_PLAIN=1                    # Plain text mode (if emoji rendering issues)
    #
    # Performance & Intake (override CONFIG)
    AGGREGATOR_MAX_NEW_PER_CYCLE=30
    INITIAL_ANALYSIS_CONCURRENCY=10
    RE_ANALYZER_BATCH_LIMIT=40
    TARGET_PROCESSING_TIME=25.0
    MIN_BATCH_SIZE=5
    MAX_BATCH_SIZE=16
    ADAPTIVE_BATCH_SIZE=1
    PERFORMANCE_MONITORING=1
    SNAPSHOT_STALENESS_SECONDS=1200
    DISCOVERED_RETENTION_HOURS=8
    TRIAGE_ROUTE_GRACE_MINUTES=10
    ```

4.  Run the Bot
    ```bash
    python Token_TonyV10.py
    ```

The bot should now be running. Use `/start` in a DM or configure channel permissions and `/setpublic`/`/setvip` (owner only) to enable auto-pushes.

### Maintenance and Cleanup
- The bot runs a background maintenance worker that:
  - Prunes old snapshots/rejected rows per retention settings
  - Drops stale discovered rows to avoid backlog bloat
  - Checkpoints the SQLite WAL file (`wal_checkpoint(TRUNCATE)`) to keep disk usage small
  - Periodically VACUUMs after pruning
- On-demand admin commands:
  - `/dbclean` — prune per retention settings and truncate WAL
  - `/dbprune` — same as above with status messages
  - `/dbpurge confirm` — wipe all DB state and VACUUM
  - `/logclean` — remove older rotated logs beyond the latest 7

- Weekly job: runs every Sunday 03:30 UTC to checkpoint WAL, VACUUM, and clean logs (respects `LOG_KEEP_COUNT`).
