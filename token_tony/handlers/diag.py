# -*- coding: utf-8 -*-
"""/diag command for Token Tony."""
from __future__ import annotations

import time
from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from config import CONFIG, TELEGRAM_TOKEN, HELIUS_API_KEY, BIRDEYE_API_KEY
from token_tony.services.health import API_HEALTH, LITE_MODE_UNTIL
from token_tony.db_core import _execute_db
from token_tony.utils.telegram import _maybe_send_typing


async def diag(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Tony's comprehensive diagnostic report - everything you need to know."""
    await _maybe_send_typing(u)
    
    status_lines = ["🔧 **Tony's Full System Diagnostic**\n"]
    
    # Tony's config sanity check
    status_lines.append("**📋 Configuration Status:**")
    db_path = CONFIG.get('DB_FILE', 'data/tony_memory.db')
    log_path = CONFIG.get('TONY_LOG_FILE', 'data/tony_log.log')
    
    # Check file system access
    try:
        import os
        db_writable = os.access(os.path.dirname(db_path), os.W_OK) if os.path.exists(os.path.dirname(db_path)) else False
        log_writable = os.access(os.path.dirname(log_path), os.W_OK) if os.path.exists(os.path.dirname(log_path)) else False
    except Exception:
        db_writable = log_writable = False
    
    status_lines.append(f"• Database: `{db_path}` {'✅' if db_writable else '❌'}")
    status_lines.append(f"• Log file: `{log_path}` {'✅' if log_writable else '❌'}")
    status_lines.append(f"• Aggregator interval: {CONFIG.get('AGGREGATOR_POLL_INTERVAL_MINUTES', 1)}min")
    status_lines.append(f"• Re-analyzer batch: {CONFIG.get('RE_ANALYZER_BATCH_LIMIT', 40)}")
    status_lines.append(f"• Command cooldown: {CONFIG.get('COMMAND_COOLDOWN_HOURS', 12)}h")
    
    # Tony's API key inventory
    status_lines.append("\n**🔑 API Arsenal:**")
    status_lines.append(f"• Telegram: {'✅' if TELEGRAM_TOKEN else '❌'}")
    status_lines.append(f"• Helius: {'✅' if HELIUS_API_KEY else '❌'}")
    status_lines.append(f"• BirdEye: {'✅' if BIRDEYE_API_KEY else '❌'}")
    status_lines.append(f"• Gemini AI: {'✅' if os.getenv('GEMINI_API_KEY') else '❌'}")
    
    # Tony's API health monitoring
    status_lines.append("\n**🌐 API Health Status:**")
    for provider, stats in API_HEALTH.items():
        total = stats['success'] + stats['failure']
        if total > 0:
            success_rate = (stats['success'] / total) * 100
            circuit_status = "🔴 OPEN" if stats['circuit_open'] else "🟢 CLOSED"
            last_success = stats.get('last_success', 0)
            age = int(time.time() - last_success) if last_success else 999999
            age_str = f"{age}s ago" if age < 3600 else f"{age//3600}h ago" if age < 86400 else "old"
            status_lines.append(f"• {provider.title()}: {success_rate:.1f}% success, circuit {circuit_status}, last success {age_str}")
        else:
            status_lines.append(f"• {provider.title()}: No requests yet")
    
    # Tony's lite mode status
    if LITE_MODE_UNTIL > time.time():
        remaining = int(LITE_MODE_UNTIL - time.time())
        status_lines.append(f"\n⚠️ **Lite Mode Active** ({remaining}s remaining)")
        status_lines.append("*Tony's being conservative due to API issues*")
    
    # Tony's AI brain status
    try:
        from ai_router import get_ai_health_status
        ai_status = get_ai_health_status()
        status_lines.append("\n**🤖 AI Brain Status:**")
        status_lines.append(f"• Gemini configured: {'✅' if ai_status['gemini_configured'] else '❌'}")
        status_lines.append(f"• Explanation cache: {ai_status['cache_size']} entries")
        if ai_status.get('cache_hits', 0) + ai_status.get('cache_misses', 0) > 0:
            hit_rate = ai_status['cache_hits'] / (ai_status['cache_hits'] + ai_status['cache_misses']) * 100
            status_lines.append(f"• Cache hit rate: {hit_rate:.1f}%")
    except Exception as e:
        status_lines.append(f"\n**🤖 AI Brain Status:** Error - {e}")
    
    # Tony's queue monitoring
    try:
        discovered_count = await _execute_db("SELECT COUNT(*) FROM TokenLog WHERE status = 'discovered'", fetch='one')
        analyzing_count = await _execute_db("SELECT COUNT(*) FROM TokenLog WHERE status = 'analyzing'", fetch='one')
        analyzed_count = await _execute_db("SELECT COUNT(*) FROM TokenLog WHERE status = 'analyzed'", fetch='one')
        served_count = await _execute_db("SELECT COUNT(*) FROM TokenLog WHERE status = 'served'", fetch='one')
        
        status_lines.append("\n**📊 Tony's Queue Status:**")
        status_lines.append(f"• Discovered: {discovered_count[0] if discovered_count else 0}")
        status_lines.append(f"• Analyzing: {analyzing_count[0] if analyzing_count else 0}")
        status_lines.append(f"• Analyzed: {analyzed_count[0] if analyzed_count else 0}")
        status_lines.append(f"• Served: {served_count[0] if served_count else 0}")
    except Exception as e:
        status_lines.append(f"\n❌ Queue status error: {e}")
    
    # Tony's firehose monitoring
    status_lines.append("\n**🔥 Data Firehose Status:**")
    for source, status in FIREHOSE_STATUS.items():
        status_lines.append(f"• {source}: {status}")
    if provider_state:
        status_lines.append("\n**📡 Provider Health:**")
        now = time.time()
        for provider, stats in provider_state.items():
            last_success = stats.get("last_success") or 0
            if last_success:
                age = int(now - last_success)
                if age < 60:
                    last_success_str = f"{age}s ago"
                elif age < 3600:
                    last_success_str = f"{age // 60}m ago"
                elif age < 86400:
                    last_success_str = f"{age // 3600}h ago"
                else:
                    last_success_str = "stale"
            else:
                last_success_str = "never"
            last_failure = stats.get("last_failure") or 0
            if last_failure:
                fail_age = int(now - last_failure)
                if fail_age < 60:
                    last_failure_str = f"{fail_age}s ago"
                elif fail_age < 3600:
                    last_failure_str = f"{fail_age // 60}m ago"
                elif fail_age < 86400:
                    last_failure_str = f"{fail_age // 3600}h ago"
                else:
                    last_failure_str = "stale"
            else:
                last_failure_str = "never"
            failures = stats.get("consecutive_failures", 0)
            msg_total = stats.get("messages_received", 0)
            backoff = int(stats.get("current_backoff") or 0)
            parts = [
                f"• {provider}: {msg_total} msgs",
                f"last success {last_success_str}",
                f"consecutive failures {failures}",
            ]
            if last_failure_str != "never":
                parts.append(f"last failure {last_failure_str}")
            if failures:
                parts.append(f"backoff {backoff}s")
            if stats.get("last_error"):
                err = stats["last_error"]
                if len(err) > 80:
                    err = err[:77] + "..."
                parts.append(f"error `{err}`")
            status_lines.append(
                ", ".join(parts)
            )

    # Tony's bucket distribution
    try:
        bucket_query = """
            SELECT enhanced_bucket, COUNT(*) 
            FROM TokenLog 
            WHERE status IN ('analyzed', 'served') 
            AND enhanced_bucket IS NOT NULL
            GROUP BY enhanced_bucket
            ORDER BY COUNT(*) DESC
        """
        bucket_rows = await _execute_db(bucket_query, fetch='all')
        if bucket_rows:
            status_lines.append("\n**🪣 Token Buckets:**")
            for bucket, count in bucket_rows:
                status_lines.append(f"• {bucket}: {count}")
    except Exception as e:
        log.warning(f"Bucket stats error: {e}")
    
    # Tony's push status
    status_lines.append("\n**📢 Push Status:**")
    status_lines.append(f"• Active pushes: {len(ACTIVE_PUSHES)}")
    status_lines.append(f"• Failed pushes: {len(PUSH_FAILURES)}")
    if PUSH_FAILURES:
        for key, (last_fail, count) in list(PUSH_FAILURES.items())[:3]:
            age = int(time.time() - last_fail)
            status_lines.append(f"  - {key}: {count} failures, last {age}s ago")
    
    # Tony's performance metrics
    try:
        if hasattr(globals(), 'recent_processing_times') and recent_processing_times:
            import statistics
            avg_time = statistics.mean(recent_processing_times)
            status_lines.append("\n**⚡ Performance:**")
            status_lines.append(f"• Avg processing time: {avg_time:.1f}s")
            status_lines.append(f"• Current batch size: {adaptive_batch_size}")
    except Exception:
        pass
    
    report = "\n".join(status_lines)
    
    # Split if too long for Telegram
    if len(report) > 4000:
        parts = [report[i:i+4000] for i in range(0, len(report), 4000)]
        for i, part in enumerate(parts):
            if i == 0:
                await u.message.reply_text(part, parse_mode=ParseMode.MARKDOWN)
            else:
                await u.message.reply_text(f"**Diagnostic Report (Part {i+1}):**\n\n{part}", parse_mode=ParseMode.MARKDOWN)
    else:
        await u.message.reply_text(report, parse_mode=ParseMode.MARKDOWN)
