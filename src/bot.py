import logging
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

from src.config import Config
from src.database import Database
from src.analyzer import AIAnalyzer
from src.scheduler import Scheduler, format_instant_message, is_active_hours
from src.fetchers.fred import FREDFetcher
from src.fetchers.bls import BLSFetcher
from src.fetchers.bea import BEAFetcher
from src.fetchers.eia import EIAFetcher
from src.fetchers.eurostat import EurostatFetcher
from src.fetchers.cftc import CFTCFetcher
from src.fetchers.treasury import TreasuryDirectFetcher
from src.fetchers.atlanta_fed import AtlantaFedFetcher
from src.fetchers.forexfactory import ForexFactoryFetcher

logger = logging.getLogger("trading_bot")


class TradingBot:
    """Telegram bot with admin controls, auto scheduling, and queue-based messaging."""

    def __init__(self, db: Database):
        self.db = db
        self.analyzer = AIAnalyzer(db)

        # All data fetchers
        self.fetchers = {
            "fred": (FREDFetcher(db), "🇺🇸 FRED"),
            "bls": (BLSFetcher(db), "🇺🇸 BLS"),
            "bea": (BEAFetcher(db), "🇺🇸 BEA"),
            "eia": (EIAFetcher(db), "⛽ EIA"),
            "eurostat": (EurostatFetcher(db), "🇪🇺 Eurostat"),
            "cftc": (CFTCFetcher(db), "📊 CFTC COT"),
            "treasury": (TreasuryDirectFetcher(db), "🏛 Treasury"),
            "atlanta": (AtlantaFedFetcher(db), "🏦 Atlanta Fed"),
        }

        # ForexFactory (separate — no AI, just reminders)
        self.ff = ForexFactoryFetcher(db)

        self.app = (
            Application.builder()
            .token(Config.BOT_TOKEN)
            .build()
        )

        self.scheduler = Scheduler(
            db=db,
            analyzer=self.analyzer,
            fetchers=self.fetchers,
            ff_fetcher=self.ff,
            send_fn=self.send_to_channel,
            alert_fn=self.alert_admin,
        )

        self._register_handlers()

    def _register_handlers(self) -> None:
        commands = {
            # Admin controls
            "start": self._cmd_help,
            "help": self._cmd_help,
            "status": self._cmd_status,
            "auto": self._cmd_auto,
            "pause": self._cmd_pause,
            "resume": self._cmd_resume,
            # Manual triggers
            "fetch": self._cmd_fetch,
            "digest": self._cmd_digest,
            "calendar": self._cmd_calendar,
            "reminders": self._cmd_reminders,
            "markall": self._cmd_markall,
            # Monitoring
            "logs": self._cmd_logs,
            "spending": self._cmd_spending,
            "queue": self._cmd_queue,
            "sources": self._cmd_sources,
        }
        for name, handler in commands.items():
            self.app.add_handler(CommandHandler(name, handler))

    def _is_admin(self, update: Update) -> bool:
        return update.effective_user.id == Config.ADMIN_ID

    async def _admin_only(self, update: Update) -> bool:
        if not self._is_admin(update):
            await update.message.reply_text("⛔ Access denied.")
            return True
        return False

    # ========== Admin Controls ==========

    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        await update.message.reply_text(
            "Trading News Bot\n\n"
            "Controls:\n"
            "/status — Full status overview\n"
            "/auto on — Start automatic mode\n"
            "/auto off — Stop automatic mode\n"
            "/pause — Emergency stop\n"
            "/resume — Resume from pause\n\n"
            "Manual:\n"
            "/fetch <source> <limit> — Fetch & analyze\n"
            "/digest — Generate morning summary\n"
            "/calendar — Refresh ForexFactory events\n"
            "/reminders — Send pending reminders\n"
            "/markall — Mark all data as seen\n\n"
            "Monitoring:\n"
            "/logs — Recent activity\n"
            "/spending — Token costs\n"
            "/queue — Message queue\n"
            "/sources — Source health\n\n"
            "Sources: fred, bls, bea, eia, eurostat, cftc, treasury, atlanta"
        )

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return

        paused = self.db.is_paused()
        active = is_active_hours()
        auto = self.scheduler.is_auto_enabled

        if paused:
            state = "🔴 PAUSED"
        elif auto:
            state = f"🟢 AUTO {'(active)' if active else '(silent)'}"
        else:
            state = f"⚪ MANUAL {'(active hours)' if active else '(silent hours)'}"

        sources = self.db.get_all_source_statuses()
        failing = sum(1 for s in sources if s["fail_count"] >= 3)

        queue = self.db.get_queue_count()
        tokens = self.db.get_today_tokens()

        logs = self.db.get_recent_logs(1)
        last = logs[0]["timestamp"][11:16] if logs else "—"

        # Scheduled jobs info
        if auto:
            jobs = self.scheduler.get_jobs_info()
            jobs_str = f"\nJobs: {len(jobs)}"
        else:
            jobs_str = ""

        await update.message.reply_text(
            f"📊 Status\n\n"
            f"State: {state}{jobs_str}\n"
            f"Sources: {len(sources)} tracked, {failing} failing\n"
            f"Queue: {queue['pending']} pending, {queue['sent']} sent today\n"
            f"AI cost today: ${tokens['cost_total']:.4f}\n"
            f"Last activity: {last} UTC"
        )

    async def _cmd_auto(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return

        args = context.args
        if not args or args[0].lower() not in ("on", "off"):
            status = "ON" if self.scheduler.is_auto_enabled else "OFF"
            await update.message.reply_text(
                f"Auto mode is currently: {status}\n\n"
                f"Usage: /auto on or /auto off"
            )
            return

        if args[0].lower() == "on":
            if self.db.is_paused():
                await update.message.reply_text("⚠️ Bot is paused. Use /resume first.")
                return
            self.scheduler.start_auto()
            await update.message.reply_text(
                "🟢 Auto mode ON\n\n"
                "Sources will be checked automatically.\n"
                "Active hours (7AM-4PM London): instant messages.\n"
                "Silent hours: queued for morning digest.\n"
                "ForexFactory reminders: every 15 min.\n\n"
                "Use /auto off to stop."
            )
        else:
            self.scheduler.stop_auto()
            await update.message.reply_text(
                "⚪ Auto mode OFF\n\nUse /fetch for manual checks."
            )

    async def _cmd_pause(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        if self.db.is_paused():
            await update.message.reply_text("Already paused.")
            return
        self.db.set_paused(True)
        self.scheduler.stop_auto()
        self.db.log_activity("system", "pause", "Emergency stop by admin")
        logger.info("Bot PAUSED by admin")
        await update.message.reply_text(
            "🔴 PAUSED — everything stopped.\n"
            "Auto mode disabled. Use /resume then /auto on to restart."
        )

    async def _cmd_resume(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        if not self.db.is_paused():
            await update.message.reply_text("Not paused.")
            return
        self.db.set_paused(False)
        self.db.log_activity("system", "resume", "Resumed by admin")
        logger.info("Bot RESUMED by admin")
        await update.message.reply_text(
            "▶️ Resumed. Auto mode is still OFF.\n"
            "Use /auto on to restart automatic scheduling."
        )

    # ========== Manual Triggers ==========

    async def _cmd_fetch(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return

        args = context.args
        if not args:
            await update.message.reply_text(
                "Usage: /fetch <source> [limit]\n\n"
                f"Sources: {', '.join(self.fetchers.keys())}\n"
                "Default limit: 2"
            )
            return

        source = args[0].lower()
        limit = 2
        if len(args) > 1:
            if args[1].lower() == "all":
                limit = None
            else:
                try:
                    limit = int(args[1])
                except ValueError:
                    await update.message.reply_text(f"Invalid limit: {args[1]}")
                    return

        if source not in self.fetchers:
            await update.message.reply_text(
                f"Unknown source: {source}\n"
                f"Available: {', '.join(self.fetchers.keys())}"
            )
            return

        if self.db.is_paused():
            await update.message.reply_text("Bot is paused. Use /resume first.")
            return

        fetcher, label = self.fetchers[source]
        await update.message.reply_text(f"🔄 Fetching {source.upper()}...")

        try:
            new_items = await fetcher.fetch_new_data(limit=limit)
        except Exception as e:
            await update.message.reply_text(f"❌ Fetch failed: {e}")
            return

        if not new_items:
            await update.message.reply_text(f"✅ {source.upper()} — no new data.")
            return

        await update.message.reply_text(f"📊 {len(new_items)} item(s). Analyzing...")

        active = is_active_hours()
        total_cost = 0.0
        sent_count = 0
        queued_count = 0

        for item in new_items:
            raw_text = fetcher.format_for_ai([item])
            result = await self.analyzer.analyze(source, raw_text)

            if result is None or not result.get("summary"):
                continue

            queue_id = self.db.add_to_queue(
                source=source,
                importance=result["importance"],
                category=result["category"],
                region=result["region"],
                title=result["title"],
                summary=result["summary"],
                impact=result["impact"],
            )
            self.db.mark_as_sent(source, item["item_hash"])
            total_cost += result.get("cost_usd", 0)

            if active:
                message = format_instant_message({
                    **result,
                    "source": source,
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
                })
                if await self.send_to_channel(message, parse_mode="HTML"):
                    self.db.mark_queue_sent(queue_id)
                    sent_count += 1
            else:
                queued_count += 1

        mode_info = f"Sent: {sent_count}" if active else f"Queued: {queued_count} (silent hours)"
        await update.message.reply_text(
            f"✅ Done\n{mode_info}\nCost: ${total_cost:.4f}"
        )

    async def _cmd_digest(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        pending = self.db.get_pending_digest_items()
        if not pending:
            await update.message.reply_text("No pending items for digest.")
            return
        await update.message.reply_text(f"Generating digest from {len(pending)} items...")
        success = await self.scheduler.generate_and_send_digest()
        await update.message.reply_text("✅ Digest sent!" if success else "❌ Failed. Check /logs.")

    async def _cmd_calendar(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        await update.message.reply_text("📅 Fetching calendar...")
        new_count = await self.ff.store_events()
        upcoming = self.db.get_ff_upcoming_events(10)

        if not upcoming:
            await update.message.reply_text(f"✅ {new_count} new events. No upcoming High impact events.")
            return

        lines = [f"✅ {new_count} new events stored.\n"]
        for e in upcoming:
            icon = "🔔" if not e["reminder_sent"] else "✅"
            lines.append(f"{icon} {e['title']} ({e['country']})")
            lines.append(f"   {e['event_time_london']} London")
            parts = []
            if e.get("forecast"):
                parts.append(f"Прогноз: {e['forecast']}")
            if e.get("previous"):
                parts.append(f"Пред: {e['previous']}")
            if parts:
                lines.append(f"   {' | '.join(parts)}")
        await update.message.reply_text("\n".join(lines))

    async def _cmd_reminders(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        events = await self.ff.get_upcoming_reminders()
        if not events:
            await update.message.reply_text("No events needing reminders in next 60-75 min.")
            return
        sent = 0
        for event in events:
            message = self.ff.format_reminder(event)
            if await self.send_to_channel(message, parse_mode="HTML"):
                self.db.mark_ff_reminder_sent(event["id"])
                sent += 1
        await update.message.reply_text(f"✅ {sent} reminder(s) sent.")

    async def _cmd_markall(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Mark all current data as seen — run before enabling auto mode."""
        if await self._admin_only(update):
            return

        args = context.args
        if not args or args[0].lower() != "confirm":
            await update.message.reply_text(
                "⚠️ This will mark ALL current data across all sources as 'already seen'.\n"
                "No AI calls, no messages sent. Just fills the database.\n\n"
                "After this, auto mode will only trigger on genuinely NEW data.\n\n"
                "Run: /markall confirm"
            )
            return

        await update.message.reply_text("🔄 Marking all current data as seen...")
        counts = await self.scheduler.mark_all_as_seen()

        lines = ["✅ Done. Items marked per source:\n"]
        for source, count in counts.items():
            if count < 0:
                lines.append(f"❌ {source}: failed")
            else:
                lines.append(f"✅ {source}: {count} items")

        await update.message.reply_text("\n".join(lines))

    # ========== Monitoring ==========

    async def _cmd_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        logs = self.db.get_recent_logs(10)
        if not logs:
            await update.message.reply_text("No activity yet.")
            return
        lines = []
        for log in logs:
            icon = "✅" if log["status"] == "ok" else "❌"
            time_str = log["timestamp"][11:16]
            summary = log["summary"] or log["action"]
            if len(summary) > 60:
                summary = summary[:57] + "..."
            lines.append(f"{icon} {time_str} {log['source']}: {summary}")
        await update.message.reply_text("Recent Activity\n\n" + "\n".join(lines))

    async def _cmd_spending(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        stats = self.db.get_today_tokens()
        limit = Config.DAILY_COST_LIMIT_USD
        pct = (stats["cost_total"] / limit * 100) if limit > 0 else 0
        filled = int(pct / 10)
        bar = "█" * filled + "░" * (10 - filled)
        await update.message.reply_text(
            f"AI Spending Today\n\n"
            f"Calls: {stats['call_count']}\n"
            f"Tokens: {stats['input_total']:,} in / {stats['output_total']:,} out\n"
            f"Cost: ${stats['cost_total']:.4f}\n"
            f"Limit: ${limit:.2f}\n"
            f"[{bar}] {pct:.1f}%"
        )

    async def _cmd_queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        stats = self.db.get_queue_count()
        active = is_active_hours()
        mode = "Active (sending)" if active else "Silent (queuing)"
        await update.message.reply_text(
            f"Message Queue\n\n"
            f"Mode: {mode}\n"
            f"Pending: {stats['pending']}\n"
            f"Sent: {stats['sent']}\n"
            f"Digested: {stats['digested']}\n"
            f"Total: {stats['total']}"
        )

    async def _cmd_sources(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        sources = self.db.get_all_source_statuses()
        if not sources:
            await update.message.reply_text("No sources registered yet.")
            return
        lines = []
        for s in sources:
            if not s["enabled"]:
                icon = "⏹"
            elif s["fail_count"] >= 3:
                icon = "🔴"
            elif s["fail_count"] >= 1:
                icon = "🟡"
            else:
                icon = "🟢"
            last = s["last_success"]
            last_str = last[5:16] if last else "never"
            lines.append(f"{icon} {s['source']} — {last_str}")
        await update.message.reply_text("Source Health\n\n" + "\n".join(lines))

    # ========== Channel Messaging ==========

    async def send_to_channel(self, text: str, parse_mode: str = "HTML") -> bool:
        try:
            await self.app.bot.send_message(
                chat_id=Config.CHANNEL_ID,
                text=text,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
            return True
        except Exception as e:
            logger.error("Channel send failed: %s", e)
            return False

    async def alert_admin(self, text: str) -> None:
        try:
            await self.app.bot.send_message(
                chat_id=Config.ADMIN_ID,
                text=f"🚨 {text}",
            )
        except Exception as e:
            logger.error("Admin alert failed: %s", e)

    def run(self) -> None:
        logger.info("Starting Telegram bot...")
        self.app.run_polling(drop_pending_updates=True)
