import logging
from datetime import datetime, timezone

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
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
from src.fetchers.fed import FedReserveFetcher
from src.fetchers.edgar import EDGARFetcher
from src.fetchers.nyfed import NYFedFetcher
from src.fetchers.lbma import LBMAFetcher
from src.fetchers.cleveland_fed import ClevelandFedFetcher
from src.fetchers.cnn_fg import CNNFearGreedFetcher
from src.fetchers.opec import OPECFetcher
from src.fetchers.wgc import WorldGoldFetcher
from src.fetchers.finanzagentur import FinanzagenturFetcher
from src.fetchers.iea import IEAFetcher
from src.fetchers.abs import ABSFetcher
from src.fetchers.ons import ONSFetcher
from src.fetchers.ism import ISMFetcher

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
            "fed": (FedReserveFetcher(db), "🏛 Fed Reserve"),
            "edgar": (EDGARFetcher(db), "📄 SEC/EDGAR"),
            "nyfed": (NYFedFetcher(db), "🏦 NY Fed"),
            "lbma": (LBMAFetcher(db), "🥇 LBMA"),
            "cleveland": (ClevelandFedFetcher(db), "🏦 Cleveland Fed"),
            "cnn": (CNNFearGreedFetcher(db), "📊 CNN Fear&Greed"),
            "opec": (OPECFetcher(db), "🛢 OPEC"),
            "wgc": (WorldGoldFetcher(db), "🥇 World Gold Council"),
            "dfa": (FinanzagenturFetcher(db), "🇩🇪 Finanzagentur"),
            "iea": (IEAFetcher(db), "⚡ IEA"),
            "abs": (ABSFetcher(db), "🇦🇺 ABS Australia"),
            "ons": (ONSFetcher(db), "🇬🇧 UK ONS"),
            "ism": (ISMFetcher(db), "🏭 ISM PMI"),
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
            pin_fn=self.send_to_channel_and_pin,
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
            "importance": self._cmd_importance,
            "menu": self._cmd_menu,
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
            "health": self._cmd_health,
        }
        for name, handler in commands.items():
            self.app.add_handler(CommandHandler(name, handler))

        # Inline button callbacks
        self.app.add_handler(CallbackQueryHandler(self._handle_callback))

    # --- Button helpers ---

    def _menu_keyboard(self) -> InlineKeyboardMarkup:
        """Main menu keyboard — dynamic pause/resume button."""
        paused = self.db.is_paused()
        if paused:
            pause_btn = InlineKeyboardButton("▶️ Resume", callback_data="cb_resume_confirm")
        else:
            pause_btn = InlineKeyboardButton("⏸ Pause", callback_data="cb_pause_confirm")

        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📊 Status", callback_data="cb_status"),
                InlineKeyboardButton("💰 Spending", callback_data="cb_spending"),
            ],
            [
                InlineKeyboardButton("🏥 Health", callback_data="cb_health"),
                InlineKeyboardButton("📋 Queue", callback_data="cb_queue"),
            ],
            [
                InlineKeyboardButton("📅 Calendar", callback_data="cb_calendar"),
                InlineKeyboardButton("🎚 Importance", callback_data="cb_importance"),
            ],
            [
                InlineKeyboardButton("🔇 Mute sources", callback_data="cb_sources"),
                pause_btn,
            ],
        ])

    def _back_to_menu(self) -> InlineKeyboardMarkup:
        """Just a back-to-menu button."""
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Menu", callback_data="cb_menu")],
        ])

    def _confirm_keyboard(self, action: str) -> InlineKeyboardMarkup:
        """Yes/No confirmation for dangerous actions."""
        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes", callback_data=f"cb_{action}_yes"),
                InlineKeyboardButton("❌ No", callback_data=f"cb_{action}_no"),
            ],
        ])

    async def _handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle all inline button callbacks."""
        query = update.callback_query
        if query.from_user.id != Config.ADMIN_ID:
            await query.answer("Access denied")
            return

        await query.answer()
        data = query.data

        if data == "cb_menu":
            await query.message.reply_text("📋 Menu", reply_markup=self._menu_keyboard())

        elif data == "cb_status":
            await self._send_status(query.message)

        elif data == "cb_spending":
            await self._send_spending(query.message, detail=False)

        elif data == "cb_spending_detail":
            await self._send_spending(query.message, detail=True)

        elif data == "cb_health":
            await self._send_health(query.message)

        elif data == "cb_queue":
            await self._send_queue(query.message)

        elif data == "cb_calendar":
            await query.message.reply_text("📅 Refreshing calendar...")
            await self._send_calendar(query.message)

        elif data == "cb_importance":
            await self._send_importance(query.message)

        elif data.startswith("cb_imp_set_"):
            val = int(data.split("_")[-1])
            self.db.set_importance_threshold(val)
            await self._send_importance(query.message, confirmed=val)

        elif data == "cb_sources":
            await self._send_source_toggles(query.message)

        elif data.startswith("cb_toggle_"):
            source_name = data[len("cb_toggle_"):]
            await self._toggle_source(query.message, source_name)

        elif data == "cb_pause_confirm":
            await query.message.reply_text(
                "⚠️ Are you sure you want to PAUSE the bot?\n"
                "All auto-checks will stop immediately.",
                reply_markup=self._confirm_keyboard("pause"),
            )

        elif data == "cb_pause_yes":
            self.db.set_paused(True)
            self.scheduler.stop_auto()
            self.db.log_activity("system", "pause", "Paused by admin")
            await query.message.reply_text(
                "🔴 PAUSED — everything stopped.\n"
                "Use /resume then /auto on to restart.",
                reply_markup=self._back_to_menu(),
            )

        elif data == "cb_pause_no":
            await query.message.reply_text(
                "✅ Cancelled. Bot continues running.",
                reply_markup=self._back_to_menu(),
            )

        elif data == "cb_resume_confirm":
            self.db.set_paused(False)
            self.db.log_activity("system", "resume", "Resumed by admin")
            logger.info("Bot RESUMED by admin via menu")
            await query.message.reply_text(
                "▶️ Resumed.\n\nEnable auto mode?",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("✅ Yes, turn on auto", callback_data="cb_auto_on"),
                        InlineKeyboardButton("❌ No, manual only", callback_data="cb_auto_no"),
                    ],
                ]),
            )

        elif data == "cb_auto_on":
            self.scheduler.start_auto()
            await query.message.reply_text(
                "🟢 Auto mode ON\n\n"
                "Sources will be checked automatically.",
                reply_markup=self._back_to_menu(),
            )

        elif data == "cb_auto_no":
            await query.message.reply_text(
                "⚪ Manual mode. Use /fetch for manual checks.",
                reply_markup=self._back_to_menu(),
            )

    def _is_admin(self, update: Update) -> bool:
        return update.effective_user.id == Config.ADMIN_ID

    async def _admin_only(self, update: Update) -> bool:
        if not self._is_admin(update):
            await update.message.reply_text("⛔ Access denied.")
            return True
        return False

    # ========== Admin Controls ==========

    async def _cmd_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        await update.message.reply_text("📋 Menu", reply_markup=self._menu_keyboard())

    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        await update.message.reply_text(
            "Trading News Bot\n\n"
            "Controls:\n"
            "/menu — Interactive menu with buttons\n"
            "/status — Full status overview\n"
            "/auto on|off — Start/stop automatic mode\n"
            "/pause — Emergency stop\n"
            "/resume — Resume from pause\n"
            "/importance <1-5> — Set min importance\n\n"
            "Manual:\n"
            "/fetch <source> <limit> — Fetch & analyze\n"
            "/digest — Generate morning summary\n"
            "/markall confirm — Mark all data as seen\n\n"
            "Monitoring:\n"
            "/spending — AI usage (add 'detail')\n"
            "/health — Source health\n"
            "/queue — Message queue\n"
            "/logs — Recent activity\n\n"
            f"Sources: {', '.join(self.fetchers.keys())}",
            reply_markup=self._menu_keyboard(),
        )

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        await self._send_status(update.message)

    async def _send_status(self, message) -> None:
        paused = self.db.is_paused()
        active = is_active_hours()
        auto = self.scheduler.is_auto_enabled
        threshold = self.db.get_importance_threshold()

        if paused:
            state = "🔴 PAUSED"
        elif auto:
            state = f"🟢 AUTO {'(active)' if active else '(silent)'}"
        else:
            state = f"⚪ MANUAL {'(active hours)' if active else '(silent hours)'}"

        sources = self.db.get_all_source_statuses()
        healthy = sum(1 for s in sources if s["fail_count"] == 0)
        failing = sum(1 for s in sources if s["fail_count"] > 0)

        queue = self.db.get_queue_count()
        tokens = self.db.get_today_tokens()
        total_tokens = tokens["input_total"] + tokens["output_total"]
        free_limit = Config.FREE_DAILY_TOKENS

        logs = self.db.get_recent_logs(1)
        last = logs[0]["timestamp"][11:16] if logs else "—"

        jobs_str = ""
        if auto:
            jobs = self.scheduler.get_jobs_info()
            jobs_str = f"\nJobs: {len(jobs)}"

        await message.reply_text(
            f"📊 Status\n\n"
            f"State: {state}{jobs_str}\n"
            f"Importance: {threshold}+\n"
            f"Sources: {healthy} 🟢  {failing} {'🔴' if failing else ''}\n"
            f"Queue: {queue['pending']} pending, {queue['sent']} sent\n"
            f"Tokens today: {total_tokens:,} / {free_limit:,}\n"
            f"Last activity: {last} UTC",
            reply_markup=self._menu_keyboard(),
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
            "▶️ Resumed.\n\nEnable auto mode?",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Yes, turn on auto", callback_data="cb_auto_on"),
                    InlineKeyboardButton("❌ No, manual only", callback_data="cb_auto_no"),
                ],
            ]),
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

            # Manual fetch always sends, regardless of active/silent hours
            message = format_instant_message({
                **result,
                "source": source,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            })
            if await self.send_to_channel(message, parse_mode="HTML"):
                self.db.mark_queue_sent(queue_id)
                sent_count += 1

        mode_info = f"Sent: {sent_count}"
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

    async def _cmd_importance(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        args = context.args
        current = self.db.get_importance_threshold()
        if not args:
            labels = {1: "All (1-5)", 2: "2+ only", 3: "3+ only", 4: "4+ only", 5: "Critical only (5)"}
            await update.message.reply_text(
                f"Importance Filter\n\n"
                f"Current threshold: {current} ({labels.get(current, '?')})\n"
                f"Messages below threshold go to morning digest only.\n\n"
                f"Set: /importance <1-5>",
                reply_markup=self._back_to_menu(),
            )
            return
        try:
            val = int(args[0])
            if val < 1 or val > 5:
                raise ValueError
        except ValueError:
            await update.message.reply_text("Use a number 1-5.")
            return
        self.db.set_importance_threshold(val)
        labels = {1: "все сообщения", 2: "важность 2+", 3: "важность 3+", 4: "важность 4+", 5: "только критичные (5)"}
        await update.message.reply_text(
            f"✅ Threshold set to {val} — {labels.get(val, '?')}\n"
            f"Messages with importance < {val} will only appear in morning digest.",
            reply_markup=self._back_to_menu(),
        )

    # ========== Monitoring ==========

    async def _cmd_logs(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        await self._send_logs(update.message)

    async def _send_logs(self, message) -> None:
        logs = self.db.get_recent_logs(10)
        if not logs:
            await message.reply_text("No activity yet.", reply_markup=self._back_to_menu())
            return
        lines = []
        for log in logs:
            icon = "✅" if log["status"] == "ok" else "❌"
            time_str = log["timestamp"][11:16]
            summary = log["summary"] or log["action"]
            if len(summary) > 60:
                summary = summary[:57] + "..."
            lines.append(f"{icon} {time_str} {log['source']}: {summary}")
        await message.reply_text(
            "Recent Activity\n\n" + "\n".join(lines),
            reply_markup=self._back_to_menu(),
        )

    async def _cmd_spending(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        detail = bool(context.args and context.args[0].lower() == "detail")
        await self._send_spending(update.message, detail=detail)

    async def _send_spending(self, message, detail: bool = False) -> None:
        stats = self.db.get_today_tokens()
        free_limit = Config.FREE_DAILY_TOKENS
        total_tokens = stats["input_total"] + stats["output_total"]
        pct = (total_tokens / free_limit * 100) if free_limit > 0 else 0
        filled = min(10, int(pct / 10))
        bar = "█" * filled + "░" * (10 - filled)

        # Calculate paid cost (only for overage)
        if total_tokens > free_limit:
            overage_in = max(0, stats["input_total"] - int(free_limit * 0.8))
            overage_out = max(0, stats["output_total"] - int(free_limit * 0.2))
            paid = (overage_in / 1_000_000) * Config.INPUT_COST_PER_M + \
                   (overage_out / 1_000_000) * Config.OUTPUT_COST_PER_M
            cost_line = f"⚠️ OVER LIMIT — Paid: ${paid:.4f}"
        else:
            cost_line = f"Free remaining: {free_limit - total_tokens:,} tokens"

        text = (
            f"💰 AI Usage Today\n\n"
            f"Tokens: {total_tokens:,} / {free_limit:,}\n"
            f"[{bar}] {pct:.1f}%\n"
            f"Input: {stats['input_total']:,}  |  Output: {stats['output_total']:,}\n"
            f"Calls: {stats['call_count']}\n"
            f"{cost_line}"
        )

        if detail:
            by_source = self.db.get_spending_by_source_today()
            if by_source:
                text += "\n\nBy source today:"
                for row in by_source:
                    text += f"\n  {row['source']}: {row['calls']} calls"

            history = self.db.get_spending_daily_history(7)
            if history:
                text += "\n\n7-day history:"
                total_7d_tokens = 0
                for row in history:
                    text += f"\n  {row['date']}: {row['calls']} calls"
                    total_7d_tokens += 1

            keyboard = self._back_to_menu()
        else:
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("📊 Detail", callback_data="cb_spending_detail"),
                    InlineKeyboardButton("📋 Menu", callback_data="cb_menu"),
                ],
            ])

        await message.reply_text(text, reply_markup=keyboard)

    async def _cmd_queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        await self._send_queue(update.message)

    async def _send_queue(self, message) -> None:
        stats = self.db.get_queue_count()
        active = is_active_hours()
        threshold = self.db.get_importance_threshold()
        mode = "Active (sending)" if active else "Silent (queuing)"
        await message.reply_text(
            f"📋 Message Queue\n\n"
            f"Mode: {mode}\n"
            f"Importance filter: {threshold}+\n"
            f"Pending: {stats['pending']}\n"
            f"Sent: {stats['sent']}\n"
            f"Digested: {stats['digested']}\n"
            f"Total: {stats['total']}",
            reply_markup=self._back_to_menu(),
        )

    async def _cmd_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await self._admin_only(update):
            return
        await self._send_health(update.message)

    async def _send_health(self, message) -> None:
        sources = self.db.get_all_source_statuses()
        if not sources:
            await message.reply_text(
                "No sources registered yet. Run /markall confirm first.",
                reply_markup=self._back_to_menu(),
            )
            return

        healthy = 0
        warning = 0
        failing = 0
        lines = []

        for s in sources:
            if not s["enabled"]:
                icon = "⏹"
            elif s["fail_count"] >= 3:
                icon = "🔴"
                failing += 1
            elif s["fail_count"] >= 1:
                icon = "🟡"
                warning += 1
            else:
                icon = "🟢"
                healthy += 1

            last = s["last_success"]
            if last:
                last_str = last[5:16]
            else:
                last_str = "never"

            err_info = ""
            if s.get("errors_today", 0) > 0:
                err_info = f" ({s['errors_today']} err)"
            if s["fail_count"] > 0 and s.get("last_error_msg"):
                err_info = f" [{s['last_error_msg'][:40]}]"

            lines.append(f"{icon} {s['source']}: {last_str}{err_info}")

        header = f"🏥 Source Health: {healthy} 🟢  {warning} 🟡  {failing} 🔴\n"
        await message.reply_text(
            header + "\n" + "\n".join(lines),
            reply_markup=self._back_to_menu(),
        )

    async def _send_calendar(self, message) -> None:
        """Refresh ForexFactory calendar, then show full week calendar."""
        try:
            new_count = await self.ff.store_events()
        except Exception as e:
            logger.error("Calendar refresh failed: %s", e)
            new_count = 0

        events = self.db.get_ff_all_week_events()
        if not events:
            await message.reply_text(
                f"📅 Calendar refreshed ({new_count} new).\nNo High impact events this week.",
                reply_markup=self._back_to_menu(),
            )
            return

        now_utc = datetime.now(timezone.utc).isoformat()

        lines = [f"📅 This Week — High Impact ({len(events)} events)\n"]
        current_day = None

        for e in events:
            london_time = e.get("event_time_london", "")

            # Parse "HH:MM, DD Month YYYY" format
            if ", " in london_time:
                time_part, date_part = london_time.split(", ", 1)
            else:
                time_part = ""
                date_part = london_time

            # Group by date
            if date_part and date_part != current_day:
                current_day = date_part
                lines.append(f"\n{date_part}:")

            # Determine status
            is_past = e.get("event_time_utc", "") < now_utc
            if is_past:
                icon = "✅"
            elif e["reminder_sent"]:
                icon = "🔔"
            else:
                icon = "⏳"

            parts = []
            if e.get("forecast"):
                parts.append(f"F: {e['forecast']}")
            if e.get("previous"):
                parts.append(f"P: {e['previous']}")
            extra = f" ({' | '.join(parts)})" if parts else ""

            lines.append(f"  {icon} {time_part} {e['title']} — {e['country']}{extra}")

        await message.reply_text(
            "\n".join(lines),
            reply_markup=self._back_to_menu(),
        )

    async def _send_importance(self, message, confirmed: int | None = None) -> None:
        """Show importance threshold with selection buttons."""
        current = self.db.get_importance_threshold()
        labels = {1: "All", 2: "2+", 3: "3+", 4: "4+", 5: "5"}

        if confirmed:
            text = f"✅ Set to {labels.get(confirmed, '?')}\n\n"
        else:
            text = ""

        text += (
            f"🎚 Importance Filter\n\n"
            f"Messages below threshold go to morning digest only.\n"
            f"Select threshold:"
        )

        buttons = []
        for val in range(1, 6):
            check = "✅ " if val == current else ""
            buttons.append([InlineKeyboardButton(
                f"{check}{labels[val]}",
                callback_data=f"cb_imp_set_{val}",
            )])

        buttons.append([InlineKeyboardButton("📋 Menu", callback_data="cb_menu")])

        await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    async def _send_source_toggles(self, message) -> None:
        """Show all sources with mute/unmute toggle buttons."""
        sources = self.db.get_all_source_statuses()
        source_keys = list(self.fetchers.keys())

        # Build enabled map from DB
        enabled_map = {}
        for s in sources:
            enabled_map[s["source"]] = s.get("enabled", 1)

        buttons = []
        row = []
        for key in source_keys:
            is_enabled = enabled_map.get(key, 1)
            icon = "🔊" if is_enabled else "🔇"
            row.append(InlineKeyboardButton(
                f"{icon} {key}",
                callback_data=f"cb_toggle_{key}",
            ))
            if len(row) == 2:
                buttons.append(row)
                row = []

        if row:
            buttons.append(row)

        buttons.append([InlineKeyboardButton("📋 Menu", callback_data="cb_menu")])

        muted = sum(1 for v in enabled_map.values() if not v)
        header = f"🔇 Source Toggles ({muted} muted)\nTap to toggle:"

        await message.reply_text(header, reply_markup=InlineKeyboardMarkup(buttons))

    async def _toggle_source(self, message, source_name: str) -> None:
        """Toggle a source on/off and refresh the buttons."""
        if source_name not in self.fetchers:
            return

        with self.db._connect() as conn:
            row = conn.execute(
                "SELECT enabled FROM source_status WHERE source = ?",
                (source_name,),
            ).fetchone()

            if row is None:
                # Source not in DB yet, insert as disabled
                conn.execute(
                    """INSERT INTO source_status
                       (source, last_check, fail_count, enabled)
                       VALUES (?, ?, 0, 0)""",
                    (source_name, datetime.now(timezone.utc).isoformat()),
                )
                new_state = 0
            else:
                new_state = 0 if row["enabled"] else 1
                conn.execute(
                    "UPDATE source_status SET enabled = ? WHERE source = ?",
                    (new_state, source_name),
                )

        action = "unmuted" if new_state else "muted"
        logger.info("Source %s %s by admin", source_name, action)
        self.db.log_activity("system", "toggle_source", f"{source_name} {action}")

        # Refresh the toggle display
        await self._send_source_toggles(message)

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

    async def send_to_channel_and_pin(self, text: str, parse_mode: str = "HTML") -> bool:
        """Send message to channel and pin it (without unpinning previous pins)."""
        try:
            msg = await self.app.bot.send_message(
                chat_id=Config.CHANNEL_ID,
                text=text,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
            await self.app.bot.pin_chat_message(
                chat_id=Config.CHANNEL_ID,
                message_id=msg.message_id,
                disable_notification=True,
            )
            return True
        except Exception as e:
            logger.error("Channel send+pin failed: %s", e)
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
