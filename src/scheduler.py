import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from src.config import Config
from src.database import Database
from src.analyzer import AIAnalyzer

logger = logging.getLogger("trading_bot")

LONDON_TZ = ZoneInfo("Europe/London")

REGION_EMOJI = {
    "США": "🇺🇸",
    "Европа": "🇪🇺",
    "Великобритания": "🇬🇧",
    "Азия": "🌏",
    "Мир": "🌍",
}

# Source schedule config: (active_interval_hours, silent_interval_hours)
SOURCE_INTERVALS = {
    "fred": (2, 6),
    "bls": (2, 6),
    "bea": (2, 6),
    "eia": (1, 6),
    "eurostat": (4, 6),
    "cftc": (4, 6),
    "treasury": (2, 6),
    "atlanta": (2, 6),
}


def is_active_hours() -> bool:
    """Check if we're in the active sending window (7AM-4PM London)."""
    now_london = datetime.now(LONDON_TZ)
    return 7 <= now_london.hour < 16


def format_instant_message(item: dict) -> str:
    """Format a queue item as an instant Telegram message."""
    importance = item.get("importance", 3)
    category = item.get("category", "")
    region = item.get("region", "")
    region_emoji = REGION_EMOJI.get(region, "🌍")
    title = item.get("title", "")
    summary = item.get("summary", "")
    impact = item.get("impact", "")
    source = item.get("source", "")
    timestamp = item.get("timestamp", "")[:16]

    if importance >= 4:
        importance_line = f"🔴 ({importance}/5)"
    elif importance == 3:
        importance_line = f"🟡 ({importance}/5)"
    else:
        importance_line = f"⚪ ({importance}/5)"

    lines = [
        importance_line,
        f"🏷 {category} | {region_emoji} {region}",
        "",
        title,
        summary,
    ]

    if impact:
        lines.append("")
        lines.append(f"Возможное влияние: {impact}")

    lines.append("")
    lines.append(f"Источник: {source.upper()} | {timestamp} UTC")

    return "\n".join(lines)


def format_digest_message(digest_text: str, item_count: int) -> str:
    """Format the morning digest as a Telegram message."""
    now_london = datetime.now(LONDON_TZ)
    date_str = now_london.strftime("%d %B %Y")

    lines = [
        f"Утренняя сводка — {date_str}",
        f"Событий за ночь: {item_count}",
        "",
        digest_text,
    ]

    return "\n".join(lines)


class Scheduler:
    """Controls automatic fetching, analysis, sending, digests, and reminders."""

    def __init__(self, db, analyzer, fetchers, ff_fetcher, send_fn, alert_fn):
        """
        Args:
            db: Database instance
            analyzer: AIAnalyzer instance
            fetchers: dict of {name: (fetcher_instance, label)}
            ff_fetcher: ForexFactoryFetcher instance
            send_fn: async function to send message to channel
            alert_fn: async function to alert admin
        """
        self.db = db
        self.analyzer = analyzer
        self.fetchers = fetchers
        self.ff = ff_fetcher
        self.send_fn = send_fn
        self.alert_fn = alert_fn
        self._scheduler = AsyncIOScheduler(timezone=LONDON_TZ)
        self._auto_enabled = False

    @property
    def is_auto_enabled(self) -> bool:
        return self._auto_enabled

    def start_auto(self) -> None:
        """Start automatic scheduling."""
        if self._auto_enabled:
            return

        # Add source check jobs — use active interval, the job itself
        # will skip or adjust based on active/silent hours
        for source_name, (active_h, silent_h) in SOURCE_INTERVALS.items():
            self._scheduler.add_job(
                self._auto_run_source,
                IntervalTrigger(hours=active_h),
                args=[source_name],
                id=f"source_{source_name}",
                replace_existing=True,
                max_instances=1,
            )

        # Morning digest — 7:00 AM London
        self._scheduler.add_job(
            self._auto_morning_digest,
            CronTrigger(hour=7, minute=0, timezone=LONDON_TZ),
            id="morning_digest",
            replace_existing=True,
        )

        # ForexFactory calendar refresh — 6:00 AM London daily
        self._scheduler.add_job(
            self._auto_refresh_calendar,
            CronTrigger(hour=6, minute=0, timezone=LONDON_TZ),
            id="ff_calendar",
            replace_existing=True,
        )

        # ForexFactory reminder check — every 15 minutes during active hours
        self._scheduler.add_job(
            self._auto_check_reminders,
            IntervalTrigger(minutes=15),
            id="ff_reminders",
            replace_existing=True,
            max_instances=1,
        )

        if not self._scheduler.running:
            self._scheduler.start()

        self._auto_enabled = True
        self.db.log_activity("system", "auto_on", "Automatic mode enabled")
        logger.info("Auto mode ENABLED — scheduler started with %d jobs",
                     len(self._scheduler.get_jobs()))

    def stop_auto(self) -> None:
        """Stop automatic scheduling."""
        if not self._auto_enabled:
            return

        self._scheduler.remove_all_jobs()
        self._auto_enabled = False
        self.db.log_activity("system", "auto_off", "Automatic mode disabled")
        logger.info("Auto mode DISABLED — all jobs removed")

    def get_jobs_info(self) -> list[str]:
        """Get info about scheduled jobs."""
        if not self._auto_enabled:
            return ["Auto mode is OFF"]
        jobs = self._scheduler.get_jobs()
        return [f"{j.id}: next run {j.next_run_time.strftime('%H:%M %Z') if j.next_run_time else 'N/A'}"
                for j in jobs]

    # --- Auto job handlers ---

    async def _auto_run_source(self, source_name: str) -> None:
        """Auto-scheduled source check."""
        if self.db.is_paused():
            return

        try:
            count = await self.run_source(source_name)
            if count > 0:
                logger.info("Auto: %s produced %d items", source_name, count)
        except Exception as e:
            logger.error("Auto: %s crashed: %s", source_name, e)

    async def _auto_morning_digest(self) -> None:
        """Auto-scheduled morning digest at 7AM London."""
        if self.db.is_paused():
            return

        logger.info("Auto: generating morning digest")
        try:
            await self.generate_and_send_digest()
        except Exception as e:
            logger.error("Auto: digest failed: %s", e)
            await self.alert_fn(f"Утренняя сводка не удалась: {e}")

    async def _auto_refresh_calendar(self) -> None:
        """Auto-scheduled ForexFactory calendar refresh at 6AM London."""
        if self.db.is_paused():
            return

        logger.info("Auto: refreshing ForexFactory calendar")
        try:
            count = await self.ff.store_events()
            logger.info("Auto: stored %d new FF events", count)
        except Exception as e:
            logger.error("Auto: FF calendar refresh failed: %s", e)

    async def _auto_check_reminders(self) -> None:
        """Auto-scheduled reminder check every 15 minutes."""
        if self.db.is_paused():
            return

        # Only send reminders during active hours
        if not is_active_hours():
            return

        try:
            events = await self.ff.get_upcoming_reminders()
            for event in events:
                message = self.ff.format_reminder(event)
                sent = await self.send_fn(message, parse_mode="HTML")
                if sent:
                    self.db.mark_ff_reminder_sent(event["id"])
                    logger.info("Auto: sent reminder for %s", event.get("title", "?"))
        except Exception as e:
            logger.error("Auto: reminder check failed: %s", e)

    # --- Core processing methods ---

    async def run_source(self, source_name: str) -> int:
        """Fetch and analyze one source. Returns number of items processed."""
        if self.db.is_paused():
            return 0

        fetcher, label = self.fetchers[source_name]

        try:
            new_items = await fetcher.fetch_new_data()
        except Exception as e:
            logger.error("%s fetch crashed: %s", source_name, e)
            fail_count = self.db.update_source_status(source_name, False)
            if fail_count >= 3:
                await self.alert_fn(
                    f"Источник {source_name.upper()} не работает уже {fail_count} раз подряд."
                )
            return 0

        if not new_items:
            return 0

        active = is_active_hours()
        processed = 0

        for item in new_items:
            raw_text = fetcher.format_for_ai([item])
            result = await self.analyzer.analyze(source_name, raw_text)

            if result is None or not result.get("summary"):
                continue

            queue_id = self.db.add_to_queue(
                source=source_name,
                importance=result["importance"],
                category=result["category"],
                region=result["region"],
                title=result["title"],
                summary=result["summary"],
                impact=result["impact"],
            )

            self.db.mark_as_sent(source_name, item["item_hash"])

            if active:
                message = format_instant_message({
                    **result,
                    "source": source_name,
                    "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
                })
                sent = await self.send_fn(message, parse_mode="HTML")
                if sent:
                    self.db.mark_queue_sent(queue_id)

            processed += 1

        if processed > 0:
            self.db.log_activity(
                source_name, "process",
                f"{processed} items, active={active}",
            )

        return processed

    async def run_all_sources(self) -> int:
        """Run all sources once. Returns total items processed."""
        total = 0
        for source_name in self.fetchers:
            count = await self.run_source(source_name)
            total += count
        return total

    async def mark_all_as_seen(self) -> dict:
        """Fetch all sources and mark everything as seen without AI or sending.
        Returns dict with counts per source.
        """
        counts = {}
        for source_name, (fetcher, label) in self.fetchers.items():
            try:
                items = await fetcher.fetch_new_data()
                for item in items:
                    self.db.mark_as_sent(source_name, item["item_hash"])
                counts[source_name] = len(items)
                logger.info("markall: %s — %d items marked", source_name, len(items))
            except Exception as e:
                logger.error("markall: %s failed: %s", source_name, e)
                counts[source_name] = -1

        self.db.log_activity("system", "markall", f"Marked all current data as seen")
        return counts

    async def generate_and_send_digest(self) -> bool:
        """Generate morning digest from queued items and send to channel."""
        pending = self.db.get_pending_digest_items()

        if not pending:
            logger.info("No pending items for digest")
            return False

        logger.info("Generating digest from %d items", len(pending))

        result = await self.analyzer.generate_digest(pending)

        if result is None or not result.get("text"):
            logger.error("Digest generation failed")
            return False

        message = format_digest_message(result["text"], len(pending))
        sent = await self.send_fn(message, parse_mode="HTML")

        if sent:
            queue_ids = [item["id"] for item in pending]
            self.db.mark_queue_digested(queue_ids)
            self.db.log_activity(
                "digest", "send",
                f"{len(pending)} items, ${result['cost_usd']:.4f}",
            )
            logger.info("Digest sent: %d items", len(pending))
            return True

        return False
