import hashlib
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp

from src.database import Database

logger = logging.getLogger("trading_bot")

LONDON_TZ = ZoneInfo("Europe/London")

# Country code to region/flag mapping
COUNTRY_MAP = {
    "USD": ("США", "🇺🇸"),
    "EUR": ("Еврозона", "🇪🇺"),
    "GBP": ("Великобритания", "🇬🇧"),
    "JPY": ("Япония", "🇯🇵"),
    "AUD": ("Австралия", "🇦🇺"),
    "NZD": ("Новая Зеландия", "🇳🇿"),
    "CAD": ("Канада", "🇨🇦"),
    "CHF": ("Швейцария", "🇨🇭"),
    "CNY": ("Китай", "🇨🇳"),
}


class ForexFactoryFetcher:
    """Fetches economic calendar from ForexFactory mirror and manages reminders."""

    def __init__(self, db: Database):
        self.db = db
        self.calendar_url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

    def _make_hash(self, title: str, date: str, country: str) -> str:
        raw = f"ff:{title}:{date}:{country}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def fetch_weekly_calendar(self) -> list[dict]:
        """Fetch this week's calendar and return High impact events only."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.calendar_url,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.error("ForexFactory calendar error: HTTP %d", resp.status)
                        self.db.update_source_status("forexfactory", False)
                        return []

                    data = await resp.json()

        except Exception as e:
            logger.error("ForexFactory fetch failed: %s", e)
            self.db.update_source_status("forexfactory", False)
            return []

        # Filter for High impact only
        high_impact = [e for e in data if e.get("impact") == "High"]

        self.db.update_source_status("forexfactory", True)

        logger.info(
            "ForexFactory calendar: %d total events, %d High impact",
            len(data), len(high_impact),
        )

        return high_impact

    async def store_events(self) -> int:
        """Fetch calendar and store High impact events in database.
        Returns number of new events stored.
        """
        events = await self.fetch_weekly_calendar()
        new_count = 0

        for event in events:
            title = event.get("title", "")
            date_str = event.get("date", "")
            country = event.get("country", "")
            forecast = event.get("forecast", "")
            previous = event.get("previous", "")

            if not title or not date_str:
                continue

            event_hash = self._make_hash(title, date_str, country)

            # Check if already stored
            if self.db.is_ff_event_stored(event_hash):
                continue

            # Parse the date string (format: 2026-03-24T04:30:00-04:00)
            try:
                event_dt = datetime.fromisoformat(date_str)
                # Convert to London time for display
                event_london = event_dt.astimezone(LONDON_TZ)
                london_time_str = event_london.strftime("%H:%M")
                london_date_str = event_london.strftime("%d %B %Y")
            except (ValueError, TypeError):
                london_time_str = ""
                london_date_str = ""
                event_dt = None

            self.db.store_ff_event(
                event_hash=event_hash,
                title=title,
                country=country,
                event_time_utc=event_dt.astimezone(timezone.utc).isoformat() if event_dt else "",
                event_time_london=f"{london_time_str}, {london_date_str}",
                forecast=forecast,
                previous=previous,
            )
            new_count += 1

        logger.info("ForexFactory: stored %d new events", new_count)
        return new_count

    async def get_upcoming_reminders(self) -> list[dict]:
        """Check for events happening in the next 60-75 minutes
        that haven't had a reminder sent yet.
        """
        now_utc = datetime.now(timezone.utc)
        window_start = now_utc + timedelta(minutes=55)
        window_end = now_utc + timedelta(minutes=75)

        events = self.db.get_ff_events_needing_reminder(
            window_start.isoformat(),
            window_end.isoformat(),
        )

        return events

    def format_reminder(self, event: dict) -> str:
        """Format a calendar event as a reminder message."""
        title = event.get("title", "")
        country = event.get("country", "")
        forecast = event.get("forecast", "")
        previous = event.get("previous", "")
        london_time = event.get("event_time_london", "")

        region_name, flag = COUNTRY_MAP.get(country, (country, "🌍"))

        lines = [
            f"🔴 Forex Factory",
            "",
            f"{title} — {region_name} ({country})",
            f"Время: {london_time} London",
        ]

        if forecast or previous:
            parts = []
            if forecast:
                parts.append(f"Прогноз: {forecast}")
            if previous:
                parts.append(f"Предыдущее: {previous}")
            lines.append(" | ".join(parts))

        return "\n".join(lines)
