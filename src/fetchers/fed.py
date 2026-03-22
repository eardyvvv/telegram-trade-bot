import hashlib
import logging
import re
from datetime import datetime, timezone
from xml.etree import ElementTree

import aiohttp

from src.database import Database

logger = logging.getLogger("trading_bot")

# Fed RSS feeds to monitor
# Each feed covers a different type of release
FED_FEEDS = [
    (
        "https://www.federalreserve.gov/feeds/press_monetary.xml",
        "Monetary Policy (FOMC)",
        "Ставки/ЦБ",
    ),
    (
        "https://www.federalreserve.gov/feeds/press_bcreg.xml",
        "Banking Regulation",
        "Гос.долг",
    ),
    (
        "https://www.federalreserve.gov/feeds/press_speech.xml",
        "Speeches",
        "Ставки/ЦБ",
    ),
    (
        "https://www.federalreserve.gov/feeds/press_testimony.xml",
        "Testimony",
        "Ставки/ЦБ",
    ),
]


class FedReserveFetcher:
    """Fetches Federal Reserve press releases, speeches, and FOMC statements via RSS."""

    def __init__(self, db: Database):
        self.db = db

    def _make_hash(self, url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()[:16]

    def _clean_text(self, text: str) -> str:
        """Remove CDATA wrappers and extra whitespace."""
        if not text:
            return ""
        text = text.strip()
        text = re.sub(r'\s+', ' ', text)
        return text

    async def _fetch_feed(self, feed_url: str) -> list[dict]:
        """Fetch and parse a single RSS feed."""
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    feed_url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.error("Fed RSS error for %s: HTTP %d", feed_url, resp.status)
                        return []
                    text = await resp.text()

        except Exception as e:
            logger.error("Fed RSS fetch failed for %s: %s", feed_url, e)
            return []

        items = []
        try:
            root = ElementTree.fromstring(text)
            for item in root.findall('.//item'):
                title = self._clean_text(item.findtext('title', ''))
                link = self._clean_text(item.findtext('link', ''))
                description = self._clean_text(item.findtext('description', ''))
                pub_date = self._clean_text(item.findtext('pubDate', ''))

                if not title or not link:
                    continue

                items.append({
                    "title": title,
                    "link": link,
                    "description": description[:500],
                    "pub_date": pub_date,
                })

        except ElementTree.ParseError as e:
            logger.error("Fed RSS parse error: %s", e)

        return items

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch all Fed RSS feeds and return new items."""
        feeds_to_check = FED_FEEDS[:limit] if limit else FED_FEEDS
        new_items = []

        for feed_url, feed_name, category in feeds_to_check:
            items = await self._fetch_feed(feed_url)

            for item in items[:5]:  # Only check 5 most recent per feed
                item_hash = self._make_hash(item["link"])

                if self.db.is_already_sent("fed", item_hash):
                    continue

                new_items.append({
                    "series_id": feed_name,
                    "name": f"ФРС — {feed_name}",
                    "category": category,
                    "date": item["pub_date"][:16] if item["pub_date"] else "",
                    "value": item["title"],
                    "previous_value": None,
                    "previous_date": None,
                    "description": item["description"],
                    "link": item["link"],
                    "item_hash": item_hash,
                })

        self.db.update_source_status("fed", True)

        logger.info(
            "Fed RSS check: %d new items (checked %d feeds)",
            len(new_items), len(feeds_to_check),
        )
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Новые публикации Федеральной Резервной Системы (ФРС США):\n"]

        for item in items:
            lines.append(
                f"- Тип: {item['series_id']}\n"
                f"  Заголовок: {item['value']}\n"
                f"  Описание: {item.get('description', 'нет')}\n"
                f"  Дата: {item['date']}"
            )

        return "\n".join(lines)
