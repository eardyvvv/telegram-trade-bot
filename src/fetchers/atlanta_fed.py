import hashlib
import logging
import re

import aiohttp
from bs4 import BeautifulSoup

from src.database import Database

logger = logging.getLogger("trading_bot")


class AtlantaFedFetcher:
    """Fetches Atlanta Fed GDPNow estimate by scraping the archives page."""

    def __init__(self, db: Database):
        self.db = db
        self.url = "https://www.atlantafed.org/cqer/research/gdpnow/archives"

    def _make_hash(self, date: str, value: str) -> str:
        raw = f"gdpnow:{date}:{value}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest GDPNow estimate from the archives page."""
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.error("Atlanta Fed error: HTTP %d", resp.status)
                        self.db.update_source_status("atlanta", False)
                        return []

                    html = await resp.text()

        except Exception as e:
            logger.error("Atlanta Fed fetch failed: %s", e)
            self.db.update_source_status("atlanta", False)
            return []

        soup = BeautifulSoup(html, "html.parser")

        # Find paragraphs containing GDPNow estimates
        # Pattern: "is X.X percent on Month Day"
        pattern = re.compile(
            r"is\s+(?:<strong>)?([-\d.]+)\s+percent(?:</strong>)?\s+on\s+(\w+\s+\d+)",
            re.IGNORECASE,
        )

        # Also capture "down from X.X percent" or "up from X.X percent"
        prev_pattern = re.compile(
            r"(?:down|up|unchanged)\s+(?:from\s+)?([\d.]+)\s+percent",
            re.IGNORECASE,
        )

        new_items = []
        max_items = limit or 1  # Default: just the latest estimate

        for p_tag in soup.find_all("p"):
            if len(new_items) >= max_items:
                break

            text = str(p_tag)
            match = pattern.search(text)
            if not match:
                continue

            value = match.group(1)
            date_str = match.group(2)

            # Try to find previous value
            prev_match = prev_pattern.search(text)
            prev_value = prev_match.group(1) if prev_match else None

            item_hash = self._make_hash(date_str, value)

            if self.db.is_already_sent("atlanta", item_hash):
                continue

            # Extract clean text for context
            clean_text = p_tag.get_text(strip=True)

            new_items.append({
                "series_id": "GDPNow",
                "name": "Atlanta Fed GDPNow — прогноз ВВП США",
                "category": "gdp",
                "date": date_str,
                "value": f"{value}%",
                "previous_value": f"{prev_value}%" if prev_value else None,
                "previous_date": None,
                "context": clean_text[:300],
                "item_hash": item_hash,
            })

        self.db.update_source_status("atlanta", True)

        logger.info(
            "Atlanta Fed check: %d new estimates found",
            len(new_items),
        )
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Новые данные Atlanta Fed GDPNow (прогноз ВВП США в реальном времени):\n"]

        for item in items:
            line = f"- GDPNow прогноз: {item['value']} (дата: {item['date']})"
            if item.get("previous_value"):
                line += f" | предыдущий прогноз: {item['previous_value']}"
            if item.get("context"):
                line += f"\n  Контекст: {item['context']}"
            lines.append(line)

        return "\n".join(lines)
