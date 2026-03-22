import hashlib
import logging
import re

from playwright.async_api import async_playwright

from src.database import Database

logger = logging.getLogger("trading_bot")


class IEAFetcher:
    """Fetches IEA (International Energy Agency) news using Playwright."""

    def __init__(self, db: Database):
        self.db = db
        self.url = "https://www.iea.org/news"

    def _make_hash(self, title: str, date: str) -> str:
        raw = f"iea:{title}:{date}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest IEA news headlines."""
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--disable-blink-features=AutomationControlled"],
                )
                context = await browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1920, "height": 1080},
                )

                page = await context.new_page()
                await page.goto(self.url, timeout=20000)
                await page.wait_for_timeout(5000)

                text = await page.inner_text("body")
                await browser.close()

        except Exception as e:
            logger.error("IEA fetch failed: %s", e)
            self.db.update_source_status("iea_news", False)
            return []

        lines = [l.strip() for l in text.split("\n") if l.strip()]

        # Parse news: date line followed by category, then title
        date_pattern = re.compile(
            r"^(\d{1,2}\s+(?:January|February|March|April|May|June|July|"
            r"August|September|October|November|December)\s+\d{4})$"
        )

        articles = []
        i = 0
        while i < len(lines):
            date_match = date_pattern.match(lines[i])
            if date_match:
                date = date_match.group(1)
                # Next non-date, non-empty line is the title or category
                # Look ahead for the actual title
                title = None
                for j in range(i + 1, min(i + 4, len(lines))):
                    candidate = lines[j]
                    # Skip category labels and navigation
                    if candidate in ("News", "Commentary", "Press release", "Prev", "Next"):
                        continue
                    if date_pattern.match(candidate):
                        break
                    if len(candidate) > 15:
                        title = candidate
                        break

                if title:
                    articles.append({"date": date, "title": title})

            i += 1

        max_items = limit or 5
        articles = articles[:max_items]

        new_items = []
        for article in articles:
            item_hash = self._make_hash(article["title"], article["date"])

            if self.db.is_already_sent("iea_news", item_hash):
                continue

            new_items.append({
                "series_id": "IEA_NEWS",
                "name": "IEA — Новость",
                "category": "Энергетика",
                "date": article["date"],
                "value": article["title"],
                "previous_value": None,
                "previous_date": None,
                "item_hash": item_hash,
            })

        self.db.update_source_status("iea_news", True)
        logger.info("IEA check: %d new articles", len(new_items))
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Новые публикации IEA (Международное энергетическое агентство):\n"]

        for item in items:
            lines.append(
                f"- Заголовок: {item['value']}\n"
                f"  Дата: {item['date']}"
            )

        return "\n".join(lines)
