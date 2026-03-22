import hashlib
import logging
import re

from playwright.async_api import async_playwright

from src.database import Database

logger = logging.getLogger("trading_bot")


class OPECFetcher:
    """Fetches OPEC press releases using Playwright."""

    def __init__(self, db: Database):
        self.db = db
        self.url = "https://www.opec.org/opec_web/en/press_room/28.htm"

    def _make_hash(self, title: str, date: str) -> str:
        raw = f"opec:{title}:{date}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest OPEC press releases."""
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
            logger.error("OPEC fetch failed: %s", e)
            self.db.update_source_status("opec", False)
            return []

        # Parse press releases from page text
        lines = [l.strip() for l in text.split("\n") if l.strip()]

        releases = []
        in_press = False
        current_title = None

        for line in lines:
            if "PRESS RELEASES" in line:
                in_press = True
                continue

            if "NEWS & ARTICLES" in line:
                break  # Stop at news section

            if not in_press:
                continue

            if line == "READ MORE":
                continue

            # Date pattern: "4 March 2026"
            date_match = re.match(
                r"^(\d{1,2}\s+(?:January|February|March|April|May|June|July|"
                r"August|September|October|November|December)\s+\d{4})$",
                line,
            )

            if date_match:
                if current_title:
                    releases.append({
                        "title": current_title,
                        "date": date_match.group(1),
                    })
                current_title = None
            elif len(line) > 20 and not line.startswith("READ"):
                current_title = line

        max_items = limit or 5
        releases = releases[:max_items]

        new_items = []
        for release in releases:
            item_hash = self._make_hash(release["title"], release["date"])

            if self.db.is_already_sent("opec", item_hash):
                continue

            new_items.append({
                "series_id": "OPEC_PR",
                "name": "OPEC — Пресс-релиз",
                "category": "Энергетика",
                "date": release["date"],
                "value": release["title"],
                "previous_value": None,
                "previous_date": None,
                "item_hash": item_hash,
            })

        self.db.update_source_status("opec", True)
        logger.info("OPEC check: %d new press releases", len(new_items))
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Новые пресс-релизы OPEC:\n"]

        for item in items:
            lines.append(
                f"- Заголовок: {item['value']}\n"
                f"  Дата: {item['date']}"
            )

        return "\n".join(lines)
