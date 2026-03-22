import hashlib
import logging
import re

from playwright.async_api import async_playwright

from src.database import Database

logger = logging.getLogger("trading_bot")


class CNNFearGreedFetcher:
    """Fetches CNN Fear & Greed Index using Playwright (headless browser)."""

    def __init__(self, db: Database):
        self.db = db
        self.url = "https://edition.cnn.com/markets/fear-and-greed"

    def _make_hash(self, score: str, date: str) -> str:
        raw = f"cnn_fg:{score}:{date}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _classify_score(self, score: int) -> str:
        """Classify the Fear & Greed score."""
        if score <= 25:
            return "Extreme Fear (Крайний страх)"
        elif score <= 45:
            return "Fear (Страх)"
        elif score <= 55:
            return "Neutral (Нейтральный)"
        elif score <= 75:
            return "Greed (Жадность)"
        else:
            return "Extreme Greed (Крайняя жадность)"

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch current Fear & Greed Index score."""
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
                await page.goto(self.url, timeout=30000)
                await page.wait_for_timeout(5000)

                text = await page.inner_text("body")
                await browser.close()

        except Exception as e:
            logger.error("CNN Fear & Greed fetch failed: %s", e)
            self.db.update_source_status("cnn_fg", False)
            return []

        # Parse the score from page text
        # Looking for patterns like "15\nExtreme Fear" or just the number
        lines = [l.strip() for l in text.split("\n") if l.strip()]

        score = None
        previous_close = None
        week_ago = None

        for i, line in enumerate(lines):
            # The score appears as a standalone number near "Fear & Greed Index"
            if line == "Fear & Greed Index" and i + 1 < len(lines):
                try:
                    score = int(lines[i + 1])
                except ValueError:
                    pass

            # Previous close
            if line == "Previous close" and i + 1 < len(lines):
                try:
                    previous_close = int(lines[i + 1])
                except ValueError:
                    pass

            # 1 week ago
            if line == "1 week ago" and i + 1 < len(lines):
                try:
                    week_ago = int(lines[i + 1])
                except ValueError:
                    pass

        if score is None:
            # Try alternative: find any standalone number between 0-100
            # near fear/greed text
            for i, line in enumerate(lines):
                if "fear" in line.lower() or "greed" in line.lower():
                    # Check surrounding lines for a number
                    for j in range(max(0, i - 3), min(len(lines), i + 3)):
                        try:
                            num = int(lines[j])
                            if 0 <= num <= 100:
                                score = num
                                break
                        except ValueError:
                            continue
                    if score:
                        break

        if score is None:
            logger.warning("CNN Fear & Greed: could not find score in page")
            self.db.update_source_status("cnn_fg", False)
            return []

        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        item_hash = self._make_hash(str(score), today)

        if self.db.is_already_sent("cnn_fg", item_hash):
            self.db.update_source_status("cnn_fg", True)
            return []

        classification = self._classify_score(score)

        value_str = f"{score}/100 — {classification}"
        prev_str = None
        if previous_close is not None:
            prev_str = f"{previous_close}/100"

        new_items = [{
            "series_id": "FearGreed",
            "name": "CNN Fear & Greed Index",
            "category": "Потребитель",
            "date": today,
            "value": value_str,
            "previous_value": prev_str,
            "previous_date": "previous close",
            "item_hash": item_hash,
            "score": score,
            "previous_close": previous_close,
            "week_ago": week_ago,
        }]

        self.db.update_source_status("cnn_fg", True)
        logger.info("CNN Fear & Greed: score=%d, prev=%s, week_ago=%s",
                     score, previous_close, week_ago)
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Обновление CNN Fear & Greed Index (индекс страха и жадности):\n"]

        for item in items:
            lines.append(
                f"- Текущий индекс: {item['score']}/100 ({self._classify_score(item['score'])})\n"
                f"  Предыдущее закрытие: {item.get('previous_close', 'н/д')}\n"
                f"  Неделю назад: {item.get('week_ago', 'н/д')}\n"
                f"  Дата: {item['date']}"
            )

        return "\n".join(lines)
