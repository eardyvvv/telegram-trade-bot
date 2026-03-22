import hashlib
import logging
import re

from playwright.async_api import async_playwright

from src.database import Database

logger = logging.getLogger("trading_bot")


class ABSFetcher:
    """Fetches Australian Bureau of Statistics GDP data using Playwright."""

    def __init__(self, db: Database):
        self.db = db
        self.gdp_url = (
            "https://www.abs.gov.au/statistics/economy/national-accounts/"
            "australian-national-accounts-national-income-expenditure-and-product/"
            "latest-release"
        )

    def _make_hash(self, content: str) -> str:
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest ABS GDP data."""
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
                await page.goto(self.gdp_url, timeout=20000)
                await page.wait_for_timeout(5000)

                text = await page.inner_text("body")
                await browser.close()

        except Exception as e:
            logger.error("ABS fetch failed: %s", e)
            self.db.update_source_status("abs", False)
            return []

        lines = [l.strip() for l in text.split("\n") if l.strip()]

        # Extract key figures
        key_figures = {}
        quarter = None

        for line in lines:
            # Find quarter reference
            q_match = re.search(
                r'((?:March|June|September|December)\s+\d{4})', line, re.IGNORECASE
            )
            if q_match and not quarter:
                quarter = q_match.group(1)

            # GDP growth
            gdp_match = re.match(
                r'The Australian economy (?:rose|grew|fell|contracted)\s+([\d.]+)%', line
            )
            if gdp_match:
                key_figures["gdp_growth"] = gdp_match.group(1)

            # Nominal GDP
            nom_match = re.match(r'In nominal terms.*?(\d+\.?\d*)%', line)
            if nom_match:
                key_figures["nominal_gdp"] = nom_match.group(1)

            # Terms of trade
            tot_match = re.match(r'The terms of trade.*?(\d+\.?\d*)%', line)
            if tot_match:
                key_figures["terms_of_trade"] = tot_match.group(1)

            # Saving ratio
            sav_match = re.match(r'Household saving.*?(\d+\.?\d*)%.*?(\d+\.?\d*)%', line)
            if sav_match:
                key_figures["saving_ratio"] = sav_match.group(1)
                key_figures["saving_ratio_prev"] = sav_match.group(2)

            # GDP table row
            if line.startswith("GDP") and "\t" in line:
                parts = re.findall(r'[\d.]+', line)
                if parts:
                    key_figures["gdp_quarters"] = parts

        if not key_figures:
            logger.warning("ABS: could not extract GDP data from page")
            self.db.update_source_status("abs", False)
            return []

        # Build hash from all values
        hash_content = f"{quarter}:{key_figures.get('gdp_growth', '')}"
        item_hash = self._make_hash(hash_content)

        if self.db.is_already_sent("abs", item_hash):
            self.db.update_source_status("abs", True)
            return []

        gdp_val = key_figures.get("gdp_growth", "?")
        value_str = f"ВВП Австралии: {gdp_val}% кв/кв"

        new_items = [{
            "series_id": "ABS_GDP",
            "name": "ABS — ВВП Австралии",
            "category": "ВВП/Рост",
            "date": quarter or "latest",
            "value": value_str,
            "previous_value": None,
            "previous_date": None,
            "item_hash": item_hash,
            "key_figures": key_figures,
        }]

        self.db.update_source_status("abs", True)
        logger.info("ABS check: %d new items", len(new_items))
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Обновление данных ABS (Бюро статистики Австралии):\n"]

        for item in items:
            kf = item.get("key_figures", {})
            lines.append(f"- Период: {item['date']}")
            lines.append(f"  ВВП (реальный, кв/кв): {kf.get('gdp_growth', 'н/д')}%")
            if kf.get("nominal_gdp"):
                lines.append(f"  ВВП (номинальный): {kf['nominal_gdp']}%")
            if kf.get("terms_of_trade"):
                lines.append(f"  Условия торговли: {kf['terms_of_trade']}%")
            if kf.get("saving_ratio"):
                lines.append(
                    f"  Норма сбережений: {kf['saving_ratio']}% "
                    f"(пред.: {kf.get('saving_ratio_prev', 'н/д')}%)"
                )

        return "\n".join(lines)
