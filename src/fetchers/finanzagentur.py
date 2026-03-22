import hashlib
import logging
import re

from playwright.async_api import async_playwright

from src.database import Database

logger = logging.getLogger("trading_bot")


class FinanzagenturFetcher:
    """Fetches German Federal bond auction results using Playwright."""

    def __init__(self, db: Database):
        self.db = db
        self.url = "https://www.deutsche-finanzagentur.de/en/federal-securities/auction-results"

    def _make_hash(self, date: str, issuance: str, yield_val: str) -> str:
        raw = f"dfa:{date}:{issuance}:{yield_val}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _clean_issuance(self, text: str) -> str:
        """Clean issuance text: 'Bund (R)\\n\\t\\t\\tDE000...' -> 'Bund (R) DE000...'"""
        return re.sub(r'\s+', ' ', text).strip()

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest German bond auction results."""
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

                # Extract table rows
                rows = await page.query_selector_all("tr")
                raw_rows = []
                for row in rows:
                    cells = await row.query_selector_all("td, th")
                    if cells:
                        vals = []
                        for cell in cells:
                            t = await cell.text_content()
                            vals.append(t.strip() if t else "")
                        raw_rows.append(vals)

                await browser.close()

        except Exception as e:
            logger.error("Finanzagentur fetch failed: %s", e)
            self.db.update_source_status("dfa", False)
            return []

        # Parse auction results (tables with Bid/Cover and Yield columns)
        results = []
        in_results = False

        for row in raw_rows:
            if len(row) >= 5:
                # Detect results header
                if row[0] == "Date" and "Bid/Cover" in row[2]:
                    in_results = True
                    continue
                # Detect upcoming header (stop parsing results)
                if row[0] == "Date" and "Volume" in row[2]:
                    in_results = False
                    continue

                if in_results and row[0] and re.match(r'\d{2}\.\d{2}\.\d{4}', row[0]):
                    results.append({
                        "date": row[0],
                        "issuance": self._clean_issuance(row[1]),
                        "bid_cover": row[2],
                        "yield": row[4],
                    })

        max_items = limit or 5
        results = results[:max_items]

        new_items = []
        for result in results:
            item_hash = self._make_hash(result["date"], result["issuance"], result["yield"])

            if self.db.is_already_sent("dfa", item_hash):
                continue

            # Extract security type from issuance
            sec_type = result["issuance"].split("(")[0].strip()
            isin = ""
            isin_match = re.search(r'(DE\w+)', result["issuance"])
            if isin_match:
                isin = isin_match.group(1)

            new_items.append({
                "series_id": f"DFA_{sec_type}",
                "name": f"Finanzagentur — Аукцион {sec_type}",
                "category": "Гос.долг",
                "date": result["date"],
                "value": f"{sec_type} {isin}: доходность {result['yield']}, bid/cover {result['bid_cover']}",
                "previous_value": None,
                "previous_date": None,
                "item_hash": item_hash,
                "security_type": sec_type,
                "isin": isin,
                "bid_cover": result["bid_cover"],
                "yield_pct": result["yield"],
            })

        self.db.update_source_status("dfa", True)
        logger.info("Finanzagentur check: %d new auction results", len(new_items))
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Результаты аукционов гособлигаций Германии (Deutsche Finanzagentur):\n"]

        for item in items:
            lines.append(
                f"- Дата: {item['date']}\n"
                f"  Тип: {item['security_type']} (ISIN: {item['isin']})\n"
                f"  Доходность: {item['yield_pct']}\n"
                f"  Bid-to-cover: {item['bid_cover']}"
            )

        return "\n".join(lines)
