import hashlib
import logging
import re

from playwright.async_api import async_playwright

from src.database import Database

logger = logging.getLogger("trading_bot")

# ISM report URLs — manufacturing and services
ISM_REPORTS = {
    "manufacturing": {
        "base_url": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/",
        "name": "Manufacturing PMI",
    },
    "services": {
        "base_url": "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-pmi-reports/services/",
        "name": "Services PMI",
    },
}

MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]


class ISMFetcher:
    """Fetches ISM Manufacturing and Services PMI data using Playwright."""

    def __init__(self, db: Database):
        self.db = db

    def _make_hash(self, report_type: str, month: str, pmi_value: str) -> str:
        raw = f"ism:{report_type}:{month}:{pmi_value}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _find_latest_month_url(self, base_url: str) -> list[str]:
        """Generate URLs for last 3 months to try."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        urls = []
        for offset in range(1, 4):
            month_idx = (now.month - offset) % 12
            if month_idx == 0:
                month_idx = 12
            month_name = MONTHS[month_idx - 1]
            urls.append(f"{base_url}{month_name}/")
        return urls

    async def _fetch_report(self, report_type: str, config: dict) -> dict | None:
        """Fetch a single ISM PMI report."""
        urls = self._find_latest_month_url(config["base_url"])

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

                for url in urls:
                    page = await context.new_page()
                    try:
                        resp = await page.goto(url, timeout=15000)
                        if not resp or resp.status != 200:
                            await page.close()
                            continue

                        await page.wait_for_timeout(3000)
                        text = await page.inner_text("body")
                        await page.close()

                        # Extract PMI value
                        pmi_match = re.search(
                            r'(?:Manufacturing|Services)\s+PMI[®™]*\s+at\s+([\d.]+)%',
                            text,
                        )
                        if not pmi_match:
                            pmi_match = re.search(
                                r'registered\s+([\d.]+)\s+percent',
                                text,
                            )

                        if pmi_match:
                            pmi_value = pmi_match.group(1)

                            # Extract month from URL
                            month = url.rstrip("/").split("/")[-1].capitalize()

                            # Extract key details
                            details = []
                            lines = text.split("\n")
                            for line in lines[:30]:
                                line = line.strip()
                                if len(line) > 30 and any(w in line.lower() for w in [
                                    "registered", "expanded", "contracted",
                                    "new orders", "production", "employment",
                                ]):
                                    details.append(line[:200])
                                    if len(details) >= 3:
                                        break

                            await browser.close()
                            return {
                                "pmi_value": pmi_value,
                                "month": month,
                                "report_type": report_type,
                                "details": " ".join(details)[:500],
                            }

                    except Exception:
                        await page.close()
                        continue

                await browser.close()

        except Exception as e:
            logger.error("ISM fetch failed for %s: %s", report_type, e)

        return None

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest ISM PMI reports."""
        reports = list(ISM_REPORTS.items())
        if limit:
            reports = reports[:limit]

        new_items = []

        for report_type, config in reports:
            result = await self._fetch_report(report_type, config)
            if not result:
                continue

            item_hash = self._make_hash(
                report_type, result["month"], result["pmi_value"]
            )

            if self.db.is_already_sent("ism", item_hash):
                continue

            pmi = float(result["pmi_value"])
            status = "расширение" if pmi > 50 else "сокращение"

            new_items.append({
                "series_id": f"ISM_{report_type}",
                "name": f"ISM — {config['name']}",
                "category": "Промышленность",
                "date": result["month"],
                "value": f"{result['pmi_value']}% ({status})",
                "previous_value": None,
                "previous_date": None,
                "item_hash": item_hash,
                "pmi_value": result["pmi_value"],
                "details": result["details"],
            })

        self.db.update_source_status("ism", True)
        logger.info("ISM check: %d new PMI reports", len(new_items))
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Обновление ISM PMI (Институт управления поставками, США):\n"]

        for item in items:
            lines.append(
                f"- {item['name']}: {item['value']}\n"
                f"  Месяц: {item['date']}\n"
                f"  Детали: {item.get('details', 'нет')}"
            )

        return "\n".join(lines)
