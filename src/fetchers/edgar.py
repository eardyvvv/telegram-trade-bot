import hashlib
import logging
from datetime import datetime, timezone, timedelta

import aiohttp

from src.database import Database

logger = logging.getLogger("trading_bot")

# SEC filing types to monitor
# 10-K = annual report, 10-Q = quarterly, 8-K = current events
# We focus on 8-K (material events) and major filings from large companies
TRACKED_FORMS = ["8-K", "10-K", "10-Q"]

# Major companies to track (CIK numbers for the biggest ones)
# These are the S&P 500 heavyweights that move markets
TRACKED_COMPANIES = {
    "0000320193": "Apple",
    "0000789019": "Microsoft",
    "0001652044": "Alphabet (Google)",
    "0001018724": "Amazon",
    "0001045810": "NVIDIA",
    "0001326801": "Meta (Facebook)",
    "0001318605": "Tesla",
    "0000078003": "Pfizer",
    "0000021344": "Coca-Cola",
    "0000200406": "Johnson & Johnson",
    "0000051143": "IBM",
    "0000093410": "Chevron",
    "0000034088": "Exxon Mobil",
    "0000070858": "Bank of America",
    "0000019617": "JPMorgan Chase",
    "0000831001": "Citigroup",
    "0000072971": "Wells Fargo",
    "0000004962": "American Express",
    "0001403161": "Visa",
    "0001141391": "Mastercard",
}


class EDGARFetcher:
    """Fetches SEC/EDGAR filings for major companies."""

    def __init__(self, db: Database):
        self.db = db
        self.search_url = "https://efts.sec.gov/LATEST/search-index"
        self.filings_url = "https://data.sec.gov/submissions/CIK{cik}.json"

    def _make_hash(self, accession: str) -> str:
        return hashlib.sha256(accession.encode()).hexdigest()[:16]

    async def _fetch_recent_filings(self, cik: str, company: str) -> list[dict]:
        """Fetch recent filings for a specific company."""
        url = self.filings_url.format(cik=cik)
        headers = {
            "User-Agent": "TradingNewsBot/1.0 (contact@tradingbot.com)",
            "Accept-Encoding": "gzip, deflate",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.error("EDGAR error for %s: HTTP %d", company, resp.status)
                        return []
                    data = await resp.json()

        except Exception as e:
            logger.error("EDGAR fetch failed for %s: %s", company, e)
            return []

        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accessions = recent.get("accessionNumber", [])
        descriptions = recent.get("primaryDocDescription", [])

        results = []
        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

        for i in range(min(len(forms), 20)):
            form = forms[i] if i < len(forms) else ""
            date = dates[i] if i < len(dates) else ""
            accession = accessions[i] if i < len(accessions) else ""
            desc = descriptions[i] if i < len(descriptions) else ""

            # Only track our target form types
            if form not in TRACKED_FORMS:
                continue

            # Only recent filings (last 7 days)
            if date < cutoff:
                continue

            results.append({
                "company": company,
                "cik": cik,
                "form": form,
                "date": date,
                "accession": accession,
                "description": desc or form,
            })

        return results

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch recent SEC filings for tracked companies."""
        companies = list(TRACKED_COMPANIES.items())
        if limit:
            companies = companies[:limit]

        new_items = []

        for cik, company in companies:
            filings = await self._fetch_recent_filings(cik, company)

            for filing in filings:
                item_hash = self._make_hash(filing["accession"])

                if self.db.is_already_sent("edgar", item_hash):
                    continue

                new_items.append({
                    "series_id": f"{company}/{filing['form']}",
                    "name": f"SEC Filing — {company}",
                    "category": "Гос.долг",
                    "date": filing["date"],
                    "value": f"{filing['form']}: {filing['description']}",
                    "previous_value": None,
                    "previous_date": None,
                    "company": company,
                    "form": filing["form"],
                    "item_hash": item_hash,
                })

        self.db.update_source_status("edgar", True)

        logger.info(
            "EDGAR check: %d new filings (checked %d companies)",
            len(new_items), len(companies),
        )
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Новые SEC-отчётности (EDGAR, Комиссия по ценным бумагам США):\n"]

        for item in items:
            form = item.get("form", "")
            form_desc = {
                "8-K": "текущий отчёт (материальные события)",
                "10-K": "годовой отчёт",
                "10-Q": "квартальный отчёт",
            }.get(form, form)

            lines.append(
                f"- {item['company']} — {form_desc}\n"
                f"  Тип формы: {form}\n"
                f"  Описание: {item['value']}\n"
                f"  Дата подачи: {item['date']}"
            )

        return "\n".join(lines)
