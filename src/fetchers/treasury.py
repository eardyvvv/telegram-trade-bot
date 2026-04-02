import hashlib
import logging

import aiohttp

from src.database import Database

logger = logging.getLogger("trading_bot")


class TreasuryDirectFetcher:
    """Fetches US Treasury auction results from the Fiscal Data API."""

    def __init__(self, db: Database):
        self.db = db
        self.api_url = (
            "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
            "/v1/accounting/od/auctions_query"
        )

    def _make_hash(self, cusip: str, auction_date: str) -> str:
        raw = f"treasury:{cusip}:{auction_date}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch recent Treasury auction results."""
        fetch_count = limit or 10

        params = {
            "sort": "-auction_date",
            "page[size]": str(fetch_count),
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.api_url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.warning("TreasuryDirect API error: HTTP %d", resp.status)
                        self.db.update_source_status("treasury", False)
                        return []

                    data = await resp.json()

        except Exception as e:
            logger.warning("TreasuryDirect fetch failed: %s", e)
            self.db.update_source_status("treasury", False)
            return []

        rows = data.get("data", [])
        new_items = []

        for row in rows:
            cusip = row.get("cusip", "")
            auction_date = row.get("auction_date", "")
            security_type = row.get("security_type", "")
            security_term = row.get("security_term", "")
            high_yield = row.get("high_yield", "")
            bid_cover = row.get("bid_to_cover_ratio", "")
            interest_rate = row.get("interest_rate", "")

            if not cusip or not auction_date:
                continue

            # Skip auctions that haven't happened yet (no yield data)
            if not high_yield or high_yield == "null":
                continue

            item_hash = self._make_hash(cusip, auction_date)

            if self.db.is_already_sent("treasury", item_hash):
                continue

            new_items.append({
                "series_id": cusip,
                "name": f"Аукцион US Treasury — {security_type} {security_term}",
                "category": "bonds",
                "date": auction_date,
                "value": f"Доходность: {high_yield}%",
                "security_type": security_type,
                "security_term": security_term,
                "high_yield": high_yield,
                "bid_to_cover": bid_cover,
                "interest_rate": interest_rate,
                "item_hash": item_hash,
                "previous_value": None,
                "previous_date": None,
            })

        self.db.update_source_status("treasury", True)

        logger.info(
            "TreasuryDirect check: %d new auctions (fetched %d)",
            len(new_items), len(rows),
        )
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Результаты аукционов Казначейства США (TreasuryDirect):\n"]

        for item in items:
            lines.append(
                f"- {item['security_type']} {item['security_term']} "
                f"(CUSIP: {item['series_id']}, дата аукциона: {item['date']}):\n"
                f"  Доходность (High Yield): {item['high_yield']}%\n"
                f"  Bid-to-Cover Ratio: {item['bid_to_cover']}\n"
                f"  Купонная ставка: {item['interest_rate']}%"
            )

        return "\n".join(lines)
