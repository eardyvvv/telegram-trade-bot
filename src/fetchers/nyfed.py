import hashlib
import logging

import aiohttp

from src.database import Database

logger = logging.getLogger("trading_bot")


class NYFedFetcher:
    """Fetches NY Fed SOMA (System Open Market Account) data — Fed balance sheet."""

    def __init__(self, db: Database):
        self.db = db
        self.soma_url = "https://markets.newyorkfed.org/api/soma/summary.json"

    def _make_hash(self, date: str, total: str) -> str:
        raw = f"nyfed:{date}:{total}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _format_trillions(self, value_str: str) -> str:
        """Convert raw number string to readable trillions."""
        try:
            val = float(value_str)
            return f"${val / 1_000_000_000_000:.3f}T"
        except (ValueError, TypeError):
            return value_str

    def _format_billions(self, value_str: str) -> str:
        """Convert raw number string to readable billions."""
        try:
            val = float(value_str)
            return f"${val / 1_000_000_000:.1f}B"
        except (ValueError, TypeError):
            return value_str

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest SOMA summary data."""
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.soma_url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.error("NY Fed SOMA error: HTTP %d", resp.status)
                        self.db.update_source_status("nyfed", False)
                        return []
                    data = await resp.json()

        except Exception as e:
            logger.error("NY Fed fetch failed: %s", e)
            self.db.update_source_status("nyfed", False)
            return []

        entries = data.get("soma", {}).get("summary", [])
        if not entries:
            self.db.update_source_status("nyfed", False)
            return []

        # Get latest and previous
        latest = entries[-1]
        previous = entries[-2] if len(entries) > 1 else None

        date = latest.get("asOfDate", "")
        total = latest.get("total", "0")

        item_hash = self._make_hash(date, total)

        if self.db.is_already_sent("nyfed", item_hash):
            self.db.update_source_status("nyfed", True)
            return []

        # Build detailed breakdown
        prev_total = previous.get("total", "0") if previous else None
        prev_date = previous.get("asOfDate", "") if previous else None

        new_items = [{
            "series_id": "SOMA",
            "name": "NY Fed — Баланс ФРС (SOMA)",
            "category": "Ставки/ЦБ",
            "date": date,
            "value": self._format_trillions(total),
            "previous_value": self._format_trillions(prev_total) if prev_total else None,
            "previous_date": prev_date,
            "item_hash": item_hash,
            "breakdown": {
                "treasuries_notes_bonds": self._format_trillions(latest.get("notesbonds", "0")),
                "treasuries_bills": self._format_billions(latest.get("bills", "0")),
                "mbs": self._format_trillions(latest.get("mbs", "0")),
                "tips": self._format_billions(latest.get("tips", "0")),
                "agencies": self._format_billions(latest.get("agencies", "0")),
            },
        }]

        # Apply limit
        if limit and limit < len(new_items):
            new_items = new_items[:limit]

        self.db.update_source_status("nyfed", True)
        logger.info("NY Fed SOMA check: %d new items", len(new_items))
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Новые данные NY Fed — Баланс Федеральной Резервной Системы (SOMA):\n"]

        for item in items:
            lines.append(
                f"- Общий портфель ФРС: {item['value']} (дата: {item['date']})"
            )
            if item.get("previous_value"):
                lines.append(
                    f"  Предыдущее: {item['previous_value']} ({item['previous_date']})"
                )

            breakdown = item.get("breakdown", {})
            if breakdown:
                lines.append(f"  Разбивка:")
                lines.append(f"    Казначейские облигации (Notes/Bonds): {breakdown.get('treasuries_notes_bonds', 'н/д')}")
                lines.append(f"    Казначейские векселя (Bills): {breakdown.get('treasuries_bills', 'н/д')}")
                lines.append(f"    Ипотечные бумаги (MBS): {breakdown.get('mbs', 'н/д')}")
                lines.append(f"    TIPS: {breakdown.get('tips', 'н/д')}")
                lines.append(f"    Агентские бумаги: {breakdown.get('agencies', 'н/д')}")

        return "\n".join(lines)
