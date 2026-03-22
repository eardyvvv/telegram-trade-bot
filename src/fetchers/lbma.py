import hashlib
import logging
from datetime import datetime, timezone

import aiohttp

from src.database import Database

logger = logging.getLogger("trading_bot")


class LBMAFetcher:
    """Fetches LBMA London vault holdings data (gold and silver stocks)."""

    def __init__(self, db: Database):
        self.db = db
        self.vault_url = "https://www.lbma.org.uk/vault-holdings-data/data.json"

    def _make_hash(self, timestamp: int, gold: int, silver: int) -> str:
        raw = f"lbma:{timestamp}:{gold}:{silver}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _ts_to_date(self, ts_ms: int) -> str:
        """Convert millisecond timestamp to readable date."""
        try:
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d")
        except (ValueError, OSError):
            return str(ts_ms)

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest LBMA vault holdings."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.vault_url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.error("LBMA error: HTTP %d", resp.status)
                        self.db.update_source_status("lbma", False)
                        return []
                    data = await resp.json()

        except Exception as e:
            logger.error("LBMA fetch failed: %s", e)
            self.db.update_source_status("lbma", False)
            return []

        if not data or len(data) < 2:
            self.db.update_source_status("lbma", False)
            return []

        # Data format: [timestamp_ms, gold_thousands_oz, silver_thousands_oz]
        latest = data[-1]
        previous = data[-2]

        ts = latest[0]
        gold = latest[1]
        silver = latest[2]

        item_hash = self._make_hash(ts, gold, silver)

        if self.db.is_already_sent("lbma", item_hash):
            self.db.update_source_status("lbma", True)
            return []

        prev_gold = previous[1]
        prev_silver = previous[2]
        prev_date = self._ts_to_date(previous[0])

        gold_change = gold - prev_gold
        silver_change = silver - prev_silver

        new_items = [{
            "series_id": "LBMA_VAULT",
            "name": "LBMA — Запасы металлов в Лондоне",
            "category": "Металлы",
            "date": self._ts_to_date(ts),
            "value": f"Золото: {gold:,} тыс. унций, Серебро: {silver:,} тыс. унций",
            "previous_value": f"Золото: {prev_gold:,} тыс. унций, Серебро: {prev_silver:,} тыс. унций",
            "previous_date": prev_date,
            "item_hash": item_hash,
            "gold": gold,
            "silver": silver,
            "gold_change": gold_change,
            "silver_change": silver_change,
        }]

        if limit and limit < len(new_items):
            new_items = new_items[:limit]

        self.db.update_source_status("lbma", True)
        logger.info("LBMA check: %d new items", len(new_items))
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Обновление запасов металлов в лондонских хранилищах (LBMA):\n"]

        for item in items:
            gold_dir = "+" if item["gold_change"] > 0 else ""
            silver_dir = "+" if item["silver_change"] > 0 else ""

            lines.append(
                f"- Дата: {item['date']}\n"
                f"  Золото: {item['gold']:,} тыс. унций ({gold_dir}{item['gold_change']:,} к пред. месяцу)\n"
                f"  Серебро: {item['silver']:,} тыс. унций ({silver_dir}{item['silver_change']:,} к пред. месяцу)\n"
                f"  Предыдущий период ({item['previous_date']}): "
                f"Золото {item['gold'] - item['gold_change']:,}, Серебро {item['silver'] - item['silver_change']:,}"
            )

        return "\n".join(lines)
