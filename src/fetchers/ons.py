import hashlib
import logging

import aiohttp

from src.database import Database

logger = logging.getLogger("trading_bot")

# ONS time series — direct JSON endpoints (no API key needed)
# Format: https://www.ons.gov.uk/...timeseries/{CDID}/{DATASET}/data
ONS_SERIES = {
    "cpi_yoy": {
        "url": "https://www.ons.gov.uk/economy/inflationandpriceindices/timeseries/d7g7/mm23/data",
        "name": "CPI (г/г)",
        "category": "Инфляция",
        "data_key": "months",
    },
    "unemployment": {
        "url": "https://www.ons.gov.uk/employmentandlabourmarket/peoplenotinwork/unemployment/timeseries/mgsx/lms/data",
        "name": "Безработица",
        "category": "Рынок труда",
        "data_key": "months",
    },
    "gdp_monthly": {
        "url": "https://www.ons.gov.uk/economy/grossdomesticproductgdp/timeseries/ecyy/mgdp/data",
        "name": "ВВП (м/м)",
        "category": "ВВП/Рост",
        "data_key": "months",
    },
}


class ONSFetcher:
    """Fetches UK ONS economic data from their JSON time series endpoints."""

    def __init__(self, db: Database):
        self.db = db

    def _make_hash(self, series_id: str, date: str, value: str) -> str:
        raw = f"ons:{series_id}:{date}:{value}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def _fetch_series(self, series_id: str, config: dict) -> dict | None:
        """Fetch a single ONS time series."""
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    config["url"],
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.error("ONS error for %s: HTTP %d", series_id, resp.status)
                        return None
                    import json
                    data = json.loads(await resp.text())

        except Exception as e:
            logger.error("ONS fetch failed for %s: %s", series_id, e)
            return None

        entries = data.get(config["data_key"], [])
        if not entries:
            return None

        # Get latest and previous non-empty values
        latest = None
        previous = None
        for entry in reversed(entries):
            val = entry.get("value", "").strip()
            if val:
                if latest is None:
                    latest = entry
                elif previous is None:
                    previous = entry
                    break

        if not latest:
            return None

        return {
            "latest": latest,
            "previous": previous,
        }

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest ONS data for all tracked series."""
        series_to_check = list(ONS_SERIES.items())
        if limit:
            series_to_check = series_to_check[:limit]

        new_items = []

        for series_id, config in series_to_check:
            result = await self._fetch_series(series_id, config)
            if not result:
                continue

            latest = result["latest"]
            previous = result["previous"]

            date = latest.get("date", "?")
            value = latest.get("value", "?")

            item_hash = self._make_hash(series_id, date, value)

            if self.db.is_already_sent("ons", item_hash):
                continue

            prev_value = previous.get("value", "?") if previous else None
            prev_date = previous.get("date", "?") if previous else None

            new_items.append({
                "series_id": series_id,
                "name": f"ONS — {config['name']}",
                "category": config["category"],
                "date": date,
                "value": f"{value}%",
                "previous_value": f"{prev_value}%" if prev_value else None,
                "previous_date": prev_date,
                "item_hash": item_hash,
            })

        self.db.update_source_status("ons", True)
        logger.info("ONS check: %d new items", len(new_items))
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Обновление данных ONS (Национальная статистика Великобритании):\n"]

        for item in items:
            lines.append(
                f"- {item['name']}: {item['value']} (период: {item['date']})"
            )
            if item.get("previous_value"):
                lines.append(
                    f"  Предыдущее: {item['previous_value']} ({item['previous_date']})"
                )

        return "\n".join(lines)
