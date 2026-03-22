import hashlib
import logging
from datetime import datetime, timezone

import aiohttp

from src.config import Config
from src.database import Database

logger = logging.getLogger("trading_bot")

# Key economic indicators to track on FRED
# Each entry: (series_id, human-readable name, category)
FRED_SERIES = [
    # GDP & Growth
    ("GDP", "ВВП США (квартальный)", "gdp"),
    ("GDPC1", "Реальный ВВП США", "gdp"),
    # Inflation
    ("CPIAUCSL", "CPI (индекс потребительских цен)", "inflation"),
    ("CPILFESL", "Core CPI (без еды и энергии)", "inflation"),
    ("PCEPI", "PCE (расходы на личное потребление)", "inflation"),
    ("PCEPILFE", "Core PCE", "inflation"),
    # Labor Market
    ("UNRATE", "Уровень безработицы", "labor"),
    ("PAYEMS", "Nonfarm Payrolls (занятость вне с/х)", "labor"),
    ("ICSA", "Первичные заявки на пособие по безработице", "labor"),
    # Interest Rates & Fed
    ("FEDFUNDS", "Ставка по федеральным фондам", "rates"),
    ("DGS10", "Доходность 10-летних US Treasuries", "rates"),
    ("DGS2", "Доходность 2-летних US Treasuries", "rates"),
    ("T10Y2Y", "Спред 10Y-2Y (инверсия кривой)", "rates"),
    # Money Supply
    ("M2SL", "Денежная масса M2", "money"),
    # Consumer & Business
    ("RSAFS", "Розничные продажи (Retail Sales)", "consumer"),
    ("UMCSENT", "Индекс потребительского доверия (UMich)", "consumer"),
    # Housing
    ("HOUST", "Начало строительства жилья", "housing"),
    # Trade
    ("BOPGSTB", "Торговый баланс США", "trade"),
]


class FREDFetcher:
    """Fetches latest economic data from FRED API."""

    def __init__(self, db: Database):
        self.db = db
        self.base_url = "https://api.stlouisfed.org/fred"

    def _make_hash(self, series_id: str, date: str, value: str) -> str:
        """Create a unique fingerprint for a data point."""
        raw = f"{series_id}:{date}:{value}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def fetch_series(
        self, series_id: str, name: str
    ) -> dict | None:
        """Fetch the latest observation for a FRED series.

        Returns dict with: series_id, name, date, value, previous_value, previous_date
        Returns None if the request fails.
        """
        url = f"{self.base_url}/series/observations"
        params = {
            "series_id": series_id,
            "api_key": Config.FRED_API_KEY,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 2,  # Get latest + previous for comparison
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        logger.error(
                            "FRED API error for %s: HTTP %d", series_id, resp.status
                        )
                        return None

                    data = await resp.json()

            observations = data.get("observations", [])
            if not observations:
                logger.warning("No observations for %s", series_id)
                return None

            latest = observations[0]
            previous = observations[1] if len(observations) > 1 else None

            # FRED uses "." for missing values
            if latest["value"] == ".":
                return None

            return {
                "series_id": series_id,
                "name": name,
                "date": latest["date"],
                "value": latest["value"],
                "previous_value": previous["value"] if previous and previous["value"] != "." else None,
                "previous_date": previous["date"] if previous else None,
            }

        except Exception as e:
            logger.error("FRED fetch failed for %s: %s", series_id, e)
            return None

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch all tracked series, return only NEW data points.

        Args:
            limit: Max number of series to check (for testing). None = all.
        """
        new_items = []

        series_to_check = FRED_SERIES[:limit] if limit else FRED_SERIES
        for series_id, name, category in series_to_check:
            result = await self.fetch_series(series_id, name)
            if result is None:
                continue

            # Check if we already sent this exact data point
            item_hash = self._make_hash(
                series_id, result["date"], result["value"]
            )

            if self.db.is_already_sent("fred", item_hash):
                continue  # Already sent, skip

            result["item_hash"] = item_hash
            result["category"] = category
            new_items.append(result)

        # Update source status
        success = True  # If we got here without crashing, FRED API is working
        fail_count = self.db.update_source_status("fred", success)

        logger.info(
            "FRED check complete: %d new data points found", len(new_items)
        )
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        """Format FRED data items into a text block for AI analysis."""
        if not items:
            return ""

        lines = ["Новые экономические данные от FRED (Federal Reserve Economic Data):\n"]

        for item in items:
            line = f"- {item['name']} ({item['series_id']}): {item['value']} (дата: {item['date']})"
            if item.get("previous_value"):
                line += f" | предыдущее: {item['previous_value']} ({item['previous_date']})"
            lines.append(line)

        return "\n".join(lines)
