import hashlib
import logging
from datetime import datetime, timezone

import aiohttp

from src.config import Config
from src.database import Database

logger = logging.getLogger("trading_bot")

# EIA API v2 series to track
# Format: (route, facets_dict, frequency, human_name, category)
# facets_dict maps facet_name -> facet_value for filtering
# Discovered via API metadata exploration
EIA_SERIES = [
    # Crude Oil — spot prices (daily)
    (
        "petroleum/pri/spt/data",
        {"product": "EPCWTI"},
        "daily",
        "Нефть WTI — спотовая цена ($/барр.)",
        "oil",
    ),
    (
        "petroleum/pri/spt/data",
        {"product": "EPCBRENT"},
        "daily",
        "Нефть Brent — спотовая цена ($/барр.)",
        "oil",
    ),
    # US Crude Oil Stocks — weekly
    (
        "petroleum/stoc/wstk/data",
        {"product": "EPC0"},
        "weekly",
        "Коммерческие запасы нефти в США (тыс. барр.)",
        "oil",
    ),
    # US Crude Oil Production — monthly
    (
        "petroleum/crd/crpdn/data",
        {"duoarea": "NUS", "product": "EPC0"},
        "monthly",
        "Добыча нефти в США (тыс. барр./день)",
        "oil",
    ),
    # Natural Gas — Henry Hub futures (daily)
    (
        "natural-gas/pri/fut/data",
        {"series": "RNGC1"},
        "daily",
        "Природный газ Henry Hub — фьючерс ($/млн BTU)",
        "gas",
    ),
    # Gasoline — US regular retail price (weekly)
    (
        "petroleum/pri/gnd/data",
        {"series": "EMM_EPMR_PTE_NUS_DPG"},
        "weekly",
        "Розничная цена бензина в США ($/галлон)",
        "fuel",
    ),
    # STEO forecasts — OPEC production (monthly)
    (
        "steo/data",
        {"seriesId": "PAPR_OPEC"},
        "monthly",
        "Добыча нефти OPEC — прогноз EIA (млн барр./день)",
        "opec",
    ),
    # STEO forecasts — Brent price forecast (monthly)
    (
        "steo/data",
        {"seriesId": "BREPUUS"},
        "monthly",
        "Прогноз цены Brent ($/барр.) — EIA STEO",
        "oil",
    ),
]


class EIAFetcher:
    """Fetches energy data from EIA API v2."""

    def __init__(self, db: Database):
        self.db = db
        self.base_url = "https://api.eia.gov/v2"

    def _make_hash(self, route: str, period: str, value: str) -> str:
        raw = f"eia:{route}:{period}:{value}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def _fetch_series(
        self,
        route: str,
        facets: dict[str, str],
        frequency: str,
    ) -> list[dict]:
        """Fetch data from a specific EIA API v2 endpoint."""
        url = f"{self.base_url}/{route}"

        params = {
            "api_key": Config.EIA_API_KEY,
            "frequency": frequency,
            "data[0]": "value",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": "2",
        }

        # Add facet filters
        for facet_name, facet_value in facets.items():
            params[f"facets[{facet_name}][]"] = facet_value

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        logger.error(
                            "EIA API error for %s: HTTP %d", route, resp.status
                        )
                        return []

                    data = await resp.json()

            response = data.get("response", {})
            rows = response.get("data", [])
            return rows

        except Exception as e:
            logger.error("EIA fetch failed for %s: %s", route, e)
            return []

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch EIA data, return only new items."""
        series_to_check = EIA_SERIES[:limit] if limit else EIA_SERIES
        new_items = []

        for route, facets, frequency, name, category in series_to_check:
            rows = await self._fetch_series(route, facets, frequency)

            if not rows:
                logger.warning("EIA: no data for %s %s", route, facets)
                continue

            latest = rows[0]
            period = latest.get("period", "")
            value = latest.get("value")

            if value is None or value == "":
                continue

            value_str = str(value)

            # Get previous for comparison
            previous = rows[1] if len(rows) > 1 else None
            prev_value = str(previous["value"]) if previous and previous.get("value") is not None else None
            prev_period = previous.get("period", "") if previous else None

            # Build a readable series key from facets
            series_key = "/".join(f"{k}={v}" for k, v in facets.items())

            item_hash = self._make_hash(route, period, value_str)

            if self.db.is_already_sent("eia", item_hash):
                continue

            # Include units if available
            units = latest.get("units", "")
            display_value = f"{value_str} {units}".strip()
            display_prev = f"{prev_value} {units}".strip() if prev_value else None

            new_items.append({
                "series_id": series_key,
                "name": name,
                "category": category,
                "date": period,
                "value": display_value,
                "previous_value": display_prev,
                "previous_date": prev_period,
                "item_hash": item_hash,
            })

        self.db.update_source_status("eia", True)

        logger.info(
            "EIA check complete: %d new data points found (checked %d series)",
            len(new_items),
            len(series_to_check),
        )
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        """Format EIA data items for AI analysis."""
        if not items:
            return ""

        lines = ["Новые данные по энергетике от EIA (Energy Information Administration, США):\n"]

        for item in items:
            line = f"- {item['name']}: {item['value']} (период: {item['date']})"
            if item.get("previous_value"):
                line += f" | предыдущее: {item['previous_value']} ({item['previous_date']})"
            lines.append(line)

        return "\n".join(lines)
