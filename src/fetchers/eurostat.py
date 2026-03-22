import hashlib
import logging

import aiohttp

from src.database import Database

logger = logging.getLogger("trading_bot")

# Eurostat datasets to track
# Format: (dataset_id, params_dict, human_name, category)
# API: https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}
EUROSTAT_SERIES = [
    # GDP growth — EU27 quarterly
    (
        "namq_10_gdp",
        {"geo": "EU27_2020", "unit": "CLV_PCH_PRE", "s_adj": "SCA", "na_item": "B1GQ", "freq": "Q"},
        "ВВП ЕС — % изменение (кварт.)",
        "gdp",
    ),
    # GDP growth — Euro area
    (
        "namq_10_gdp",
        {"geo": "EA20", "unit": "CLV_PCH_PRE", "s_adj": "SCA", "na_item": "B1GQ", "freq": "Q"},
        "ВВП еврозоны — % изменение (кварт.)",
        "gdp",
    ),
    # HICP inflation — EU27 monthly
    (
        "prc_hicp_manr",
        {"geo": "EU27_2020", "coicop": "CP00", "freq": "M"},
        "Инфляция HICP ЕС — годовой % (месячн.)",
        "inflation",
    ),
    # HICP inflation — Euro area
    (
        "prc_hicp_manr",
        {"geo": "EA20", "coicop": "CP00", "freq": "M"},
        "Инфляция HICP еврозоны — годовой %",
        "inflation",
    ),
    # Core HICP — Euro area (excl energy, food, alcohol, tobacco)
    (
        "prc_hicp_manr",
        {"geo": "EA20", "coicop": "TOT_X_NRG_FOOD", "freq": "M"},
        "Core HICP еврозоны (без энергии и еды)",
        "inflation",
    ),
    # Unemployment rate — EU27
    (
        "une_rt_m",
        {"geo": "EU27_2020", "s_adj": "SA", "age": "TOTAL", "unit": "PC_ACT", "sex": "T", "freq": "M"},
        "Безработица ЕС — %",
        "labor",
    ),
    # Unemployment rate — Euro area
    (
        "une_rt_m",
        {"geo": "EA20", "s_adj": "SA", "age": "TOTAL", "unit": "PC_ACT", "sex": "T", "freq": "M"},
        "Безработица еврозоны — %",
        "labor",
    ),
    # Industrial production — EU27 monthly
    (
        "sts_inpr_m",
        {"geo": "EU27_2020", "unit": "PCH_SM", "s_adj": "SCA", "nace_r2": "B-D", "freq": "M"},
        "Промышленное производство ЕС — % изм. (месячн.)",
        "industry",
    ),
    # Retail trade volume — Euro area
    (
        "sts_trtu_m",
        {"geo": "EA20", "unit": "PCH_SM", "s_adj": "SCA", "nace_r2": "G47", "freq": "M"},
        "Розничная торговля еврозоны — % изм. (месячн.)",
        "consumer",
    ),
]


class EurostatFetcher:
    """Fetches EU economic data from Eurostat JSON API. No API key needed."""

    def __init__(self, db: Database):
        self.db = db
        self.base_url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

    def _make_hash(self, dataset: str, geo: str, period: str, value: str) -> str:
        raw = f"eurostat:{dataset}:{geo}:{period}:{value}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def _fetch_dataset(
        self, dataset_id: str, params: dict
    ) -> tuple[list[str], list[float | None]]:
        """Fetch from Eurostat JSON API.

        Returns (time_periods, values) sorted most recent first.
        """
        url = f"{self.base_url}/{dataset_id}"

        query_params = {
            "format": "JSON",
            "lang": "en",
            "lastTimePeriod": "2",
            **params,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    params=query_params,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        logger.error(
                            "Eurostat API error for %s: HTTP %d",
                            dataset_id,
                            resp.status,
                        )
                        return [], []

                    data = await resp.json()

            # Eurostat JSON-stat format: values indexed by position
            values_dict = data.get("value", {})
            time_dim = data.get("dimension", {}).get("time", {})
            time_index = time_dim.get("category", {}).get("index", {})

            if not values_dict or not time_index:
                return [], []

            # Sort time periods — most recent first
            sorted_periods = sorted(time_index.items(), key=lambda x: x[1])
            periods = [p[0] for p in sorted_periods]
            values = [values_dict.get(str(p[1])) for p in sorted_periods]

            # Reverse to get most recent first
            periods.reverse()
            values.reverse()

            return periods, values

        except Exception as e:
            logger.error("Eurostat fetch failed for %s: %s", dataset_id, e)
            return [], []

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch Eurostat data, return only new items."""
        series_to_check = EUROSTAT_SERIES[:limit] if limit else EUROSTAT_SERIES
        new_items = []

        for dataset_id, params, name, category in series_to_check:
            periods, values = await self._fetch_dataset(dataset_id, params)

            if not periods or not values or values[0] is None:
                logger.warning("Eurostat: no data for %s", dataset_id)
                continue

            latest_period = periods[0]
            latest_value = str(values[0])

            prev_value = str(values[1]) if len(values) > 1 and values[1] is not None else None
            prev_period = periods[1] if len(periods) > 1 else None

            geo = params.get("geo", "EU")
            item_hash = self._make_hash(dataset_id, geo, latest_period, latest_value)

            if self.db.is_already_sent("eurostat", item_hash):
                continue

            new_items.append({
                "series_id": f"{dataset_id}/{geo}",
                "name": name,
                "category": category,
                "date": latest_period,
                "value": latest_value,
                "previous_value": prev_value,
                "previous_date": prev_period,
                "item_hash": item_hash,
            })

        self.db.update_source_status("eurostat", True)

        logger.info(
            "Eurostat check complete: %d new data points (checked %d)",
            len(new_items),
            len(series_to_check),
        )
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Новые экономические данные Eurostat (Европейский Союз):\n"]

        for item in items:
            line = f"- {item['name']}: {item['value']}% (период: {item['date']})"
            if item.get("previous_value"):
                line += f" | предыдущее: {item['previous_value']}% ({item['previous_date']})"
            lines.append(line)

        return "\n".join(lines)
