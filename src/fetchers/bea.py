import hashlib
import logging
from datetime import datetime, timezone

import aiohttp

from src.config import Config
from src.database import Database

logger = logging.getLogger("trading_bot")

# BEA NIPA tables to track
# Format: (TableName, Frequency, SeriesCode filter, human name, category)
# SeriesCode = specific line item within the table (None = get all top-level)
# Reference: https://apps.bea.gov/iTable/?reqid=19&step=2
BEA_QUERIES = [
    # Table 1.1.1 — % Change in Real GDP (quarterly)
    ("T10101", "Q", "A191RL", "Реальный ВВП — % изменение (кварт.)", "gdp"),
    # Table 2.1 — Personal Income and Disposition (monthly)
    ("T20100", "M", "A065RC", "Личный доход (Personal Income)", "income"),
    ("T20100", "M", "A068RC", "Располагаемый личный доход", "income"),
    # Table 2.3.2 — Contributions to PCE change (quarterly)
    ("T20301", "Q", "DPCERL", "PCE — % изменение реальных расходов", "consumer"),
    # Table 2.6 — Personal Savings Rate (monthly)
    ("T20600", "M", "A072RC", "Норма сбережений (Personal Savings Rate)", "consumer"),
    # Table 3.9.3 — Real Government Spending (quarterly)
    ("T30903", "Q", "A822RL", "Гос.расходы — % изменение (реальные)", "government"),
    # Table 4.1 — Foreign Trade (quarterly)
    ("T40100", "Q", "A019RC", "Экспорт товаров и услуг", "trade"),
    ("T40100", "Q", "A021RC", "Импорт товаров и услуг", "trade"),
    # Table 5.3.3 — Private Fixed Investment (quarterly)
    ("T50303", "Q", "A007RL", "Частные инвестиции — % изменение (реальные)", "investment"),
]


class BEAFetcher:
    """Fetches latest economic data from BEA API (NIPA dataset)."""

    def __init__(self, db: Database):
        self.db = db
        self.base_url = "https://apps.bea.gov/api/data/"

    def _make_hash(self, table: str, series: str, period: str, value: str) -> str:
        raw = f"bea:{table}:{series}:{period}:{value}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def _fetch_table(
        self, table_name: str, frequency: str
    ) -> list[dict]:
        """Fetch the most recent data from a NIPA table."""
        current_year = datetime.now().year
        # Request current and previous year to get comparison data
        years = f"{current_year - 1},{current_year}"

        params = {
            "UserID": Config.BEA_API_KEY,
            "method": "GetData",
            "datasetname": "NIPA",
            "TableName": table_name,
            "Frequency": frequency,
            "Year": years,
            "ResultFormat": "JSON",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.base_url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        logger.error("BEA API error: HTTP %d", resp.status)
                        return []

                    data = await resp.json()

            results = data.get("BEAAPI", {}).get("Results", {})

            # BEA returns errors in a specific format
            if "Error" in results:
                logger.error("BEA API error: %s", results["Error"])
                return []

            return results.get("Data", [])

        except Exception as e:
            logger.error("BEA fetch failed for %s: %s", table_name, e)
            return []

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch BEA data, return only new items.

        Args:
            limit: Max number of queries to check. None = all.
        """
        queries = BEA_QUERIES[:limit] if limit else BEA_QUERIES
        new_items = []

        # Group queries by (table, frequency) to minimize API calls
        table_groups: dict[tuple[str, str], list[tuple]] = {}
        for table, freq, series_code, name, category in queries:
            key = (table, freq)
            if key not in table_groups:
                table_groups[key] = []
            table_groups[key].append((series_code, name, category))

        for (table, freq), series_list in table_groups.items():
            raw_data = await self._fetch_table(table, freq)
            if not raw_data:
                continue

            for series_code, name, category in series_list:
                # Filter rows matching our series code
                matching = [
                    r for r in raw_data
                    if r.get("SeriesCode") == series_code
                ]

                if not matching:
                    logger.warning(
                        "BEA: no data for %s in table %s", series_code, table
                    )
                    continue

                # Sort by TimePeriod descending to get latest first
                matching.sort(
                    key=lambda r: r.get("TimePeriod", ""), reverse=True
                )

                latest = matching[0]
                value = latest.get("DataValue", "").replace(",", "")
                period = latest.get("TimePeriod", "")

                if not value or value == "...":
                    continue

                # Get previous period for comparison
                previous = matching[1] if len(matching) > 1 else None
                prev_value = None
                prev_period = None
                if previous:
                    prev_value = previous.get("DataValue", "").replace(",", "")
                    prev_period = previous.get("TimePeriod", "")
                    if prev_value == "...":
                        prev_value = None

                # Check for duplicates
                item_hash = self._make_hash(table, series_code, period, value)
                if self.db.is_already_sent("bea", item_hash):
                    continue

                new_items.append({
                    "series_id": f"{table}/{series_code}",
                    "name": name,
                    "category": category,
                    "date": period,
                    "value": value,
                    "previous_value": prev_value,
                    "previous_date": prev_period,
                    "item_hash": item_hash,
                })

        self.db.update_source_status("bea", True)

        logger.info(
            "BEA check complete: %d new data points found (checked %d queries)",
            len(new_items),
            len(queries),
        )
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        """Format BEA data items for AI analysis."""
        if not items:
            return ""

        lines = ["Новые экономические данные от BEA (Bureau of Economic Analysis, США):\n"]

        for item in items:
            line = f"- {item['name']} ({item['series_id']}): {item['value']} (период: {item['date']})"
            if item.get("previous_value"):
                line += f" | предыдущее: {item['previous_value']} ({item['previous_date']})"
            lines.append(line)

        return "\n".join(lines)
