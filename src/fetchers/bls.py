import hashlib
import logging
from datetime import datetime, timezone

import aiohttp

from src.config import Config
from src.database import Database

logger = logging.getLogger("trading_bot")

# Key BLS series to track
# Format: (series_id, human-readable name, category)
# Series IDs found at: https://www.bls.gov/help/hlpforma.htm
BLS_SERIES = [
    # CPI — Consumer Price Index
    ("CUSR0000SA0", "CPI — все товары и услуги (сезонно скорр.)", "inflation"),
    ("CUSR0000SA0L1E", "Core CPI — без еды и энергии", "inflation"),
    ("CUSR0000SAF1", "CPI — продовольствие", "inflation"),
    ("CUSR0000SETA01", "CPI — новые автомобили", "inflation"),
    ("CUSR0000SEHA", "CPI — аренда жилья", "inflation"),
    # PPI — Producer Price Index
    ("WPSFD4", "PPI — конечный спрос (Final Demand)", "inflation"),
    ("WPUFD49104", "Core PPI — без еды и энергии", "inflation"),
    # Employment Situation
    ("CES0000000001", "Nonfarm Payrolls — общая занятость", "labor"),
    ("CES0500000003", "Средняя почасовая зарплата (частный сектор)", "labor"),
    ("LNS14000000", "Уровень безработицы (U-3)", "labor"),
    ("LNS14000006", "Безработица — U-6 (расширенная)", "labor"),
    ("LNS11300000", "Участие в рабочей силе (LFPR)", "labor"),
    # Productivity
    ("PRS85006092", "Производительность труда (несельскохозяйственный сектор)", "productivity"),
]


class BLSFetcher:
    """Fetches latest economic data from BLS API v2."""

    def __init__(self, db: Database):
        self.db = db
        self.api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    def _make_hash(self, series_id: str, year: str, period: str, value: str) -> str:
        """Create a unique fingerprint for a data point."""
        raw = f"{series_id}:{year}:{period}:{value}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def fetch_series_batch(
        self, series_ids: list[str]
    ) -> dict[str, list[dict]]:
        """Fetch multiple series in one API call (BLS allows up to 50).

        Returns dict mapping series_id -> list of observations.
        """
        # BLS v2 uses POST with JSON body
        payload = {
            "seriesid": series_ids,
            "registrationkey": Config.BLS_API_KEY,
            "startyear": str(datetime.now().year - 1),
            "endyear": str(datetime.now().year),
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        logger.error("BLS API error: HTTP %d", resp.status)
                        return {}

                    data = await resp.json()

            if data.get("status") != "REQUEST_SUCCEEDED":
                logger.error(
                    "BLS API error: %s", data.get("message", "unknown")
                )
                return {}

            results = {}
            for series in data.get("Results", {}).get("series", []):
                sid = series["seriesID"]
                observations = series.get("data", [])
                results[sid] = observations

            return results

        except Exception as e:
            logger.error("BLS fetch failed: %s", e)
            return {}

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch all tracked series, return only NEW data points.

        Args:
            limit: Max number of series to check (for testing). None = all.
        """
        series_to_check = BLS_SERIES[:limit] if limit else BLS_SERIES
        series_ids = [s[0] for s in series_to_check]

        # Build a lookup for names and categories
        series_info = {s[0]: (s[1], s[2]) for s in series_to_check}

        # BLS allows up to 50 series per call — we're well under that
        raw_data = await self.fetch_series_batch(series_ids)

        if not raw_data:
            self.db.update_source_status("bls", False)
            return []

        new_items = []

        for series_id, observations in raw_data.items():
            if not observations:
                continue

            latest = observations[0]  # BLS returns most recent first
            value = latest.get("value", "")
            year = latest.get("year", "")
            period = latest.get("period", "")
            period_name = latest.get("periodName", "")

            if not value or value == "-":
                continue

            # Check for duplicates
            item_hash = self._make_hash(series_id, year, period, value)
            if self.db.is_already_sent("bls", item_hash):
                continue

            name, category = series_info.get(series_id, ("Unknown", "other"))

            # Get previous value if available
            previous = observations[1] if len(observations) > 1 else None

            new_items.append({
                "series_id": series_id,
                "name": name,
                "category": category,
                "date": f"{period_name} {year}",
                "value": value,
                "previous_value": previous["value"] if previous and previous.get("value", "-") != "-" else None,
                "previous_date": f"{previous.get('periodName', '')} {previous.get('year', '')}" if previous else None,
                "item_hash": item_hash,
            })

        self.db.update_source_status("bls", True)

        logger.info(
            "BLS check complete: %d new data points found (checked %d series)",
            len(new_items),
            len(series_to_check),
        )
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        """Format a single BLS data item for AI analysis."""
        if not items:
            return ""

        lines = ["Новые экономические данные от BLS (Bureau of Labor Statistics, США):\n"]

        for item in items:
            line = f"- {item['name']} ({item['series_id']}): {item['value']} (дата: {item['date']})"
            if item.get("previous_value"):
                line += f" | предыдущее: {item['previous_value']} ({item['previous_date']})"
            lines.append(line)

        return "\n".join(lines)
