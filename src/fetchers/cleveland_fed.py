import hashlib
import logging

import aiohttp

from src.database import Database

logger = logging.getLogger("trading_bot")

# Cleveland Fed nowcasting JSON endpoints
NOWCAST_ENDPOINTS = {
    "month": {
        "url": "https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_month.json",
        "label": "м/м",
        "unit": "Month-over-month percent change",
    },
    "year": {
        "url": "https://www.clevelandfed.org/-/media/files/webcharts/inflationnowcasting/nowcast_year.json",
        "label": "г/г",
        "unit": "Year-over-year percent change",
    },
}

# Series we care about (nowcast, not actual)
TARGET_SERIES = ["CPI Inflation", "Core CPI Inflation", "PCE Inflation", "Core PCE Inflation"]


class ClevelandFedFetcher:
    """Fetches Cleveland Fed inflation nowcasting data from their JSON endpoints."""

    def __init__(self, db: Database):
        self.db = db

    def _make_hash(self, period: str, freq: str, values: str) -> str:
        raw = f"clevfed:{period}:{freq}:{values}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _extract_latest(self, chart_data: list) -> dict:
        """Extract the latest nowcast values from the most recent chart."""
        if not chart_data:
            return {}

        # Last chart in the array = most recent period
        chart = chart_data[-1]
        subcaption = chart.get("chart", {}).get("subcaption", "?")

        datasets = {
            ds["seriesname"]: ds["data"]
            for ds in chart.get("dataset", [])
        }
        categories = chart.get("categories", [{}])[0].get("category", [])

        results = {"period": subcaption}

        for series_name in TARGET_SERIES:
            values = datasets.get(series_name, [])
            # Find the last non-empty value (most recent nowcast)
            for i in range(len(values) - 1, -1, -1):
                v = values[i].get("value", "")
                if v:
                    date_label = categories[i].get("label", "?") if i < len(categories) else "?"
                    results[series_name] = {
                        "value": round(float(v), 4),
                        "date": date_label,
                    }
                    break

        return results

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest inflation nowcasts."""
        headers = {"User-Agent": "Mozilla/5.0"}
        new_items = []

        # Only fetch month-over-month by default (most actionable for traders)
        endpoints_to_check = ["month"]
        if limit and limit > 1:
            endpoints_to_check = list(NOWCAST_ENDPOINTS.keys())

        for freq in endpoints_to_check:
            endpoint = NOWCAST_ENDPOINTS[freq]

            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        endpoint["url"],
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        if resp.status != 200:
                            logger.error("Cleveland Fed error: HTTP %d", resp.status)
                            continue
                        import json
                        data = json.loads(await resp.text())

            except Exception as e:
                logger.error("Cleveland Fed fetch failed: %s", e)
                continue

            latest = self._extract_latest(data)
            if not latest or len(latest) <= 1:  # Only has "period" key
                continue

            # Build a hash from all values
            values_str = "|".join(
                f"{k}={v['value']}"
                for k, v in latest.items()
                if isinstance(v, dict)
            )
            item_hash = self._make_hash(latest["period"], freq, values_str)

            if self.db.is_already_sent("cleveland", item_hash):
                continue

            # Format the values for display
            value_parts = []
            for series_name in TARGET_SERIES:
                if series_name in latest:
                    short_name = series_name.replace(" Inflation", "")
                    val = latest[series_name]["value"]
                    value_parts.append(f"{short_name}: {val}%")

            new_items.append({
                "series_id": f"nowcast_{freq}",
                "name": f"Cleveland Fed — Inflation Nowcasting ({endpoint['label']})",
                "category": "Инфляция",
                "date": latest.get("period", "?"),
                "value": ", ".join(value_parts),
                "previous_value": None,
                "previous_date": None,
                "item_hash": item_hash,
                "detail": latest,
            })

        self.db.update_source_status("cleveland", True)
        logger.info("Cleveland Fed check: %d new items", len(new_items))
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Обновление прогнозов инфляции от Cleveland Fed (Inflation Nowcasting):\n"]

        for item in items:
            detail = item.get("detail", {})
            period = detail.get("period", "?")
            lines.append(f"- Период: {period}")

            for series_name in TARGET_SERIES:
                if series_name in detail:
                    val = detail[series_name]["value"]
                    date = detail[series_name]["date"]
                    lines.append(f"  {series_name}: {val}% (обновлено: {date})")

        return "\n".join(lines)
