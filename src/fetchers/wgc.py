import hashlib
import logging

import aiohttp

from src.database import Database

logger = logging.getLogger("trading_bot")


class WorldGoldFetcher:
    """Fetches World Gold Council supply/demand data from their API."""

    def __init__(self, db: Database):
        self.db = db
        self.supply_demand_url = "https://fsapi.gold.org/api/v11/charts/supply-and-demand/40"

    def _make_hash(self, period: str, data_key: str) -> str:
        raw = f"wgc:{period}:{data_key}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest gold supply/demand data."""
        headers = {"User-Agent": "Mozilla/5.0"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.supply_demand_url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        logger.error("World Gold Council error: HTTP %d", resp.status)
                        self.db.update_source_status("wgc", False)
                        return []
                    data = await resp.json()

        except Exception as e:
            logger.error("World Gold Council fetch failed: %s", e)
            self.db.update_source_status("wgc", False)
            return []

        chart = data.get("chartData", {})
        if not chart:
            self.db.update_source_status("wgc", False)
            return []

        new_items = []

        # Get latest quarterly supply & demand
        supply_q = chart.get("Supply_Quarterly", {})
        demand_q = chart.get("Demand_Quarterly", {})

        if supply_q and demand_q:
            categories = supply_q.get("categories", [])
            if not categories:
                self.db.update_source_status("wgc", True)
                return []

            latest_period = categories[-1]  # e.g. "Q4 '25"

            # Build supply breakdown
            supply_series = supply_q.get("series", [])
            supply_data = {}
            for s in supply_series:
                name = s.get("name", "")
                values = s.get("data", [])
                if values and name != "LBMA":
                    supply_data[name] = round(values[-1], 1)

            # Build demand breakdown
            demand_series = demand_q.get("series", [])
            demand_data = {}
            for s in demand_series:
                name = s.get("name", "")
                values = s.get("data", [])
                if values and name != "LBMA":
                    demand_data[name] = round(values[-1], 1)

            # Get LBMA gold price for context
            gold_price = None
            for s in supply_series:
                if s.get("name") == "LBMA":
                    values = s.get("data", [])
                    if values:
                        gold_price = round(values[-1], 1)

            # Total supply and demand
            total_supply = round(sum(supply_data.values()), 1)
            total_demand = round(sum(demand_data.values()), 1)

            # Build a hash from the latest period data
            hash_key = f"{total_supply}|{total_demand}|{gold_price}"
            item_hash = self._make_hash(latest_period, hash_key)

            if not self.db.is_already_sent("wgc", item_hash):
                # Get previous quarter for comparison
                prev_supply = {}
                prev_demand = {}
                if len(categories) >= 2:
                    for s in supply_series:
                        name = s.get("name", "")
                        values = s.get("data", [])
                        if len(values) >= 2 and name != "LBMA":
                            prev_supply[name] = round(values[-2], 1)
                    for s in demand_series:
                        name = s.get("name", "")
                        values = s.get("data", [])
                        if len(values) >= 2 and name != "LBMA":
                            prev_demand[name] = round(values[-2], 1)

                prev_total_supply = round(sum(prev_supply.values()), 1) if prev_supply else None
                prev_total_demand = round(sum(prev_demand.values()), 1) if prev_demand else None
                prev_period = categories[-2] if len(categories) >= 2 else None

                new_items.append({
                    "series_id": "WGC_SD",
                    "name": "World Gold Council — Спрос и предложение золота",
                    "category": "Металлы",
                    "date": latest_period,
                    "value": f"Предложение: {total_supply}т, Спрос: {total_demand}т",
                    "previous_value": f"Предложение: {prev_total_supply}т, Спрос: {prev_total_demand}т" if prev_total_supply else None,
                    "previous_date": prev_period,
                    "item_hash": item_hash,
                    "supply": supply_data,
                    "demand": demand_data,
                    "total_supply": total_supply,
                    "total_demand": total_demand,
                    "gold_price": gold_price,
                })

        if limit and limit < len(new_items):
            new_items = new_items[:limit]

        self.db.update_source_status("wgc", True)
        logger.info("World Gold Council check: %d new items", len(new_items))
        return new_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Обновление данных World Gold Council (Мировой Совет по Золоту) — спрос и предложение:\n"]

        for item in items:
            lines.append(f"- Период: {item['date']}")
            lines.append(f"  Цена золота LBMA: ${item.get('gold_price', 'н/д')}/унция")
            lines.append(f"  Общее предложение: {item['total_supply']} тонн")

            supply = item.get("supply", {})
            for name, val in supply.items():
                lines.append(f"    {name}: {val}т")

            lines.append(f"  Общий спрос: {item['total_demand']} тонн")

            demand = item.get("demand", {})
            for name, val in demand.items():
                lines.append(f"    {name}: {val}т")

            if item.get("previous_value"):
                lines.append(f"  Предыдущий квартал ({item['previous_date']}): {item['previous_value']}")

        return "\n".join(lines)
