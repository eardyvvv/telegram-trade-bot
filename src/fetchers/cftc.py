import hashlib
import logging

import aiohttp

from src.database import Database

logger = logging.getLogger("trading_bot")

# Contracts to track — these strings are matched against field[0] (market name)
# Field[0] looks like: "GOLD - COMMODITY EXCHANGE INC."
COT_CONTRACTS = [
    ("GOLD", "Золото"),
    ("SILVER", "Серебро"),
    ("CRUDE OIL, LIGHT SWEET", "Нефть WTI"),
    ("NATURAL GAS", "Природный газ"),
    ("E-MINI S&P 500", "S&P 500 (E-mini)"),
    ("EURO FX", "EUR/USD"),
    ("JAPANESE YEN", "Японская йена"),
    ("BRITISH POUND", "Британский фунт"),
    ("U.S. DOLLAR INDEX", "Индекс доллара (DXY)"),
    ("10-YEAR U.S. TREASURY", "10-летние US Treasuries"),
    ("2-YEAR U.S. TREASURY", "2-летние US Treasuries"),
    ("WHEAT", "Пшеница"),
    ("CORN", "Кукуруза"),
]

# Positional column indices (no header row in this CSV)
# [0] = Market name, [2] = date (YYYY-MM-DD), [7] = Open Interest
# [8] = NonComm Long, [9] = NonComm Short
# [10] = Comm Long, [11] = Comm Short
IDX_MARKET = 0
IDX_DATE = 2
IDX_OI = 7
IDX_NCL = 8
IDX_NCS = 9
IDX_CL = 10
IDX_CS = 11


class CFTCFetcher:
    """Fetches CFTC Commitments of Traders (COT) data from public CSV."""

    def __init__(self, db: Database):
        self.db = db
        self.csv_url = "https://www.cftc.gov/dea/newcot/deacom.txt"

    def _make_hash(self, market: str, date: str, oi: str) -> str:
        raw = f"cot:{market}:{date}:{oi}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _parse_line(self, line: str) -> list[str]:
        """Parse a CSV line handling quoted fields."""
        fields = []
        in_quotes = False
        current = ""
        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == "," and not in_quotes:
                fields.append(current.strip())
                current = ""
            else:
                current += char
        fields.append(current.strip())
        return fields

    def _safe_int(self, fields: list[str], idx: int) -> int:
        try:
            return int(fields[idx].strip().replace(",", ""))
        except (ValueError, IndexError):
            return 0

    async def fetch_new_data(self, limit: int | None = None) -> list[dict]:
        """Fetch latest COT report and return new data."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.csv_url,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        logger.error("CFTC CSV download failed: HTTP %d", resp.status)
                        self.db.update_source_status("cftc", False)
                        return []
                    text = await resp.text(encoding="utf-8", errors="replace")
        except Exception as e:
            logger.error("CFTC download failed: %s", e)
            self.db.update_source_status("cftc", False)
            return []

        lines = text.strip().split("\n")
        logger.info("CFTC: downloaded %d lines", len(lines))

        # No header row — every line is data
        matched_items = []
        contracts_to_check = COT_CONTRACTS[:limit] if limit else COT_CONTRACTS

        for line in lines:
            fields = self._parse_line(line)
            if len(fields) < 12:
                continue

            market_raw = fields[IDX_MARKET].upper()

            # Try to match against our tracked contracts
            for search_term, ru_name in contracts_to_check:
                if search_term.upper() in market_raw:
                    date = fields[IDX_DATE].strip()
                    oi = self._safe_int(fields, IDX_OI)
                    ncl = self._safe_int(fields, IDX_NCL)
                    ncs = self._safe_int(fields, IDX_NCS)
                    cl = self._safe_int(fields, IDX_CL)
                    cs = self._safe_int(fields, IDX_CS)

                    item_hash = self._make_hash(search_term, date, str(oi))

                    if self.db.is_already_sent("cftc", item_hash):
                        break  # Already sent, skip this contract

                    matched_items.append({
                        "market": search_term,
                        "name": ru_name,
                        "series_id": search_term,
                        "category": "cot",
                        "date": date,
                        "open_interest": oi,
                        "noncomm_long": ncl,
                        "noncomm_short": ncs,
                        "noncomm_net": ncl - ncs,
                        "comm_long": cl,
                        "comm_short": cs,
                        "comm_net": cl - cs,
                        "item_hash": item_hash,
                    })
                    break  # Found match, don't check other contracts for this line

        self.db.update_source_status("cftc", True)

        logger.info(
            "CFTC COT check: %d new items from %d lines",
            len(matched_items),
            len(lines),
        )
        return matched_items

    def format_for_ai(self, items: list[dict]) -> str:
        if not items:
            return ""

        lines = ["Новые данные COT (Commitments of Traders, CFTC):\n"]

        for item in items:
            lines.append(
                f"- {item['name']} ({item['market']}, дата: {item['date']}):\n"
                f"  Open Interest: {item['open_interest']:,}\n"
                f"  Крупные спекулянты (Non-Commercial): "
                f"лонг {item['noncomm_long']:,}, шорт {item['noncomm_short']:,}, "
                f"нетто {item['noncomm_net']:+,}\n"
                f"  Хеджеры (Commercial): "
                f"лонг {item['comm_long']:,}, шорт {item['comm_short']:,}, "
                f"нетто {item['comm_net']:+,}"
            )

        return "\n".join(lines)
