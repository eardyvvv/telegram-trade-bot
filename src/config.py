import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(Path(__file__).parent.parent / ".env")


class Config:
    """Central configuration loaded from environment variables."""

    # Telegram
    BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    ADMIN_ID: int = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))
    CHANNEL_ID: str = os.getenv("TELEGRAM_CHANNEL_ID", "")

    # OpenAI
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-5-mini")

    # FRED
    FRED_API_KEY: str = os.getenv("FRED_API_KEY", "")

    # BLS
    BLS_API_KEY: str = os.getenv("BLS_API_KEY", "")

    # BEA
    BEA_API_KEY: str = os.getenv("BEA_API_KEY", "")

    # EIA
    EIA_API_KEY: str = os.getenv("EIA_API_KEY", "")

    # Paths
    BASE_DIR: Path = Path(__file__).parent.parent
    DB_PATH: Path = BASE_DIR / "data" / "bot.db"
    LOG_DIR: Path = BASE_DIR / "logs"

    # Bot behavior
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Token safety limits
    MAX_INPUT_TOKENS: int = 4000        # Truncate any input beyond this
    DAILY_COST_LIMIT_USD: float = 2.0   # Stop AI calls if daily spend exceeds this
    INPUT_COST_PER_M: float = 0.25      # GPT-5 Mini: $0.25 per 1M input tokens
    OUTPUT_COST_PER_M: float = 2.00     # GPT-5 Mini: $2.00 per 1M output tokens

    @classmethod
    def validate(cls) -> list[str]:
        """Return list of missing required config values."""
        errors = []
        if not cls.BOT_TOKEN:
            errors.append("TELEGRAM_BOT_TOKEN is not set")
        if cls.ADMIN_ID == 0:
            errors.append("TELEGRAM_ADMIN_ID is not set")
        if not cls.OPENAI_API_KEY:
            errors.append("OPENAI_API_KEY is not set")
        return errors
