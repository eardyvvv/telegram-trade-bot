#!/usr/bin/env python3
"""Trading News Bot — Entry point."""

import sys

from src.config import Config
from src.database import Database
from src.logger import setup_logging
from src.bot import TradingBot


def main() -> None:
    logger = setup_logging()
    logger.info("=" * 50)
    logger.info("Trading News Bot starting up...")

    # Validate configuration
    errors = Config.validate()
    if errors:
        for err in errors:
            logger.error("Config error: %s", err)
        logger.error(
            "Fix the errors above in your .env file and restart."
        )
        sys.exit(1)

    logger.info("Configuration OK")
    logger.info("Admin ID: %s", Config.ADMIN_ID)
    logger.info("AI Model: %s", Config.OPENAI_MODEL)

    # Initialize database
    db = Database()
    logger.info("Database initialized at %s", Config.DB_PATH)

    # Start bot
    bot = TradingBot(db)
    bot.run()


if __name__ == "__main__":
    main()
