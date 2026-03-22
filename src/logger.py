import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.config import Config


def setup_logging() -> logging.Logger:
    """Configure logging to both file and console."""
    Config.LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("trading_bot")
    logger.setLevel(getattr(logging, Config.LOG_LEVEL, logging.INFO))

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler — rotates at 5 MB, keeps 3 backups
    file_handler = RotatingFileHandler(
        Config.LOG_DIR / "bot.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
