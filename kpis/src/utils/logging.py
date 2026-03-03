"""
Centralised logging configuration for the KPI pipeline.

Usage:
    from utils.logging import get_logger
    logger = get_logger(__name__)
    logger.info("Something happened")
"""

import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    """Configure root logger with a consistent format.

    Call once from main.py before any other modules log anything.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stdout,
        force=True,  # Override any handlers already attached (e.g. in tests)
    )


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Preferred over calling logging.getLogger directly."""
    return logging.getLogger(name)
