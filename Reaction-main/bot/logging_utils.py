from __future__ import annotations

import logging


def build_logger() -> None:
    """Configure structured logging for the bot process."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("pyrogram").setLevel(logging.WARNING)


__all__ = ["build_logger"]
