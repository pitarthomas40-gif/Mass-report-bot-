from __future__ import annotations

import hashlib
from typing import Final

import config
from storage import build_datastore

BOT_TOKEN: Final[str] = config.BOT_TOKEN
API_ID: Final[int | None] = getattr(config, "API_ID", None)
API_HASH: Final[str | None] = getattr(config, "API_HASH", None)


def ensure_token() -> str:
    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is required. Set it via the BOT_TOKEN environment variable before starting the bot."
        )
    return BOT_TOKEN


def ensure_pyrogram_creds() -> None:
    """Guard that API credentials exist and look usable.

    Session strings on their own are not enough to talk to Telegram: the
    API ID/Hash pair used to create those sessions must also be configured on
    the worker. This check fails fast with a clear message instead of letting
    Pyrogram crash later with ``API_ID_INVALID``.
    """

    if not (API_ID and API_HASH):
        raise RuntimeError(
            "API_ID and API_HASH are required. Set valid values from https://my.telegram.org; "
            "session strings alone cannot replace them."
        )


def verify_author_integrity(author_name: str, expected_hash: str) -> None:
    computed_hash = hashlib.sha256(author_name.encode("utf-8")).hexdigest()
    if computed_hash != expected_hash:
        print("Integrity check failed: unauthorized modification.")
        raise SystemExit(1)


class _LazyDataStore:
    def __init__(self) -> None:
        self._instance = None

    def get(self):
        if self._instance is None:
            self._instance = build_datastore(config.MONGO_URI)
        return self._instance

    def __getattr__(self, item):
        return getattr(self.get(), item)


_data_store_proxy = _LazyDataStore()


def get_data_store():
    return _data_store_proxy.get()


data_store = _data_store_proxy

__all__ = [
    "BOT_TOKEN",
    "API_ID",
    "API_HASH",
    "ensure_token",
    "ensure_pyrogram_creds",
    "verify_author_integrity",
    "get_data_store",
    "data_store",
]
