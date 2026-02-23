from __future__ import annotations

"""Lightweight configuration helpers for the Pyrogram bot.

This module persists dynamic configuration such as the session/logs groups and
known chats. It reuses the existing ``DataStore`` when MongoDB is available and
falls back to in-memory storage otherwise.
"""

import asyncio

from storage import DataStore, FallbackDataStore, build_datastore


class ConfigStore:
    """Persist and retrieve runtime configuration safely."""

    def __init__(self, datastore: DataStore | FallbackDataStore) -> None:
        self.datastore = datastore
        self._memory_config: dict[str, int | list[int]] = {}
        self._lock = asyncio.Lock()

    async def get_value(self, key: str, default=None):
        if isinstance(self.datastore, FallbackDataStore):
            return self._memory_config.get(key, default)

        async with self._lock:
            doc = await self.datastore.db.config.find_one({"key": key})
            return doc["value"] if doc else default

    async def set_value(self, key: str, value) -> None:
        if isinstance(self.datastore, FallbackDataStore):
            self._memory_config[key] = value
            return

        async with self._lock:
            await self.datastore.db.config.update_one(
                {"key": key}, {"$set": {"value": value}}, upsert=True
            )

    async def add_known_chat(self, chat_id: int) -> None:
        chats = set(await self.get_value("known_chats", []))
        chats.add(int(chat_id))
        await self.set_value("known_chats", list(chats))

    async def known_chats(self) -> list[int]:
        return list(await self.get_value("known_chats", []))

    async def session_group(self) -> int | None:
        return await self.get_value("session_group")

    async def set_session_group(self, chat_id: int) -> None:
        await self.set_value("session_group", int(chat_id))

    async def logs_group(self) -> int | None:
        return await self.get_value("logs_group")

    async def set_logs_group(self, chat_id: int) -> None:
        await self.set_value("logs_group", int(chat_id))


def build_config_store(mongo_uri: str | None) -> tuple[ConfigStore, DataStore | FallbackDataStore]:
    datastore = build_datastore(mongo_uri)
    return ConfigStore(datastore), datastore


__all__ = ["ConfigStore", "build_config_store"]
