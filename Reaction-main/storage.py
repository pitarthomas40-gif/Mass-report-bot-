"""Async storage helpers for sessions, configuration, and audit logs."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
from pathlib import Path
from typing import Iterable

import config


class DataStore:
    """Persist session strings, chat configuration, and report audit records."""

    def __init__(
        self,
        client,
        db,
        *,
        mongo_uri: str | None = None,
        db_name: str = "reporter",
        mongo_env_var: str = "MONGO_URI",
    ) -> None:
        self.mongo_env_var = mongo_env_var
        self.mongo_uri = mongo_uri or os.getenv(self.mongo_env_var, "")
        self._in_memory_sessions: set[str] = set()
        self._in_memory_reports: list[dict] = []
        self._config_defaults: dict[str, int | None] = {
            "session_group": config.SESSION_GROUP_ID,
            "logs_group": config.LOGS_GROUP_ID,
        }
        self._in_memory_config: dict[str, int | None] = dict(self._config_defaults)
        self._in_memory_chats: set[int] = set()
        self._sudo_users: set[int] = set(config.SUDO_USERS)
        self._snapshot_path: Path | None = None

        self.client = client
        self.db = db or (self.client.get_default_database() if self.client else None)
        if not self.db and self.client:
            self.db = self.client[db_name]

    # ------------------- Internal helpers -------------------
    def _persist_snapshot(self) -> None:
        """Hook for subclasses to write the current in-memory state."""

    def _update_from_snapshot(self, payload: dict) -> None:
        self._in_memory_sessions = set(payload.get("sessions", []))
        self._in_memory_reports = payload.get("reports", [])
        self._in_memory_config.update(payload.get("config", {}))
        self._in_memory_chats = set(payload.get("chats", []))
        self._sudo_users = set(payload.get("sudo", [])) or set(config.SUDO_USERS)

    # ------------------- Session storage -------------------
    async def add_sessions(self, sessions: Iterable[str], added_by: int | None = None) -> list[str]:
        """Add unique session strings and return the list that were newly stored."""

        added: list[str] = []
        normalized = [s.strip() for s in sessions if s and s.strip()]

        if self.db:
            for session in normalized:
                result = await self.db.sessions.update_one(
                    {"session": session},
                    {
                        "$setOnInsert": {
                            "created_at": dt.datetime.utcnow(),
                            "added_by": added_by,
                        }
                    },
                    upsert=True,
                )
                if result.upserted_id:
                    added.append(session)
        else:
            for session in normalized:
                if session not in self._in_memory_sessions:
                    self._in_memory_sessions.add(session)
                    added.append(session)

        self._persist_snapshot()

        return added

    async def get_sessions(self) -> list[str]:
        """Return all known session strings."""

        if self.db:
            cursor = self.db.sessions.find({}, {"_id": False, "session": True})
            return [doc["session"] async for doc in cursor]

        return list(self._in_memory_sessions)

    async def remove_sessions(self, sessions: Iterable[str]) -> int:
        """Remove sessions from persistence, returning the count removed."""

        targets = {s for s in sessions if s}
        if not targets:
            return 0

        removed = 0
        if self.db:
            result = await self.db.sessions.delete_many({"session": {"$in": list(targets)}})
            removed = getattr(result, "deleted_count", 0)
        else:
            for session in list(targets):
                if session in self._in_memory_sessions:
                    self._in_memory_sessions.discard(session)
                    removed += 1

        self._persist_snapshot()

        return removed

    # ------------------- Report records -------------------
    async def record_report(self, payload: dict) -> None:
        """Persist a report summary payload."""

        payload = {
            **payload,
            "stored_at": dt.datetime.utcnow(),
        }
        if self.db:
            await self.db.reports.insert_one(payload)
        else:
            self._in_memory_reports.append(payload)

        self._persist_snapshot()

    # ------------------- Config storage -------------------
    async def set_session_group(self, chat_id: int) -> None:
        await self.save_session_group_id(chat_id)

    async def save_session_group_id(self, chat_id: int) -> None:
        self._in_memory_config["session_group"] = chat_id
        if self.db:
            await self.db.config.update_one(
                {"key": "session_group"},
                {"$set": {"value": chat_id}},
                upsert=True,
            )

        self._persist_snapshot()

    async def session_group(self) -> int | None:
        return await self.get_session_group_id()

    async def get_session_group_id(self) -> int | None:
        if self.db:
            doc = await self.db.config.find_one({"key": "session_group"})
            return doc["value"] if doc else self._in_memory_config.get("session_group")
        return self._in_memory_config.get("session_group")

    async def set_logs_group(self, chat_id: int) -> None:
        await self.save_logs_group_id(chat_id)

    async def save_logs_group_id(self, chat_id: int) -> None:
        self._in_memory_config["logs_group"] = chat_id
        if self.db:
            await self.db.config.update_one(
                {"key": "logs_group"},
                {"$set": {"value": chat_id}},
                upsert=True,
            )

        self._persist_snapshot()

    async def logs_group(self) -> int | None:
        return await self.get_logs_group_id()

    async def get_logs_group_id(self) -> int | None:
        if self.db:
            doc = await self.db.config.find_one({"key": "logs_group"})
            return doc["value"] if doc else self._in_memory_config.get("logs_group")
        return self._in_memory_config.get("logs_group")

    async def _set_config_value(self, key: str, value: int | None) -> None:
        if self.db:
            await self.db.config.update_one({"_id": "config"}, {"$set": {key: value}}, upsert=True)
        else:
            self._in_memory_config[key] = value

        self._persist_snapshot()

    async def _get_config_value(self, key: str) -> int | None:
        if self.db:
            doc = await self.db.config.find_one({"_id": "config"}, {"_id": False, key: True})
            if doc and key in doc:
                return doc.get(key)
        if key in self._in_memory_config:
            return self._in_memory_config.get(key)
        return self._config_defaults.get(key)

    # ------------------- Known chats -------------------
    async def add_known_chat(self, chat_id: int) -> None:
        if self.db:
            await self.db.chats.update_one({"chat_id": chat_id}, {"$set": {"chat_id": chat_id}}, upsert=True)
        else:
            self._in_memory_chats.add(chat_id)

        self._persist_snapshot()

    # ------------------- Sudo users -------------------
    async def add_sudo_user(self, user_id: int) -> None:
        self._sudo_users.add(user_id)
        if self.db:
            await self.db.sudo.update_one({"_id": user_id}, {"$set": {}}, upsert=True)
        config.SUDO_USERS.add(user_id)
        self._persist_snapshot()

    async def remove_sudo_user(self, user_id: int) -> None:
        self._sudo_users.discard(user_id)
        config.SUDO_USERS.discard(user_id)
        if self.db:
            await self.db.sudo.delete_one({"_id": user_id})
        self._persist_snapshot()

    async def get_sudo_users(self) -> set[int]:
        if self.db:
            users = await self.db.sudo.find().to_list(None)
            if users:
                return {u["_id"] for u in users if "_id" in u}
        return set(self._sudo_users)

    async def known_chats(self) -> list[int]:
        if self.db:
            cursor = self.db.chats.find({}, {"_id": False, "chat_id": True})
            return [doc["chat_id"] async for doc in cursor]
        return list(self._in_memory_chats)

    # ------------------- Lifecycle -------------------
    async def close(self) -> None:
        if self.client:
            self.client.close()

    @property
    def is_persistent(self) -> bool:
        """Expose whether MongoDB is available for callers that want to log mode."""

        return bool(self.db)


class FallbackDataStore(DataStore):
    """In-memory persistence used when MongoDB is unavailable."""

    def __init__(self, *, snapshot_path: str = "data_store.json") -> None:
        super().__init__(client=None, db=None)
        self._snapshot_path = Path(snapshot_path)
        self._load_snapshot()

    async def close(self) -> None:
        return None

    @property
    def is_persistent(self) -> bool:  # pragma: no cover - small override
        return False

    # ------------------- File persistence helpers -------------------
    def _load_snapshot(self) -> None:
        if not self._snapshot_path or not self._snapshot_path.exists():
            return
        try:
            payload = json.loads(self._snapshot_path.read_text())
            if isinstance(payload, dict):
                self._update_from_snapshot(payload)
        except Exception:
            logging.warning("Failed to load snapshot file; starting fresh.")

    def _persist_snapshot(self) -> None:
        if not self._snapshot_path:
            return
        payload = {
            "sessions": list(self._in_memory_sessions),
            "reports": self._in_memory_reports,
            "config": self._in_memory_config,
            "chats": list(self._in_memory_chats),
            "sudo": list(self._sudo_users),
        }
        try:
            self._snapshot_path.write_text(json.dumps(payload, default=str))
        except Exception:
            logging.warning("Failed to write snapshot to disk.")


def build_datastore(
    mongo_uri: str | None,
    *,
    db_name: str = "reporter",
    mongo_env_var: str = "MONGO_URI",
    snapshot_path: str = "data_store.json",
) -> DataStore:
    """Build a datastore safely, falling back when MongoDB is unavailable."""

    resolved_uri = mongo_uri or os.getenv(mongo_env_var, "")
    if not resolved_uri:
        logging.warning(
            "MongoDB persistence disabled; set %s to a MongoDB connection URI to enable it.",
            mongo_env_var,
        )
        return FallbackDataStore(snapshot_path=snapshot_path)

    try:  # pragma: no cover - optional dependency
        import motor.motor_asyncio as motor_asyncio
    except Exception as exc:
        logging.warning(
            "MongoDB URI provided but Motor is unavailable; falling back to in-memory storage. Import error: %s",
            exc,
        )
        return FallbackDataStore(snapshot_path=snapshot_path)

    try:
        client = motor_asyncio.AsyncIOMotorClient(resolved_uri)
        db = client.get_default_database() or client[db_name]
        logging.info("Connected to MongoDB for session persistence.")
        return DataStore(client, db, mongo_uri=resolved_uri, db_name=db_name, mongo_env_var=mongo_env_var)
    except Exception as exc:
        logging.warning(
            "Failed to initialize MongoDB client with %s; falling back to in-memory storage: %s",
            mongo_env_var,
            exc,
        )
        return FallbackDataStore(snapshot_path=snapshot_path)

