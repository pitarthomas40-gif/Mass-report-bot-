"""Safe chat access helpers for Pyrogram clients.

The utilities here protect the reporting workflow from noisy ``PeerIdInvalid``
or invite-join flood waits by:

* Resolving chats defensively with membership checks.
* Joining via invite links with per-invite locks and bounded retries.
* Caching failures so the same bad peer is not hammered repeatedly.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from bot.invite_joiner import _extract_invite_hash

try:  # Lazy import for type checkers and runtime safety
    from pyrogram.errors import (  # type: ignore
        BadRequest,
        ChannelInvalid,
        ChannelPrivate,
        ChatAdminRequired,
        ChatIdInvalid,
        FloodWait,
        InviteHashExpired,
        InviteHashInvalid,
        PeerFlood,
        PeerIdInvalid,
        RPCError,
        UsernameInvalid,
        UsernameNotOccupied,
        UserAlreadyParticipant,
    )
except Exception:  # pragma: no cover - loaded lazily in tests
    BadRequest = ChannelInvalid = ChannelPrivate = ChatIdInvalid = PeerIdInvalid = UsernameInvalid = UsernameNotOccupied = FloodWait = RPCError = InviteHashExpired = InviteHashInvalid = ChatAdminRequired = PeerFlood = UserAlreadyParticipant = type("Dummy", (), {})  # type: ignore


_FAILURE_TTL = timedelta(minutes=45)
_LOG_THROTTLE = timedelta(minutes=10)


@dataclass
class FailureRecord:
    reason: str
    expires_at: datetime


_failure_cache: dict[str, FailureRecord] = {}
_log_cooldowns: dict[str, datetime] = {}
_invite_locks: dict[str, asyncio.Lock] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_key(chat_identifier: Any, invite_link: str | None = None) -> str:
    base = str(chat_identifier).strip()
    if invite_link:
        return f"{base}:{invite_link.strip()}".lower()
    return base.lower()


def _clean_failure_cache() -> None:
    expired = [key for key, record in _failure_cache.items() if record.expires_at <= _now()]
    for key in expired:
        _failure_cache.pop(key, None)


def _log_once(key: str, level: int, message: str, *args: Any, **kwargs: Any) -> None:
    next_allowed = _log_cooldowns.get(key)
    if next_allowed and next_allowed > _now():
        return

    _log_cooldowns[key] = _now() + _LOG_THROTTLE
    logging.log(level, message, *args, **kwargs)


async def join_by_invite_safe(client: Any, invite_link: str, *, max_retries: int = 3) -> dict[str, Any]:
    """Join a chat via invite with deduped, flood-aware retries."""

    invite_hash = _extract_invite_hash(invite_link)
    if not invite_hash:
        return {"ok": False, "status": "INVALID_LINK", "detail": "Unrecognized invite link", "wait_seconds": None}

    lock = _invite_locks.setdefault(invite_hash, asyncio.Lock())
    async with lock:
        attempts = 0
        join_target = f"https://t.me/+{invite_hash}"
        while attempts < max_retries:
            attempts += 1
            try:
                await client.join_chat(join_target)
                _log_once(
                    invite_hash,
                    logging.INFO,
                    "Joined invite %s via %s (attempt %s/%s)",
                    join_target,
                    getattr(client, "name", "client"),
                    attempts,
                    max_retries,
                )
                return {"ok": True, "status": "JOINED", "detail": "joined", "wait_seconds": None}
            except UserAlreadyParticipant:
                _log_once(
                    invite_hash,
                    logging.INFO,
                    "Already participant for invite %s via %s",
                    join_target,
                    getattr(client, "name", "client"),
                )
                return {"ok": True, "status": "ALREADY_JOINED", "detail": "already_participant", "wait_seconds": None}
            except FloodWait as exc:  # type: ignore[misc]
                wait_seconds = int(getattr(exc, "value", 0) or 0)
                jitter = random.uniform(0, 1)
                _log_once(
                    invite_hash,
                    logging.WARNING,
                    "Flood wait %ss while joining %s via %s (attempt %s/%s)",
                    wait_seconds,
                    join_target,
                    getattr(client, "name", "client"),
                    attempts,
                    max_retries,
                )
                if attempts >= max_retries:
                    return {"ok": False, "status": "RATE_LIMITED", "detail": str(exc), "wait_seconds": wait_seconds}
                await asyncio.sleep(wait_seconds + jitter)
                continue
            except (InviteHashInvalid, InviteHashExpired) as exc:  # type: ignore[misc]
                _log_once(
                    invite_hash,
                    logging.INFO,
                    "Invalid invite %s via %s: %s",
                    join_target,
                    getattr(client, "name", "client"),
                    exc.__class__.__name__,
                )
                return {"ok": False, "status": "INVALID_LINK", "detail": str(exc), "wait_seconds": None}
            except PeerFlood as exc:  # type: ignore[misc]
                _log_once(
                    invite_hash,
                    logging.WARNING,
                    "Peer flood while joining %s via %s",
                    join_target,
                    getattr(client, "name", "client"),
                )
                return {"ok": False, "status": "RATE_LIMITED", "detail": str(exc), "wait_seconds": getattr(exc, "value", None)}
            except (ChannelPrivate, ChatAdminRequired) as exc:  # type: ignore[misc]
                _log_once(
                    invite_hash,
                    logging.INFO,
                    "No access to invite %s via %s: %s",
                    join_target,
                    getattr(client, "name", "client"),
                    exc.__class__.__name__,
                )
                return {"ok": False, "status": "NO_ACCESS", "detail": str(exc), "wait_seconds": None}
            except RPCError as exc:  # type: ignore[misc]
                _log_once(
                    invite_hash,
                    logging.WARNING,
                    "RPC error joining %s via %s: %s",
                    join_target,
                    getattr(client, "name", "client"),
                    exc,
                )
                return {"ok": False, "status": "RPC_ERROR", "detail": str(exc), "wait_seconds": None}
            except Exception as exc:  # noqa: BLE001
                _log_once(
                    invite_hash,
                    logging.ERROR,
                    "Unexpected error joining %s via %s: %s",
                    join_target,
                    getattr(client, "name", "client"),
                    exc,
                )
                return {"ok": False, "status": "UNKNOWN_ERROR", "detail": str(exc), "wait_seconds": None}

    return {"ok": False, "status": "UNKNOWN_ERROR", "detail": "exhausted attempts", "wait_seconds": None}


async def resolve_chat_safe(
    client: Any,
    chat_identifier: Any,
    invite_link: str | None = None,
    *,
    max_attempts: int = 2,
) -> tuple[Any | None, dict[str, Any] | None]:
    """Resolve a chat and ensure access without noisy retries."""

    _clean_failure_cache()
    key = _normalize_key(chat_identifier, invite_link)
    cached = _failure_cache.get(key)
    if cached and cached.expires_at > _now():
        return None, {"status": "cached_failure", "detail": cached.reason}

    attempts = 0
    last_reason: str | None = None
    while attempts < max_attempts:
        attempts += 1
        try:
            chat = await client.get_chat(chat_identifier)
            return chat, None
        except FloodWait as exc:  # type: ignore[misc]
            wait_seconds = int(getattr(exc, "value", 0) or 0)
            jitter = random.uniform(0, 1)
            _log_once(
                key,
                logging.WARNING,
                "Flood wait %ss while resolving %s via %s (attempt %s/%s)",
                wait_seconds,
                chat_identifier,
                getattr(client, "name", "client"),
                attempts,
                max_attempts,
            )
            if attempts >= max_attempts:
                last_reason = "FloodWait"
                break
            await asyncio.sleep(wait_seconds + jitter)
            continue
        except (PeerIdInvalid, ChatIdInvalid, UsernameInvalid, UsernameNotOccupied, ChannelInvalid, ChannelPrivate) as exc:  # type: ignore[misc]
            last_reason = exc.__class__.__name__
            _log_once(
                key,
                logging.INFO,
                "Chat %s inaccessible via %s: %s",
                chat_identifier,
                getattr(client, "name", "client"),
                last_reason,
            )

            if invite_link:
                join_result = await join_by_invite_safe(client, invite_link)
                if join_result.get("ok"):
                    try:
                        chat = await client.get_chat(chat_identifier)
                        return chat, None
                    except Exception as post_join_exc:  # noqa: BLE001
                        last_reason = post_join_exc.__class__.__name__
                else:
                    last_reason = join_result.get("status") or last_reason
            break
        except (BadRequest, RPCError) as exc:  # type: ignore[misc]
            last_reason = exc.__class__.__name__
            _log_once(
                key,
                logging.WARNING,
                "RPC error resolving %s via %s: %s",
                chat_identifier,
                getattr(client, "name", "client"),
                exc,
            )
            break
        except Exception as exc:  # noqa: BLE001
            last_reason = exc.__class__.__name__
            _log_once(
                key,
                logging.ERROR,
                "Unexpected error resolving %s via %s: %s",
                chat_identifier,
                getattr(client, "name", "client"),
                exc,
            )
            break

    _failure_cache[key] = FailureRecord(reason=last_reason or "inaccessible_chat", expires_at=_now() + _FAILURE_TTL)
    return None, {"status": "inaccessible_chat", "detail": last_reason or "unknown"}


__all__ = [
    "resolve_chat_safe",
    "join_by_invite_safe",
]
