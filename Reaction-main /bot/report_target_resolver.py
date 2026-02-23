from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

from invite_joiner import join_by_invite
from link_parser import maybe_parse_message_link
from target_resolver import parse_target as _legacy_parse_target


@dataclass(frozen=True)
class ReportTargetSpec:
    raw: str
    normalized: str
    kind: str
    username: str | None = None
    numeric_id: int | None = None
    invite_link: str | None = None
    invite_hash: str | None = None
    message_ids: list[int] | None = None
    internal_id: int | None = None

    def cache_key(self) -> str:
        return self.normalized.lower()


_TRIM_CHARS = ",.;)]}>'\\\""
_SUCCESS_TTL = timedelta(minutes=10)
_FAILURE_TTL = timedelta(minutes=5)
_JOIN_SUCCESS_TTL = timedelta(minutes=5)
_CACHE: dict[str, tuple[dict[str, Any], datetime]] = {}
_FAILURE_CACHE: dict[str, tuple[dict[str, Any], datetime]] = {}
_JOIN_CACHE: dict[tuple[str, str], datetime] = {}


def _clean_target(raw: str) -> str:
    cleaned = (raw or "").strip()
    return cleaned.rstrip(_TRIM_CHARS)


def _strip_query_and_fragment(target: str) -> str:
    parsed = urlparse(target if target.startswith("http") else f"https://{target}")
    path = parsed.path.rstrip("/")
    prefix = f"{parsed.scheme + '://' if parsed.scheme else ''}{parsed.netloc}"
    return f"{prefix}{path}" if prefix else path


def _parse_target(raw_input: str) -> ReportTargetSpec:
    """Normalize report targets into a structured representation."""

    cleaned_raw = _clean_target(raw_input)
    if not cleaned_raw:
        raise ValueError("Target is empty")

    stripped = _strip_query_and_fragment(cleaned_raw)
    parsed = urlparse(stripped if stripped.startswith("http") else f"https://{stripped}")
    path_parts: list[str] = [p for p in parsed.path.split("/") if p]
    netloc = parsed.netloc.lower()

    # Invite links
    if netloc.endswith("t.me") and path_parts:
        first = path_parts[0]
        if first.startswith("+"):
            invite_hash = first.lstrip("+")
            invite_link = f"https://t.me/+{invite_hash}"
            return ReportTargetSpec(
                raw=raw_input,
                normalized=f"invite:{invite_hash}",
                kind="invite",
                invite_link=invite_link,
                invite_hash=invite_hash,
            )
        if first.lower() == "joinchat" and len(path_parts) >= 2:
            invite_hash = path_parts[1]
            invite_link = f"https://t.me/joinchat/{invite_hash}"
            return ReportTargetSpec(
                raw=raw_input,
                normalized=f"invite:{invite_hash}",
                kind="invite",
                invite_link=invite_link,
                invite_hash=invite_hash,
            )

    message_link = maybe_parse_message_link(stripped)
    if message_link:
        if message_link.is_private:
            chat_id = int(f"-100{message_link.internal_id}") if message_link.internal_id is not None else None
            return ReportTargetSpec(
                raw=raw_input,
                normalized=message_link.normalized_url,
                kind="internal_message",
                internal_id=message_link.internal_id,
                message_ids=[message_link.message_id],
                numeric_id=chat_id,
            )
        return ReportTargetSpec(
            raw=raw_input,
            normalized=message_link.normalized_url,
            kind="message",
            username=message_link.username,
            message_ids=[message_link.message_id],
        )

    # Numeric IDs
    numeric_candidate = cleaned_raw.replace(" ", "")
    if numeric_candidate.startswith("http://") or numeric_candidate.startswith("https://"):
        numeric_candidate = numeric_candidate.split("://", 1)[1]
    if numeric_candidate.startswith("t.me/"):
        numeric_candidate = numeric_candidate.split("/", 1)[1]
    if numeric_candidate.lstrip("+").lstrip("-").isdigit():
        numeric_id = int(numeric_candidate)
        return ReportTargetSpec(
            raw=raw_input,
            normalized=str(numeric_id),
            kind="numeric",
            numeric_id=numeric_id,
        )

    # Internal message shortcut (t.me/c/...)
    if netloc.endswith("t.me") and path_parts and path_parts[0].lower() == "c":
        internal_id = int(path_parts[1]) if len(path_parts) >= 2 and path_parts[1].isdigit() else None
        message_id = int(path_parts[2]) if len(path_parts) >= 3 and path_parts[2].isdigit() else None
        if internal_id is None:
            raise ValueError("Internal message link missing chat id")
        chat_id = int(f"-100{internal_id}")
        return ReportTargetSpec(
            raw=raw_input,
            normalized=f"https://t.me/c/{internal_id}{f'/{message_id}' if message_id else ''}",
            kind="internal_message",
            internal_id=internal_id,
            message_ids=[message_id] if message_id else None,
            numeric_id=chat_id,
        )

    # Username via t.me
    if netloc.endswith("t.me") and path_parts:
        username = path_parts[0].lstrip("@")
        if not username:
            raise ValueError("The t.me link is missing a username")
        return ReportTargetSpec(raw=raw_input, normalized=username, kind="username", username=username)

    # Bare username or legacy parsing fallback
    username = cleaned_raw.lstrip("@")
    if username:
        return ReportTargetSpec(raw=raw_input, normalized=username, kind="username", username=username)

    # As a final fallback try legacy parser to avoid regression
    legacy_spec = _legacy_parse_target(raw_input)
    return ReportTargetSpec(
        raw=raw_input,
        normalized=legacy_spec.normalized,
        kind=legacy_spec.kind,
        username=legacy_spec.username,
        numeric_id=legacy_spec.numeric_id,
        invite_link=legacy_spec.invite_link,
        invite_hash=legacy_spec.invite_hash,
        message_ids=[legacy_spec.message_id] if getattr(legacy_spec, "message_id", None) else None,
        internal_id=legacy_spec.internal_id,
    )


def _purge_cache() -> None:
    now = datetime.now(timezone.utc)
    for store in (_CACHE, _FAILURE_CACHE):
        expired = [key for key, (_, exp) in store.items() if exp <= now]
        for key in expired:
            store.pop(key, None)

    expired_joins = [key for key, exp in _JOIN_CACHE.items() if exp <= now]
    for key in expired_joins:
        _JOIN_CACHE.pop(key, None)


def _get_cached(normalized: str) -> dict[str, Any] | None:
    _purge_cache()
    now = datetime.now(timezone.utc)
    key = normalized.lower()
    if key in _CACHE:
        cached, exp = _CACHE[key]
        if exp > now:
            return cached
    if key in _FAILURE_CACHE:
        cached, exp = _FAILURE_CACHE[key]
        if exp > now:
            return cached
        _FAILURE_CACHE.pop(key, None)
    return None


def _cache_result(normalized: str, result: dict[str, Any], *, failure: bool = False) -> None:
    if failure and result.get("error") in {"PeerIdInvalid", "ChannelPrivate"}:
        return
    ttl = _FAILURE_TTL if failure else _SUCCESS_TTL
    store = _FAILURE_CACHE if failure else _CACHE
    store[normalized.lower()] = (result, datetime.now(timezone.utc) + ttl)


async def _sleep_for_flood(wait_seconds: int) -> None:
    await asyncio.sleep(min(wait_seconds, 60))


async def _attempt_join(
    client: Any,
    spec: ReportTargetSpec,
    *,
    invite_link: str | None,
    username: str | None,
    allow_join: bool,
) -> dict[str, Any]:
    from pyrogram.errors import (  # type: ignore
        FloodWait,
        RPCError,
        UserAlreadyParticipant,
    )

    session_name = getattr(client, "name", "client")
    cache_key = (session_name, spec.cache_key())
    now = datetime.now(timezone.utc)
    cached = _JOIN_CACHE.get(cache_key)
    if cached and cached > now:
        return {"ok": True, "joined": False, "status": "cached", "session": session_name}

    if not allow_join or not (invite_link or username):
        return {"ok": False, "joined": False, "status": "join_not_possible", "session": session_name}

    join_target = invite_link or username
    attempts = 0

    async def _join_once() -> dict[str, Any]:
        try:
            if invite_link:
                invite_result = await join_by_invite(client, invite_link)
                if not invite_result.get("ok"):
                    wait_seconds = invite_result.get("wait_seconds") or 0
                    if invite_result.get("status") == "VALID_BUT_RATE_LIMITED" and wait_seconds:
                        await _sleep_for_flood(wait_seconds)
                        return {
                            "ok": False,
                            "joined": False,
                            "status": "flood_wait",
                            "session": session_name,
                            "error": "FloodWait",
                            "wait_seconds": wait_seconds,
                        }
                    return {
                        "ok": False,
                        "joined": False,
                        "status": invite_result.get("status", "join_failed"),
                        "session": session_name,
                        "error": invite_result.get("detail"),
                    }
                method = "invite"
            else:
                await client.join_chat(username)
                method = "username"
            _JOIN_CACHE[cache_key] = datetime.now(timezone.utc) + _JOIN_SUCCESS_TTL
            logging.info(
                "ReportTargetResolver: joined chat",
                extra={"session_name": session_name, "method": method, "join_target": join_target},
            )
            return {"ok": True, "joined": True, "status": method, "session": session_name}
        except UserAlreadyParticipant:
            _JOIN_CACHE[cache_key] = datetime.now(timezone.utc) + _JOIN_SUCCESS_TTL
            logging.info(
                "ReportTargetResolver: already participant",
                extra={"session_name": session_name, "join_target": join_target},
            )
            return {"ok": True, "joined": False, "status": "already", "session": session_name}
        except FloodWait as fw:
            wait_seconds = int(getattr(fw, "value", 0) or 0)
            await _sleep_for_flood(wait_seconds or 1)
            return {
                "ok": False,
                "joined": False,
                "status": "flood_wait",
                "session": session_name,
                "error": fw.__class__.__name__,
                "wait_seconds": wait_seconds or None,
            }
        except RPCError as exc:
            logging.warning(
                "ReportTargetResolver: join rpc error",
                extra={"session_name": session_name, "error": exc.__class__.__name__},
            )
            return {
                "ok": False,
                "joined": False,
                "status": "join_failed",
                "session": session_name,
                "error": exc.__class__.__name__,
            }
        except Exception as exc:  # noqa: BLE001
            logging.exception("ReportTargetResolver: unexpected join failure")
            return {
                "ok": False,
                "joined": False,
                "status": "join_failed",
                "session": session_name,
                "error": exc.__class__.__name__,
            }

    while attempts < 2:
        attempts += 1
        result = await _join_once()
        if result["ok"] or result.get("status") != "flood_wait":
            return result

    return {"ok": False, "joined": False, "status": "join_exhausted", "session": session_name}


async def _try_get_chat(client: Any, spec: ReportTargetSpec) -> tuple[bool, Any | None, str | None]:
    from pyrogram.errors import (  # type: ignore
        BadRequest,
        ChannelPrivate,
        FloodWait,
        PeerIdInvalid,
        RPCError,
    )

    attempts = 0
    last_error: str | None = None
    target_ref: Any
    if spec.kind == "numeric" and spec.numeric_id is not None:
        target_ref = spec.numeric_id
    elif spec.kind in {"username", "message"} and spec.username:
        target_ref = spec.username
    elif spec.kind == "internal_message" and spec.numeric_id is not None:
        target_ref = spec.numeric_id
    elif spec.kind == "invite" and spec.invite_link:
        target_ref = spec.invite_link
    else:
        return False, None, "unsupported_target"

    while attempts < 3:
        attempts += 1
        try:
            chat = await client.get_chat(target_ref)
            return True, chat, None
        except FloodWait as fw:
            wait_seconds = int(getattr(fw, "value", 0) or 0)
            await _sleep_for_flood(wait_seconds or 1)
            last_error = "FloodWait"
            continue
        except (PeerIdInvalid, ChannelPrivate, BadRequest) as exc:
            last_error = exc.__class__.__name__
            return False, None, last_error
        except RPCError as exc:
            last_error = exc.__class__.__name__
            await asyncio.sleep(min(attempts, 3))
            continue
        except Exception as exc:  # noqa: BLE001
            last_error = exc.__class__.__name__
            return False, None, last_error

    return False, None, last_error or "resolution_exhausted"


def _chat_id_from_chat(chat: Any) -> int:
    if hasattr(chat, "id"):
        return int(getattr(chat, "id"))
    if hasattr(chat, "chat_id"):
        return int(getattr(chat, "chat_id"))
    if hasattr(chat, "channel_id"):
        return int(f"-100{getattr(chat, 'channel_id')}")
    raise ValueError("Chat has no identifiable id field")


async def _resolve_with_client(client: Any, spec: ReportTargetSpec, *, allow_join: bool) -> dict[str, Any]:
    from pyrogram.errors import (  # type: ignore
        ChannelPrivate,
        PeerIdInvalid,
    )

    session_name = getattr(client, "name", "client")

    if spec.kind == "invite":
        if not allow_join:
            return {
                "ok": False,
                "kind": spec.kind,
                "normalized": spec.normalized,
                "chat_id": None,
                "message_ids": spec.message_ids,
                "resolved_by": None,
                "did_join": False,
                "note": "Joining disabled",
                "error": "join_not_allowed",
            }

        join_result = await join_by_invite(client, spec.invite_link or spec.invite_hash or spec.raw)
        if not join_result.get("ok"):
            wait_seconds = join_result.get("wait_seconds")
            if join_result.get("status") == "VALID_BUT_RATE_LIMITED" and wait_seconds:
                await _sleep_for_flood(wait_seconds)
            logging.info(
                "ReportTargetResolver: failed to join invite",
                extra={"status": join_result.get("status"), "session_name": session_name},
            )
            return {
                "ok": False,
                "kind": spec.kind,
                "normalized": spec.normalized,
                "chat_id": None,
                "message_ids": spec.message_ids,
                "resolved_by": session_name,
                "did_join": False,
                "note": join_result.get("status", "join_failed"),
                "error": join_result.get("detail"),
            }

        did_join = join_result.get("status") == "JOINED"
        ok, chat, error = await _try_get_chat(client, spec)
        if not ok or not chat:
            logging.warning(
                "ReportTargetResolver: joined but could not resolve chat",
                extra={"session_name": session_name, "status": join_result.get("status")},
            )
            return {
                "ok": False,
                "kind": spec.kind,
                "normalized": spec.normalized,
                "chat_id": None,
                "message_ids": spec.message_ids,
                "resolved_by": session_name,
                "did_join": did_join,
                "note": "joined_but_unresolved",
                "error": error or "unresolved_after_join",
            }

        chat_id = _chat_id_from_chat(chat)
        logging.info(
            "ReportTargetResolver: resolved invite target",
            extra={"session_name": session_name, "chat_id": chat_id, "joined": did_join},
        )
        return {
            "ok": True,
            "kind": spec.kind,
            "normalized": spec.normalized,
            "chat_id": chat_id,
            "message_ids": spec.message_ids,
            "resolved_by": session_name,
            "did_join": did_join,
            "note": "resolved_after_join",
            "error": None,
        }

    ok, chat, error = await _try_get_chat(client, spec)
    if ok and chat:
        chat_id = _chat_id_from_chat(chat)
        logging.info(
            "ReportTargetResolver: resolved target",
            extra={"session_name": session_name, "chat_id": chat_id, "kind": spec.kind},
        )
        return {
            "ok": True,
            "kind": spec.kind,
            "normalized": spec.normalized,
            "chat_id": chat_id,
            "message_ids": spec.message_ids,
            "resolved_by": session_name,
            "did_join": False,
            "note": "resolved",
            "error": None,
        }

    if error in {"PeerIdInvalid", "ChannelPrivate"}:
        return {
            "ok": False,
            "kind": spec.kind,
            "normalized": spec.normalized,
            "chat_id": None,
            "message_ids": spec.message_ids,
            "resolved_by": session_name,
            "did_join": False,
            "note": "try_next_client",
            "error": error,
        }

    return {
        "ok": False,
        "kind": spec.kind,
        "normalized": spec.normalized,
        "chat_id": None,
        "message_ids": spec.message_ids,
        "resolved_by": session_name,
        "did_join": False,
        "note": "unresolved",
        "error": error or "unknown_error",
    }


async def resolve_report_target(
    clients: list,
    raw_target: str,
    *,
    allow_join: bool = True,
) -> dict:
    """Resolve user-supplied report targets safely.

    Usage example::

        result = await resolve_report_target(clients, raw_target)

        if not result["ok"]:
            # log and skip report
            ...
        else:
            # safe to call report API
            # chat_id = result["chat_id"]
            # message_ids = result["message_ids"]
            ...
    """

    try:
        spec = _parse_target(raw_target)
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "kind": "invalid",
            "normalized": raw_target or "",
            "chat_id": None,
            "message_ids": None,
            "resolved_by": None,
            "did_join": False,
            "note": "parse_error",
            "error": str(exc),
        }

    cached = _get_cached(spec.cache_key())
    if cached:
        return cached

    if not clients:
        result = {
            "ok": False,
            "kind": spec.kind,
            "normalized": spec.normalized,
            "chat_id": None,
            "message_ids": spec.message_ids,
            "resolved_by": None,
            "did_join": False,
            "note": "no_clients",
            "error": "no_clients_available",
        }
        _cache_result(spec.cache_key(), result, failure=True)
        return result

    invite_link = spec.invite_link or (spec.invite_hash and f"https://t.me/+{spec.invite_hash}")
    target_username = spec.username
    join_required = spec.kind in {"internal_message", "invite"}
    join_possible = bool(invite_link or target_username)

    if join_required and not join_possible:
        return {
            "ok": False,
            "kind": spec.kind,
            "normalized": spec.normalized,
            "chat_id": None,
            "message_ids": spec.message_ids,
            "resolved_by": None,
            "did_join": False,
            "note": "invite_required_for_private_link",
            "error": "Cannot auto-join from t.me/c link without invite or membership",
        }

    join_results = []
    if allow_join and (join_possible or join_required):
        for client in clients:
            join_result = await _attempt_join(
                client,
                spec,
                invite_link=invite_link,
                username=target_username,
                allow_join=allow_join,
            )
            join_results.append(join_result)
        if any(result.get("ok") for result in join_results):
            _FAILURE_CACHE.pop(spec.cache_key(), None)

    fallback_result: dict[str, Any] | None = None
    resolution_errors: dict[str, str | None] = {}

    for client in clients:
        session_name = getattr(client, "name", "client")
        ok, chat, error = await _try_get_chat(client, spec)
        if ok and chat:
            chat_id = _chat_id_from_chat(chat)
            result = {
                "ok": True,
                "kind": spec.kind,
                "normalized": spec.normalized,
                "chat_id": chat_id,
                "message_ids": spec.message_ids,
                "resolved_by": session_name,
                "did_join": any(
                    jr.get("session") == session_name and jr.get("status") in {"invite", "username", "already"}
                    for jr in join_results
                ),
                "note": "resolved",
                "error": None,
            }
            _cache_result(spec.cache_key(), result)
            logging.info(
                "ReportTargetResolver: resolved target",
                extra={"session_name": session_name, "chat_id": chat_id, "kind": spec.kind},
            )
            return result

        resolution_errors[session_name] = error

        if error in {"PeerIdInvalid", "ChannelPrivate"}:
            logging.info(
                "ReportTargetResolver: membership error, trying next client",
                extra={"session_name": session_name, "error": error},
            )
            continue

        fallback_result = {
            "ok": False,
            "kind": spec.kind,
            "normalized": spec.normalized,
            "chat_id": None,
            "message_ids": spec.message_ids,
            "resolved_by": session_name,
            "did_join": any(
                jr.get("session") == session_name and jr.get("ok") for jr in join_results
            ),
            "note": "unresolved",
            "error": error or "unknown_error",
        }
        if error == "FloodWait":
            await _sleep_for_flood(1)
        continue

    summary = ", ".join(f"{sess}:{err}" for sess, err in resolution_errors.items() if err)
    result = fallback_result or {
        "ok": False,
        "kind": spec.kind,
        "normalized": spec.normalized,
        "chat_id": None,
        "message_ids": spec.message_ids,
        "resolved_by": None,
        "did_join": any(jr.get("ok") for jr in join_results),
        "note": "all_clients_failed",
        "error": summary or "unresolved",
    }

    if result.get("error") not in {"PeerIdInvalid", "ChannelPrivate"}:
        _cache_result(spec.cache_key(), result, failure=True)
    return result


__all__ = ["resolve_report_target"]
