from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - imported lazily by workers
    from pyrogram.client import Client
    from pyrogram.errors import (
        BadRequest,
        ChannelInvalid,
        ChannelPrivate,
        ChatIdInvalid,
        FloodWait,
        PeerIdInvalid,
        RPCError,
        UsernameInvalid,
        UsernameNotOccupied,
    )


@dataclass(frozen=True)
class NormalizedPeerInput:
    """Normalized user input for peer resolution.

    ``kind`` is one of ``"numeric"``, ``"username"``, or ``"invite"`` so
    callers can short-circuit unsupported invite links before hammering the
    Telegram API.
    """

    raw: str
    normalized: str
    kind: str
    username: str | None
    numeric_id: int | None
    invite: str | None

    def cache_key(self) -> str:
        return self.normalized.lower()


@dataclass
class PeerResolutionResult:
    ok: bool
    normalized: str
    method: str | None
    peer: object | None
    peer_type: str | None
    reason: str | None
    error: str | None


@dataclass(frozen=True)
class NormalizedTarget:
    """Lightweight representation of a Telegram target extracted from a URL.

    ``kind`` captures the intent (``"username"``, ``"message"`` or ``"invite"``)
    so callers can decide whether to proceed. Unsupported kinds are still
    returned but marked ``supported=False`` so the worker can skip them
    gracefully instead of raising in a tight loop.
    """

    raw: str
    username: str | None
    kind: str
    message_id: int | None = None
    invite: str | None = None
    supported: bool = True

    def cache_key(self) -> str | None:
        return self.username.lower() if self.username else None


@dataclass
class FailureRecord:
    reason: str
    expires_at: datetime


_failure_cache: dict[str, FailureRecord] = {}
_FAILURE_TTL = timedelta(minutes=15)


def _clean_failure_cache() -> None:
    now = datetime.now(timezone.utc)
    expired = [key for key, record in _failure_cache.items() if record.expires_at <= now]
    for key in expired:
        _failure_cache.pop(key, None)


def _cache_permanent_failure(username: str | None, reason: str) -> None:
    if not username:
        return

    _failure_cache[username.lower()] = FailureRecord(
        reason=reason,
        expires_at=datetime.now(timezone.utc) + _FAILURE_TTL,
    )


def normalize_input(raw_input: str) -> NormalizedPeerInput:
    """Normalize user-supplied peer references.

    Supported inputs:
    - ``https://t.me/<username>`` or ``t.me/<username>``
    - ``@username`` or plain ``username``
    - Numeric IDs (positive/negative)
    - ``t.me/+invite``/``t.me/joinchat/<hash>`` (marked as unsupported)
    """

    from urllib.parse import urlparse

    raw = (raw_input or "").strip()
    compact = raw.replace(" ", "")

    # Numeric IDs are handled first because ``t.me/-100...`` is uncommon
    numeric_candidate = compact.lstrip("+")
    if numeric_candidate.lstrip("-").isdigit():
        numeric_id = int(numeric_candidate)
        return NormalizedPeerInput(
            raw=raw,
            normalized=str(numeric_id),
            kind="numeric",
            username=None,
            numeric_id=numeric_id,
            invite=None,
        )

    parsed = urlparse(compact if compact.startswith("http") else f"https://{compact}")
    path_parts = [part for part in parsed.path.split("/") if part]
    netloc = parsed.netloc.lower()

    if netloc.endswith("t.me") and path_parts:
        if path_parts[0].startswith("+"):
            return NormalizedPeerInput(
                raw=raw,
                normalized=path_parts[0],
                kind="invite",
                username=None,
                numeric_id=None,
                invite=path_parts[0],
            )

        if path_parts[0].lower() == "joinchat" and len(path_parts) >= 2:
            return NormalizedPeerInput(
                raw=raw,
                normalized=path_parts[1],
                kind="invite",
                username=None,
                numeric_id=None,
                invite=path_parts[1],
            )

        username = path_parts[0].lstrip("@")
        return NormalizedPeerInput(
            raw=raw,
            normalized=username,
            kind="username",
            username=username,
            numeric_id=None,
            invite=None,
        )

    username = compact.lstrip("@") or None
    return NormalizedPeerInput(
        raw=raw,
        normalized=username or compact,
        kind="username",
        username=username,
        numeric_id=None,
        invite=None,
    )


def _peer_type_label(peer: object | None) -> str | None:
    peer_type = getattr(peer, "type", None)
    if peer_type:
        return str(peer_type)

    if peer and peer.__class__.__name__.lower() == "user":
        return "user"

    return None


def _peer_to_chat_id(peer: object) -> int:
    if hasattr(peer, "id"):
        return int(getattr(peer, "id"))
    if hasattr(peer, "chat_id"):
        return int(getattr(peer, "chat_id"))
    if hasattr(peer, "channel_id"):
        return int(f"-100{getattr(peer, 'channel_id')}")
    if hasattr(peer, "user_id"):
        return int(getattr(peer, "user_id"))
    return int(peer)


def normalize_telegram_target(url: str) -> NormalizedTarget:
    """Parse Telegram URLs/usernames into a normalized target.

    The function accepts bare usernames (``foo`` or ``@foo``), public profile
    links (``https://t.me/foo``), message links (``https://t.me/foo/123``), and
    invite links (``https://t.me/+abc`` or ``https://t.me/joinchat/abc``). Invite
    links are returned as ``supported=False`` because reporting requires a
    resolvable username/peer.
    """

    from urllib.parse import urlparse

    raw = url.strip()
    parsed = urlparse(raw if raw.startswith("http") else f"https://{raw}")
    path_parts = [part for part in parsed.path.split("/") if part]
    netloc = parsed.netloc.lower()

    if not netloc.endswith("t.me") or not path_parts:
        username = raw.lstrip("@") or None
        return NormalizedTarget(raw=raw, username=username, kind="username")

    # Join-chat/invite links
    if path_parts[0].startswith("+"):
        return NormalizedTarget(raw=raw, username=None, kind="invite", invite=path_parts[0], supported=False)

    if path_parts[0].lower() == "joinchat" and len(path_parts) >= 2:
        return NormalizedTarget(raw=raw, username=None, kind="invite", invite=path_parts[1], supported=False)

    # Message links – we care about the username for reporting, not the message id
    if len(path_parts) >= 2 and path_parts[1].isdigit():
        return NormalizedTarget(
            raw=raw,
            username=path_parts[0].lstrip("@"),
            kind="message",
            message_id=int(path_parts[1]),
        )

    username = path_parts[0].lstrip("@")
    return NormalizedTarget(raw=raw, username=username, kind="username")


async def resolve_peer(
    client: "Client",
    raw_input: str,
    *,
    max_attempts: int = 2,
    flood_wait_cap: int = 8,
) -> PeerResolutionResult:
    """Resolve a Telegram peer robustly with structured output.

    The function:
    * Normalizes input (usernames/links/IDs)
    * Tries numeric IDs via ``get_chat`` first
    * For usernames, tries ``get_users`` then ``get_chat``
    * Treats invite links as unsupported (caller must join first)
    * Caches invalid peers temporarily to avoid noisy retries
    """

    from pyrogram.errors import (
        BadRequest,
        ChannelInvalid,
        ChannelPrivate,
        ChatIdInvalid,
        FloodWait,
        PeerIdInvalid,
        RPCError,
        UsernameInvalid,
        UsernameNotOccupied,
    )

    normalized = normalize_input(raw_input)
    cache_key = normalized.cache_key()
    _clean_failure_cache()

    if normalized.kind == "invite":
        reason = "invite_link_requires_join"
        _cache_permanent_failure(cache_key, reason)
        logging.info(
            "Peer resolution skipped for invite link %s (normalized %s)",
            raw_input,
            normalized.normalized,
        )
        return PeerResolutionResult(
            ok=False,
            normalized=normalized.normalized,
            method=None,
            peer=None,
            peer_type=None,
            reason=reason,
            error="Invite links require joining before they can be resolved.",
        )

    cached = _failure_cache.get(cache_key)
    if cached and cached.expires_at > datetime.now(timezone.utc):
        logging.info(
            "Using cached peer failure for %s (normalized %s): %s",
            raw_input,
            normalized.normalized,
            cached.reason,
        )
        return PeerResolutionResult(
            ok=False,
            normalized=normalized.normalized,
            method=None,
            peer=None,
            peer_type=None,
            reason=cached.reason,
            error=None,
        )

    attempts = 0
    last_error: str | None = None

    while attempts < max_attempts:
        attempts += 1
        method: str | None = None

        try:
            if normalized.numeric_id is not None:
                method = "get_chat:numeric"
                peer = await client.get_chat(normalized.numeric_id)
            else:
                method = "get_users"
                peer = await client.get_users(normalized.username)
            peer_type = _peer_type_label(peer)
            logging.info(
                "Resolved %s (normalized %s) via %s using %s as %s",
                raw_input,
                normalized.normalized,
                getattr(client, "name", "client"),
                method,
                peer_type,
            )
            return PeerResolutionResult(
                ok=True,
                normalized=normalized.normalized,
                method=method,
                peer=peer,
                peer_type=peer_type,
                reason=None,
                error=None,
            )
        except (UsernameNotOccupied, UsernameInvalid, PeerIdInvalid) as exc:
            last_error = exc.__class__.__name__
            if normalized.numeric_id is None and method == "get_users":
                try:
                    method = "get_chat:username"
                    peer = await client.get_chat(normalized.username)
                    peer_type = _peer_type_label(peer)
                    logging.info(
                        "Resolved %s (normalized %s) via %s using %s as %s",
                        raw_input,
                        normalized.normalized,
                        getattr(client, "name", "client"),
                        method,
                        peer_type,
                    )
                    return PeerResolutionResult(
                        ok=True,
                        normalized=normalized.normalized,
                        method=method,
                        peer=peer,
                        peer_type=peer_type,
                        reason=None,
                        error=None,
                    )
                except (UsernameNotOccupied, UsernameInvalid, PeerIdInvalid) as inner_exc:
                    last_error = inner_exc.__class__.__name__
                    _cache_permanent_failure(cache_key, last_error)
                    logging.warning(
                        "Permanent peer failure for %s (normalized %s) via %s using %s: %s",
                        raw_input,
                        normalized.normalized,
                        getattr(client, "name", "client"),
                        method,
                        inner_exc,
                    )
                    return PeerResolutionResult(
                        ok=False,
                        normalized=normalized.normalized,
                        method=method,
                        peer=None,
                        peer_type=None,
                        reason=last_error,
                        error=str(inner_exc),
                    )
        except FloodWait as exc:
            wait_seconds = min(getattr(exc, "value", 1), flood_wait_cap)
            last_error = f"FloodWait:{wait_seconds}s"
            logging.warning(
                "Flood wait resolving %s (normalized %s) via %s using %s; sleeping %ss (attempt %s/%s)",
                raw_input,
                normalized.normalized,
                getattr(client, "name", "client"),
                method or "unknown",
                wait_seconds,
                attempts,
                max_attempts,
            )
            await asyncio.sleep(wait_seconds)
            continue
        except (ChannelInvalid, ChannelPrivate, ChatIdInvalid, PeerIdInvalid, UsernameInvalid, UsernameNotOccupied) as exc:
            last_error = exc.__class__.__name__
            _cache_permanent_failure(cache_key, last_error)
            logging.warning(
                "Peer resolution failed for %s (normalized %s) via %s using %s: %s",
                raw_input,
                normalized.normalized,
                getattr(client, "name", "client"),
                method or "unknown",
                exc,
            )
            return PeerResolutionResult(
                ok=False,
                normalized=normalized.normalized,
                method=method,
                peer=None,
                peer_type=None,
                reason=last_error,
                error=str(exc),
            )
        except (asyncio.TimeoutError, OSError, ConnectionError) as exc:
            last_error = exc.__class__.__name__
            logging.warning(
                "Network issue resolving %s (normalized %s) via %s using %s: %s",
                raw_input,
                normalized.normalized,
                getattr(client, "name", "client"),
                method or "unknown",
                exc,
            )
            await asyncio.sleep(min(3, flood_wait_cap))
            continue
        except BadRequest as exc:
            last_error = exc.__class__.__name__
            _cache_permanent_failure(cache_key, last_error)
            logging.warning(
                "Bad request resolving %s (normalized %s) via %s using %s: %s",
                raw_input,
                normalized.normalized,
                getattr(client, "name", "client"),
                method or "unknown",
                exc,
            )
            return PeerResolutionResult(
                ok=False,
                normalized=normalized.normalized,
                method=method,
                peer=None,
                peer_type=None,
                reason=last_error,
                error=str(exc),
            )
        except RPCError as exc:
            last_error = exc.__class__.__name__
            backoff = min(flood_wait_cap, 1 + attempts)
            logging.warning(
                "RPC error resolving %s (normalized %s) via %s using %s: %s (attempt %s/%s)",
                raw_input,
                normalized.normalized,
                getattr(client, "name", "client"),
                method or "unknown",
                exc,
                attempts,
                max_attempts,
            )
            await asyncio.sleep(backoff)
            continue

    if cache_key and last_error:
        _failure_cache[cache_key] = FailureRecord(
            reason=last_error,
            expires_at=datetime.now(timezone.utc) + _FAILURE_TTL,
        )

    return PeerResolutionResult(
        ok=False,
        normalized=normalized.normalized,
        method=None,
        peer=None,
        peer_type=None,
        reason=last_error or "max_attempts_exceeded",
        error=None,
    )


async def resolve_chat(
    client: "Client",
    target: NormalizedTarget,
    *,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
) -> int | None:
    """Resolve a normalized target to a chat id with sensible retries.

    Permanent failures are cached for ``_FAILURE_TTL`` to avoid endless retries/log spam.
    """

    cache_key = target.cache_key()
    _clean_failure_cache()

    if not target.supported:
        logging.info("Skipping unsupported target %s (%s)", target.raw, target.kind)
        return None

    if target.kind == "username" and target.message_id is None and target.invite is None:
        normalized = normalize_input(target.username or target.raw)
        cached = _failure_cache.get(normalized.cache_key())
        if cached and cached.expires_at > datetime.now(timezone.utc):
            logging.info("Skipping cached username failure for %s: %s", normalized.normalized, cached.reason)
            return None

        result = await resolve_peer(client, normalized.raw, max_attempts=min(2, max_attempts))
        if result.ok and result.peer is not None:
            return _peer_to_chat_id(result.peer)

        if result.reason:
            _cache_permanent_failure(normalized.cache_key(), result.reason)
        return None

    if cache_key and cache_key in _failure_cache:
        record = _failure_cache[cache_key]
        if record.expires_at > datetime.now(timezone.utc):
            logging.info("Skipping cached failure for %s: %s", cache_key, record.reason)
            return None

    attempts = 0
    last_reason: str | None = None

    while attempts < max_attempts:
        attempts += 1
        try:
            chat = await client.get_chat(target.username)
            resolved = await client.resolve_peer(chat.id)
            if hasattr(resolved, "user_id"):
                return int(getattr(resolved, "user_id"))
            if hasattr(resolved, "channel_id"):
                return int(f"-100{getattr(resolved, 'channel_id')}")
            if hasattr(resolved, "chat_id"):
                return int(getattr(resolved, "chat_id"))
            return int(resolved)
        except Exception as exc:  # noqa: BLE001 - deliberate branching
            from pyrogram.errors import (
                BadRequest,
                ChannelPrivate,
                FloodWait,
                PeerIdInvalid,
                RPCError,
                UsernameInvalid,
                UsernameNotOccupied,
            )

            if isinstance(exc, FloodWait):
                wait_seconds = min(getattr(exc, "value", 1), 60)
                logging.warning(
                    "Flood wait resolving %s via %s (attempt %s/%s): %ss",
                    target.raw,
                    getattr(client, "name", "client"),
                    attempts,
                    max_attempts,
                    wait_seconds,
                )
                await asyncio.sleep(wait_seconds)
                continue

            if isinstance(exc, (PeerIdInvalid, UsernameNotOccupied, UsernameInvalid, ChannelPrivate)):
                last_reason = exc.__class__.__name__
                _cache_permanent_failure(cache_key or target.username, last_reason)
                logging.warning(
                    "Permanent peer failure for %s via %s: %s",
                    target.raw,
                    getattr(client, "name", "client"),
                    exc,
                )
                return None

            if isinstance(exc, BadRequest):
                last_reason = exc.__class__.__name__
                _cache_permanent_failure(cache_key or target.username, last_reason)
                logging.warning("Bad request resolving %s: %s", target.raw, exc)
                return None

            if isinstance(exc, RPCError):
                backoff = min(max_delay, base_delay * (2 ** (attempts - 1)))
                jitter = random.uniform(0, backoff / 2)
                delay = backoff + jitter
                logging.warning(
                    "Transient resolution failure for %s via %s (attempt %s/%s): %s; retrying in %.1fs",
                    target.raw,
                    getattr(client, "name", "client"),
                    attempts,
                    max_attempts,
                    exc.__class__.__name__,
                    delay,
                )
                await asyncio.sleep(delay)
                continue

            # Network/Value errors are treated as permanent to avoid log spam
            last_reason = exc.__class__.__name__
            _cache_permanent_failure(cache_key or target.username, last_reason)
            logging.warning(
                "Unrecoverable error resolving %s via %s: %s", target.raw, getattr(client, "name", "client"), exc
            )
            return None

    if cache_key and last_reason:
        _failure_cache[cache_key] = FailureRecord(
            reason=last_reason,
            expires_at=datetime.now(timezone.utc) + _FAILURE_TTL,
        )
    return None


async def report_target(
    clients: list["Client"],
    target_url: str,
    *,
    invite_link: str | None = None,
) -> tuple[int | None, NormalizedPeerInput]:
    """Normalize and resolve a target across multiple clients.

    Returns a tuple of (chat_id, NormalizedPeerInput). chat_id is ``None`` when
    the target cannot be resolved or is unsupported.
    """

    normalized = normalize_input(target_url)

    if normalized.kind == "invite":
        logging.info("Invite link provided for %s; requires join first.", target_url)
        _cache_permanent_failure(normalized.cache_key(), "invite_link")
        return None, normalized

    if invite_link and normalized.invite is None and normalized.kind != "invite":
        logging.info("Invite link supplied alongside %s; proceeding with username/numeric resolution.", target_url)

    for client in clients:
        result = await resolve_peer(client, normalized.raw, max_attempts=2)
        if result.ok and result.peer is not None:
            chat_id = _peer_to_chat_id(result.peer)
            return chat_id, normalized

        logging.info(
            "Peer resolution failed via %s for %s (normalized %s): %s",
            getattr(client, "name", "client"),
            normalized.raw,
            normalized.normalized,
            result.reason,
        )

    logging.info("All clients failed to resolve target %s", normalized.raw)
    return None, normalized


__all__ = [
    "NormalizedTarget",
    "NormalizedPeerInput",
    "PeerResolutionResult",
    "normalize_input",
    "normalize_telegram_target",
    "resolve_peer",
    "resolve_chat",
    "report_target",
]
