from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlparse

from pyrogram.errors import (
    PeerIdInvalid,
    RPCError,
    UsernameInvalid,
    UsernameNotOccupied,
)

from pyrogram import Client

from bot.dependencies import API_HASH, API_ID
from bot.link_parser import maybe_parse_join_target


def friendly_error(message: str) -> str:
    return f"⚠️ {message}\nUse the menu below or try again."


def parse_reasons(text: str) -> list[str]:
    reasons = [line.strip() for line in text.replace(";", "\n").splitlines() if line.strip()]
    return reasons[:5]


def parse_links(text: str) -> list[str]:
    links: list[str] = []
    for chunk in text.replace(";", "\n").split():
        if is_valid_link(chunk):
            links.append(chunk)
    return links[:5]


def is_valid_link(text: str) -> bool:
    return maybe_parse_join_target(text) is not None


def parse_telegram_url(url: str) -> dict:
    parsed = urlparse(url if url.startswith("http") else f"https://{url}")
    path_parts = [p for p in parsed.path.split("/") if p]

    if not parsed.netloc.endswith("t.me") or not path_parts:
        raise ValueError("Invalid Telegram URL")

    if path_parts[0] == "c" and len(path_parts) >= 3:
        if not path_parts[1].isdigit() or not path_parts[2].isdigit():
            raise ValueError("Invalid private message link")
        return {
            "type": "private_message",
            "chat_id": int(f"-100{path_parts[1]}"),
            "message_id": int(path_parts[2]),
        }

    if len(path_parts) >= 3 and path_parts[1] in {"s", "story"}:
        return {
            "type": "story",
            "username": path_parts[0],
            "story_id": path_parts[2],
        }

    if len(path_parts) >= 2:
        if not path_parts[1].isdigit():
            raise ValueError("Invalid public message link")
        return {
            "type": "public_message",
            "username": path_parts[0],
            "message_id": int(path_parts[1]),
        }

    parsed_join = maybe_parse_join_target(url)
    if parsed_join:
        if parsed_join.type == "invite":
            return {
                "type": "invite",
                "invite_link": parsed_join.normalized_url,
                "invite_hash": parsed_join.invite_hash,
            }
        return {
            "type": "username",
            "username": parsed_join.username,
            "invite_link": parsed_join.normalized_url if parsed_join.invite_hash else None,
        }

    if len(path_parts) == 1:
        return {"type": "username", "username": path_parts[0]}

    raise ValueError("Unrecognized Telegram URL format")


def normalize_target(target: str | int) -> tuple[str, dict]:
    """Normalize user-supplied target strings to a consistent username/id form.

    The function accepts plain usernames, ``@username`` mentions, ``t.me`` links,
    HTTPS ``t.me`` links, and numeric IDs. It returns the normalized identifier
    along with the parsed details so callers can decide how to resolve it.
    """
    raw = str(target).strip()

    if raw.startswith("@"):
        raw = raw[1:]

    numeric_candidate = raw.lstrip("+")
    if numeric_candidate.startswith("-100") or numeric_candidate.lstrip("-").isdigit():
        try:
            numeric_id = int(numeric_candidate)
            return str(numeric_id), {"type": "numeric_id", "id": numeric_id}
        except ValueError:
            pass

    if "t.me" in raw or raw.startswith("http"):
        try:
            details = parse_telegram_url(raw)
            normalized = details.get("username") or details.get("chat_id") or details.get("invite_link")
            return str(normalized), details
        except Exception:
            # Fall back to treating the path segment as a username
            parsed = urlparse(raw if raw.startswith("http") else f"https://{raw}")
            path = parsed.path.lstrip("/")
            if path:
                return path.split("/", maxsplit=1)[0], {"type": "username", "username": path}

    return raw, {"type": "username", "username": raw}


def extract_target_identifier(text: str) -> str:
    text = text.strip()
    if text.startswith("@"):  # username
        return text[1:]

    parsed = urlparse(text if text.startswith("http") else f"https://{text}")
    path = parsed.path.lstrip("/")
    return path.split("/", maxsplit=1)[0]


def session_strings_from_text(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


async def validate_sessions(api_id: int, api_hash: str, sessions: list[str]) -> tuple[list[str], list[str]]:
    valid: list[str] = []
    invalid: list[str] = []

    for idx, session in enumerate(sessions):
        client = Client(
            name=f"validation_{idx}", api_id=api_id, api_hash=api_hash, session_string=session, workdir=f"/tmp/validate_{idx}"
        )
        try:
            await client.start()
            valid.append(session)
        except Exception:
            invalid.append(session)
        finally:
            try:
                await client.stop()
            except Exception:
                pass

    return valid, invalid


async def resolve_chat_id(client: Client, target: str, invite_link: str | None = None):
    """Backward-compatible wrapper returning only the chat ID."""

    peer, _ = await resolve_target_peer(client, target, invite_link)

    if hasattr(peer, "user_id"):
        return int(getattr(peer, "user_id"))
    if hasattr(peer, "channel_id"):
        # Pyrogram prepends -100 to channel IDs for chat ids
        return int(f"-100{getattr(peer, 'channel_id')}")
    if hasattr(peer, "chat_id"):
        return int(getattr(peer, "chat_id"))

    return int(peer)


async def _refresh_dialogs(client: Client) -> None:
    """Refresh dialog list once per client to improve peer resolution."""

    if getattr(client, "_dialogs_refreshed", False):
        return

    try:
        async for _ in client.get_dialogs():
            pass
    finally:
        client._dialogs_refreshed = True


async def resolve_target_peer(client: Client, target: str, invite_link: str | None = None):
    """Resolve a user-supplied target into an InputPeer/User/Peer object.

    The helper normalizes common Telegram formats (``@username``, ``t.me`` links,
    numeric IDs, and message links) and retries once after refreshing dialogs.
    It raises a ``PeerIdInvalid``/``UsernameNotOccupied``/``UsernameInvalid``
    when resolution is impossible so callers can surface a friendly message.
    """

    normalized, details = normalize_target(target)
    attempts = 0
    last_exc: Exception | None = None

    while attempts < 2:
        attempts += 1
        try:
            await _refresh_dialogs(client)

            if details.get("type") == "invite":
                try:
                    chat = await client.get_chat(details["invite_link"])
                    resolved = await client.resolve_peer(chat.id)
                    return resolved, normalized
                except ValueError as exc:
                    logging.warning(
                        "ValueError resolving invite link target '%s' (normalized '%s', invite_link=%s): %s",
                        target,
                        normalized,
                        bool(invite_link),
                        exc,
                    )
                    raise PeerIdInvalid(f"Invalid invite link target: {exc}") from exc

            if details.get("type") in {"public_message", "private_message"}:
                chat_ref = details.get("username") or details.get("chat_id")
                message_id = details.get("message_id")

                try:
                    chat = await client.get_chat(chat_ref)
                except (PeerIdInvalid, UsernameNotOccupied, UsernameInvalid, ValueError) as exc:
                    logging.warning(
                        "Peer lookup failed for message link target '%s' (normalized '%s', invite_link=%s): %s",
                        target,
                        normalized,
                        bool(invite_link),
                        exc,
                    )
                    hint = "Join the channel first or provide a valid invite link for private chats."
                    raise PeerIdInvalid(hint) from exc
                except RPCError as exc:
                    logging.warning(
                        "RPC error resolving chat for message link target '%s' (normalized '%s', invite_link=%s): %s",
                        target,
                        normalized,
                        bool(invite_link),
                        exc,
                    )
                    raise PeerIdInvalid("Cannot access the chat referenced by this link.") from exc

                try:
                    await client.get_messages(chat.id, message_id)
                except (PeerIdInvalid, ValueError) as exc:
                    logging.warning(
                        "ValueError resolving message link target '%s' (normalized '%s', invite_link=%s): %s",
                        target,
                        normalized,
                        bool(invite_link),
                        exc,
                    )
                    hint = "Ensure the bot/user session is a member and the message still exists."
                    raise PeerIdInvalid(hint) from exc
                except RPCError as exc:
                    logging.warning(
                        "RPC error fetching message for target '%s' (normalized '%s', invite_link=%s): %s",
                        target,
                        normalized,
                        bool(invite_link),
                        exc,
                    )
                    raise PeerIdInvalid("Unable to fetch the message for this link.") from exc

                resolved = await client.resolve_peer(chat.id)
                return resolved, normalized

            if details.get("type") == "story":
                try:
                    chat = await client.get_chat(details["username"])
                    resolved = await client.resolve_peer(chat.id)
                    return resolved, normalized
                except ValueError as exc:
                    logging.warning(
                        "ValueError resolving story target '%s' (normalized '%s', invite_link=%s): %s",
                        target,
                        normalized,
                        bool(invite_link),
                        exc,
                    )
                    raise PeerIdInvalid(f"Invalid story target: {exc}") from exc

            if details.get("type") == "numeric_id":
                try:
                    resolved = await client.resolve_peer(details["id"])
                    return resolved, normalized
                except ValueError as exc:
                    logging.warning(
                        "ValueError resolving numeric target '%s' (normalized '%s', invite_link=%s): %s",
                        target,
                        normalized,
                        bool(invite_link),
                        exc,
                    )
                    raise PeerIdInvalid(f"Invalid numeric target: {exc}") from exc

            try:
                resolved = await client.resolve_peer(details.get("username") or normalized)
                return resolved, normalized
            except ValueError as exc:
                logging.warning(
                    "ValueError resolving username target '%s' (normalized '%s', invite_link=%s): %s",
                    target,
                    normalized,
                    bool(invite_link),
                    exc,
                )
                raise PeerIdInvalid(f"Invalid username target: {exc}") from exc

        except (PeerIdInvalid, UsernameNotOccupied, UsernameInvalid) as exc:
            last_exc = exc
            if attempts >= 2:
                raise
            # Retry after clearing dialog cache once
            client._dialogs_refreshed = False
            continue

    if last_exc:
        raise last_exc

    raise PeerIdInvalid("Unable to resolve target")


async def validate_targets(
    targets: list[str],
    sessions: list[str],
    api_id: int | None,
    api_hash: str | None,
    invite_link: str | None = None,
) -> tuple[bool, str | None]:
    if not targets:
        return False, "No targets provided for validation."

    if not sessions:
        return False, "No sessions available to validate the provided targets."

    if not (api_id and api_hash):
        api_id = API_ID
        api_hash = API_HASH

    last_error: str | None = None

    for idx, session in enumerate(sessions):
        client = Client(
            name=f"target_validator_{idx}",
            api_id=api_id,
            api_hash=api_hash,
            session_string=session,
            workdir=f"/tmp/target_validator_{idx}",
        )

        try:
            await client.start()

            for target in targets:
                try:
                    await resolve_chat_id(client, target, invite_link)
                except Exception as exc:  # noqa: BLE001 - allow detailed error messaging
                    last_error = f"The link '{target}' is not valid: {exc}."
                    raise

            return True, None
        except Exception:
            continue
        finally:
            try:
                await client.stop()
            except Exception:
                pass

    return False, last_error

__all__ = [
    "friendly_error",
    "parse_reasons",
    "parse_links",
    "is_valid_link",
    "parse_telegram_url",
    "normalize_target",
    "extract_target_identifier",
    "session_strings_from_text",
    "validate_sessions",
    "resolve_target_peer",
    "resolve_chat_id",
    "validate_targets",
]
