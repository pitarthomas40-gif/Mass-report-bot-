from __future__ import annotations

import asyncio
import logging
import random
from typing import Any
from urllib.parse import urlparse

from pyrogram.errors import (  # type: ignore
    ChatAdminRequired,
    ChannelPrivate,
    FloodWait,
    InviteHashExpired,
    InviteHashInvalid,
    PeerFlood,
    RPCError,
    UserAlreadyParticipant,
)

try:  # Pyrogram may not expose a TooManyRequests error in all versions
    from pyrogram.errors import TooManyRequests  # type: ignore
except Exception:  # pragma: no cover - fallback for older Pyrogram versions
    class TooManyRequests(PeerFlood):  # type: ignore
        """Fallback TooManyRequests error type for compatibility."""

        def __init__(self, *args: Any, value: int | None = None, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.value = value


MAX_RETRIES = 3
_JITTER_RANGE = (0.5, 1.5)


def _extract_invite_hash(invite_link: str) -> str | None:
    """Extract the invite hash from supported Telegram invite formats."""

    if not invite_link:
        return None

    trimmed = invite_link.strip()
    if trimmed.startswith("+"):
        return trimmed.lstrip("+") or None

    parsed = urlparse(trimmed if trimmed.startswith("http") else f"https://{trimmed}")
    path_parts = [p for p in parsed.path.split("/") if p]
    if not parsed.netloc or not path_parts:
        return None

    first = path_parts[0]
    if first.startswith("+"):
        return first.lstrip("+") or None

    if first.lower() == "joinchat" and len(path_parts) >= 2:
        return path_parts[1] or None

    return None


async def join_by_invite(client: Any, invite_link: str) -> dict[str, Any]:
    """Join a chat via an invite link with robust error handling.

    Returns a dict with keys ``ok``, ``status``, ``detail``, and ``wait_seconds``.
    """

    invite_hash = _extract_invite_hash(invite_link)
    if not invite_hash:
        logging.info("INVALID_LINK: could not parse invite link %s", invite_link)
        return {"ok": False, "status": "INVALID_LINK", "detail": "Unrecognized invite link", "wait_seconds": None}

    join_target = f"https://t.me/+{invite_hash}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await client.join_chat(join_target)
            logging.info(
                "SUCCESS: joined invite %s (attempt %s)",
                join_target,
                attempt,
                extra={"invite_link": join_target, "session_name": getattr(client, "name", "client"), "step": "join_invite"},
            )
            return {"ok": True, "status": "JOINED", "detail": "joined", "wait_seconds": None}
        except UserAlreadyParticipant:
            logging.info(
                "ALREADY_JOINED: %s", join_target, extra={"invite_link": join_target, "session_name": getattr(client, "name", "client"), "step": "join_invite"}
            )
            return {"ok": True, "status": "ALREADY_JOINED", "detail": "already_participant", "wait_seconds": None}
        except FloodWait as exc:
            wait_seconds = int(getattr(exc, "value", 0) or 0)
            jitter = random.uniform(*_JITTER_RANGE)
            logging.warning(
                "FLOOD_WAIT: %ss while joining %s (attempt %s/%s)",
                wait_seconds,
                join_target,
                attempt,
                MAX_RETRIES,
                extra={"invite_link": join_target, "session_name": getattr(client, "name", "client"), "step": "join_invite"},
            )
            if attempt >= MAX_RETRIES:
                return {
                    "ok": False,
                    "status": "VALID_BUT_RATE_LIMITED",
                    "detail": str(exc),
                    "wait_seconds": wait_seconds,
                }
            await asyncio.sleep(wait_seconds + jitter)
        except (PeerFlood, TooManyRequests) as exc:
            wait_seconds = int(getattr(exc, "value", 0) or 0)
            logging.warning(
                "%s: rate limited while joining %s",
                exc.__class__.__name__,
                join_target,
                extra={"invite_link": join_target, "session_name": getattr(client, "name", "client"), "step": "join_invite"},
            )
            return {
                "ok": False,
                "status": "VALID_BUT_RATE_LIMITED",
                "detail": str(exc),
                "wait_seconds": wait_seconds or None,
            }
        except (InviteHashInvalid, InviteHashExpired) as exc:
            logging.info(
                "INVALID_LINK: %s", join_target, extra={"invite_link": join_target, "session_name": getattr(client, "name", "client"), "step": "join_invite"}
            )
            return {"ok": False, "status": "INVALID_LINK", "detail": str(exc), "wait_seconds": None}
        except (ChannelPrivate, ChatAdminRequired) as exc:
            logging.info(
                "PRIVATE: %s", join_target, extra={"invite_link": join_target, "session_name": getattr(client, "name", "client"), "step": "join_invite"}
            )
            return {"ok": False, "status": "NO_ACCESS_OR_PRIVATE", "detail": str(exc), "wait_seconds": None}
        except RPCError as exc:
            logging.exception(
                "UNKNOWN RPC error joining %s", join_target, extra={"invite_link": join_target, "session_name": getattr(client, "name", "client"), "step": "join_invite"}
            )
            return {"ok": False, "status": "UNKNOWN_ERROR", "detail": str(exc), "wait_seconds": None}
        except Exception as exc:  # noqa: BLE001
            logging.exception(
                "UNKNOWN exception joining %s", join_target, extra={"invite_link": join_target, "session_name": getattr(client, "name", "client"), "step": "join_invite"}
            )
            return {"ok": False, "status": "UNKNOWN_ERROR", "detail": str(exc), "wait_seconds": None}

    return {"ok": False, "status": "UNKNOWN_ERROR", "detail": "exhausted attempts", "wait_seconds": None}
