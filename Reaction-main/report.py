"""Reporting helpers built on top of :class:`pyrogram.Client`.

The module adds a small `send_report` helper so the bot can call the raw
MTProto ``messages.Report`` RPC with clean ergonomics. The functions here keep
networking concerns centralized: concurrency, retries, and basic resilience are
handled by higher-level flows in ``main.py``.
"""
from __future__ import annotations

import asyncio
from typing import Iterable, Sequence

from pyrogram import Client
from pyrogram.errors import BadRequest, FloodWait, MessageIdInvalid, RPCError
from pyrogram.raw.types import (
    InputReportReasonChildAbuse,
    InputReportReasonCopyright,
    InputReportReasonFake,
    InputReportReasonGeoIrrelevant,
    InputReportReasonIllegalDrugs,
    InputReportReasonOther,
    InputReportReasonPersonalDetails,
    InputReportReasonPornography,
    InputReportReasonSpam,
    InputReportReasonViolence,
)


def _build_reason(reason: int | object, message: str) -> object:
    """Return a Pyrogram InputReportReason object for the given code or instance."""

    reason_map = {
        0: InputReportReasonSpam,
        1: InputReportReasonViolence,
        2: InputReportReasonPornography,
        3: InputReportReasonChildAbuse,
        4: InputReportReasonCopyright,
        5: InputReportReasonGeoIrrelevant,
        6: InputReportReasonFake,
        7: InputReportReasonIllegalDrugs,
        8: InputReportReasonPersonalDetails,
        9: InputReportReasonOther,
    }

    if hasattr(reason, "write"):
        return reason

    try:
        reason_int = int(reason)
    except Exception:
        return InputReportReasonOther()

    reason_cls = reason_map.get(reason_int, InputReportReasonOther)
    if reason_cls is InputReportReasonOther:
        return reason_cls()

    return reason_cls()


async def _resolve_peer_for_report(client: Client, chat_id):
    """Resolve usernames or numeric ids into raw peers for reporting.

    Pyrogram's async helper resolves numeric IDs easily but can return
    ``UsernameInvalid`` for plain strings if they are not cached. To keep the
    reporting surface robust we fall back to ``contacts.ResolveUsername`` and
    build the appropriate ``InputPeer*`` instance manually.
    """

    from pyrogram.errors import UsernameInvalid
    from pyrogram.raw.functions.contacts import ResolveUsername
    from pyrogram.raw.types import InputPeerChannel, InputPeerChat, InputPeerUser

    if hasattr(chat_id, "write"):
        return chat_id

    # Allow numeric strings so callers can pass ``-100…`` chat IDs directly
    # without hitting the username resolution fallback below.
    if isinstance(chat_id, str) and chat_id.lstrip("-+").isdigit():
        chat_id = int(chat_id)

    try:
        peer = client.resolve_peer(chat_id) if hasattr(client, "resolve_peer") else chat_id
        return await peer if asyncio.iscoroutine(peer) else peer
    except UsernameInvalid:
        # Continue to raw username resolution below
        pass
    except ValueError as exc:
        # Pyrogram raises ``ValueError(Peer id invalid: ...)`` for bad numeric inputs.
        # Surface a consistent API error so callers can show a friendly message.
        raise BadRequest(f"Invalid target for reporting: {exc}") from exc
    except Exception:
        # For non-username inputs we want to bubble up the original error
        # instead of masking it with a generic "Unable to resolve" message.
        raise

    username = str(chat_id).lstrip("@")
    try:
        resolved = await client.invoke(ResolveUsername(username=username))
        if resolved.users:
            user = resolved.users[0]
            return InputPeerUser(user_id=user.id, access_hash=user.access_hash)
        if resolved.chats:
            chat = resolved.chats[0]
            if hasattr(chat, "access_hash"):
                return InputPeerChannel(channel_id=chat.id, access_hash=chat.access_hash)
            return InputPeerChat(chat_id=chat.id)
    except Exception:
        pass

    raise BadRequest("Unable to resolve the target for reporting.")


async def send_report(client: Client, chat_id, message_id: int, reason: int | object, reason_text: str) -> bool:
    """Send a report against a specific message."""
    try:
        reason_obj = _build_reason(reason, reason_text)
        await client.send_report(chat_id=chat_id, message_id=message_id, reason=reason_obj, message=reason_text)
        return True

    except MessageIdInvalid:
        print(
            f"[{getattr(client, 'name', 'unknown')}] Message ID {message_id} is invalid or deleted. Skipping this message."
        )
        return True
    except (FloodWait, BadRequest, RPCError):
        raise
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"Report API Error (Session {getattr(client, 'name', 'unknown')}): {exc}")
        return False


async def report_profile_photo(client: Client, entity_id, reason: int | object, reason_text: str) -> bool:
    """Report a user profile, chat, or generic entity."""

    try:
        reason_obj = _build_reason(reason, reason_text)
        await client.send_report(chat_id=entity_id, message_id=None, reason=reason_obj, message=reason_text)
        return True

    except (FloodWait, BadRequest, RPCError):
        raise
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"Profile/Chat Report API Error (Session {getattr(client, 'name', 'unknown')}): {exc}")
        return False


async def bulk_report_messages(
    clients: Sequence[Client],
    chat_id,
    message_ids: Iterable[int],
    reason: int,
    reason_text: str,
    *,
    concurrency: int = 5,
    retry_on_flood: bool = True,
) -> dict[str, int]:
    """Report multiple messages using multiple client sessions."""

    semaphore = asyncio.Semaphore(max(concurrency, 1))

    async def _report_single(client: Client, message_id: int) -> str:
        async with semaphore:
            try:
                ok = await send_report(client, chat_id, message_id, reason, reason_text)
                return "success" if ok else "failed"
            except FloodWait as fw:
                if not retry_on_flood:
                    print(
                        f"[{getattr(client, 'name', 'unknown')}] Flood wait {fw.value}s for message {message_id}. Skipping."
                    )
                    return "failed"

                sleep_for = getattr(fw, "value", 1)
                print(
                    f"[{getattr(client, 'name', 'unknown')}] Flood wait {sleep_for}s for message {message_id}. Retrying once."
                )
                await asyncio.sleep(sleep_for)

                try:
                    ok = await send_report(client, chat_id, message_id, reason, reason_text)
                    return "success" if ok else "failed"
                except Exception as exc:  # pragma: no cover - defensive logging
                    print(
                        f"[{getattr(client, 'name', 'unknown')}] Retry failed for message {message_id}: {exc}"
                    )
                    return "failed"

            except BadRequest as exc:
                print(
                    f"[{getattr(client, 'name', 'unknown')}] Bad request while reporting {message_id}: {exc}"
                )
                return "failed"
            except RPCError as exc:
                print(f"[{getattr(client, 'name', 'unknown')}] RPC error for {message_id}: {exc}")
                return "failed"

    tasks = [
        asyncio.create_task(_report_single(client, int(message_id)))
        for client in clients
        for message_id in message_ids
    ]

    summary = {"success": 0, "failed": 0}
    if not tasks:
        return summary

    for result in await asyncio.gather(*tasks):
        summary[result] += 1

    return summary


if not hasattr(Client, "send_report"):
    # Lazy imports so users without reporting needs avoid pulling raw types prematurely.
    from pyrogram.raw.functions.messages import Report

    async def _client_send_report(
        self,
        chat_id,
        message_id: int | None = None,
        reason: int | object = 0,
        message: str = "",
    ) -> None:
        """High-level wrapper for the raw ``messages.Report`` call."""

        try:
            peer = await _resolve_peer_for_report(self, chat_id)

            if not hasattr(peer, "write"):
                raise BadRequest("Unable to resolve the target for reporting.")

            reason_obj = _build_reason(reason, message)

            ids = [int(message_id)] if message_id is not None else []

            await self.invoke(Report(peer=peer, id=ids, reason=reason_obj, message=message or ""))

        except MessageIdInvalid:
            raise
        except (FloodWait, BadRequest, RPCError):
            raise

    setattr(Client, "send_report", _client_send_report)
