from __future__ import annotations

from typing import Any


def map_pyrogram_error(exc: Exception | None) -> tuple[str, str, int | None]:
    """Translate Pyrogram exceptions into user-facing codes and details."""

    if exc is None:
        return "UNKNOWN_ERROR", "unknown", None

    detail = str(exc)
    retry_after = getattr(exc, "value", None) if hasattr(exc, "value") else None

    try:
        from pyrogram.errors import (
            FloodWait,
            InviteHashExpired,
            InviteHashInvalid,
            UserAlreadyParticipant,
            ChannelPrivate,
            ChatAdminRequired,
            MessageIdInvalid,
        )
    except Exception:  # pragma: no cover - defensive import
        FloodWait = InviteHashExpired = InviteHashInvalid = UserAlreadyParticipant = ChannelPrivate = ChatAdminRequired = MessageIdInvalid = type("Dummy", (), {})  # type: ignore

    if isinstance(exc, FloodWait) or exc.__class__.__name__ == "FloodWait":
        return "FLOOD_WAIT", detail, int(getattr(exc, "value", retry_after) or 0)
    if isinstance(exc, InviteHashExpired):
        return "INVITE_EXPIRED", detail, None
    if isinstance(exc, InviteHashInvalid):
        return "INVITE_INVALID_HASH", detail, None
    if isinstance(exc, UserAlreadyParticipant):
        return "ALREADY_MEMBER", detail, None
    if isinstance(exc, ChannelPrivate):
        return "NO_ACCESS_OR_NOT_JOINED", detail, None
    if isinstance(exc, ChatAdminRequired):
        return "ADMIN_REQUIRED", detail, None
    if isinstance(exc, MessageIdInvalid):
        return "MESSAGE_ID_INVALID", detail, None

    if getattr(exc, "MESSAGE_NOT_FOUND", False):
        return "MESSAGE_NOT_FOUND", detail, None

    return "UNKNOWN_ERROR", f"{exc.__class__.__name__}: {detail}", None


__all__ = ["map_pyrogram_error"]
