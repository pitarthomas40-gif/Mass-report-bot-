from __future__ import annotations

"""Utilities for sending logs and errors to the configured logs group."""

import traceback

from pyrogram import Client
from pyrogram.errors import RPCError


async def send_log(client: Client, chat_id: int | None, text: str, *, parse_mode: str | None = None) -> None:
    """Send a log message safely."""

    if not chat_id:
        return
    try:
        await client.send_message(chat_id, text, parse_mode=parse_mode)
    except Exception:
        # Avoid crashing the bot on log errors
        pass


async def log_user_start(client: Client, logs_group: int | None, message) -> None:
    """Log whenever any user starts the bot."""

    if not logs_group or not message.from_user:
        return
    text = (
        "📥 New user started the bot\n"
        f"👤 {message.from_user.first_name}\n"
        f"🆔 ID: {message.from_user.id}"
    )
    await send_log(client, logs_group, text, parse_mode="markdown")


async def log_report_summary(
    client: Client,
    logs_group: int | None,
    *,
    user,
    target: str,
    elapsed: float,
    success: bool,
) -> None:
    """Send a summary entry after a report completes."""

    username = getattr(user, "username", None)
    user_label = f"@{username}" if username else getattr(user, "first_name", None) or "User"
    duration = round(elapsed, 2)
    status_label = "Success" if success else "Fail"
    status_prefix = "✅" if success else "❌"
    text = (
        "✅ Report Completed\n"
        f"👤 User: {user_label} ({getattr(user, 'id', 'n/a')})\n"
        f"🔗 Target: {target}\n"
        f"⏱ Time taken: {duration}s\n"
        f"{status_prefix} Status: {status_label}"
    )
    await send_log(client, logs_group, text, parse_mode="markdown")


async def log_error(client: Client, logs_group: int | None, exc: Exception, owner_id: int | None = None) -> None:
    """Send an error trace to the logs group, tagging the owner when known."""

    if not logs_group:
        return
    mention = f"[Owner](tg://user?id={owner_id})" if owner_id else "Owner"
    text = f"⚠️ Error Detected\n{mention}\n``{traceback.format_exc()}``"
    try:
        await client.send_message(logs_group, text, parse_mode="markdown")
    except RPCError:
        pass

