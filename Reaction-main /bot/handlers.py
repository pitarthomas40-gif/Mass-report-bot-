from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
import sys
import time
from copy import deepcopy
from html import escape

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import CallbackQueryHandler, CommandHandler, ContextTypes, ConversationHandler, MessageHandler, filters

import config
from bot.constants import (
    ADD_SESSIONS,
    API_HASH_STATE,
    API_ID_STATE,
    DEFAULT_REPORTS,
    MAX_REPORTS,
    MAX_SESSIONS,
    MIN_REPORTS,
    MIN_SESSIONS,
    PRIVATE_INVITE,
    PRIVATE_MESSAGE,
    PUBLIC_MESSAGE,
    REPORT_COUNT,
    REPORT_MESSAGE,
    REPORT_REASON_TYPE,
    REPORT_SESSIONS,
    REPORT_URLS,
    REASON_LABELS,
    SESSION_MODE,
    SESSION_PICK,
    STORY_URL,
    TARGET_KIND,
)
from bot.dependencies import API_HASH, API_ID, data_store
from bot.link_parser import maybe_parse_message_link, parse_access_link
from bot.target_resolver import (
    TargetDetails,
    ensure_join_if_needed,
    fetch_target_details,
    parse_target,
    resolve_entity,
)
from bot.health import format_duration, process_health
from bot.progress_ui import run_progress_animation
from bot.scheduler import SchedulerManager
from bot.reporting import run_report_job
from bot.state import (
    active_session_count,
    clear_report_state,
    flow_state,
    get_session_order,
    manage_selection,
    pop_view,
    profile_state,
    report_selection,
    reset_flow_state,
    reset_user_context,
    saved_session_count,
    set_session_order,
    set_view,
    ui_state,
)
from bot.ui import (
    add_restart_button,
    main_menu_keyboard,
    reason_keyboard,
    render_card,
    render_greeting,
    session_mode_keyboard,
    target_kind_keyboard,
    navigation_keyboard,
)
from bot.utils import (
    friendly_error,
    is_valid_link,
    parse_links,
    parse_reasons,
    parse_telegram_url,
    session_strings_from_text,
    validate_sessions,
)
from sudo import is_owner

HELP_MESSAGE = (
    "ℹ️ *How to use the reporter*\n"
    "1) Run /report or tap Start report.\n"
    "2) Provide your API ID and API Hash.\n"
    "3) Add 1-500 Pyrogram session strings (or type 'use saved').\n"
    "4) Pick what you are reporting (private group, public group/channel, or profile/story).\n"
    "5) Send up to 5 Telegram URLs, choose a report type, and write a short reason.\n"
    "6) Choose 500-7000 report attempts (default 5000).\n"
    "I will show successes, failures, time taken, and stop automatically if the content disappears."
)


def _main_menu_markup(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    profile = profile_state(context)
    saved = len(profile.get("saved_sessions", []))
    return main_menu_keyboard(saved, active_session_count(context))


async def _render_saved_summary(
    update: Update, context: ContextTypes.DEFAULT_TYPE, *, remember_view: bool = True
) -> None:
    saved = len(await data_store.get_sessions())
    profile_state(context)["saved_sessions"] = await data_store.get_sessions()
    if remember_view:
        set_view(context, "saved_summary")
    await _notify_user(
        update,
        f"Saved sessions: {saved}",
        reply_markup=_stacked_markup([[InlineKeyboardButton("⬅️ Back", callback_data="saved:back")]]),
    )


async def _remove_owner_welcome_after_delay(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    await asyncio.sleep(20)
    try:
        await context.bot.delete_message(chat_id=message.chat_id, message_id=message.message_id)
    except BadRequest:
        with contextlib.suppress(Exception):
            await message.edit_text("Welcome message expired.")
    except Exception:
        with contextlib.suppress(Exception):
            await message.delete()


async def _send_owner_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_lines = [
        "Welcome, you are owner of this bot.",
        "/addsudo <numeric_id> <username>",
        "/rmsudo <numeric_id> <username>",
        "/sudolist",
    ]
    message = await update.effective_message.reply_text("\n".join(help_lines))
    context.application.create_task(_remove_owner_welcome_after_delay(message, context))


async def safe_edit_message(query, text: str, *, reply_markup=None, parse_mode=None, **kwargs):
    current = query.message
    html_text = getattr(current, "text_html", None)
    current_text = (html_text if parse_mode == ParseMode.HTML else current.text) or ""
    current_markup = current.reply_markup
    current_markup_dict = current_markup.to_dict() if current_markup else None
    new_markup_dict = reply_markup.to_dict() if reply_markup else None

    if current_text == text and current_markup_dict == new_markup_dict:
        return current

    try:
        return await query.edit_message_text(
            text, reply_markup=reply_markup, parse_mode=parse_mode, **kwargs
        )
    except BadRequest as exc:
        if "Message is not modified" in str(exc):
            return current
        raise


def _stacked_markup(rows: list[list[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    ordered_rows = [list(row) for row in rows]
    ordered_rows.append([InlineKeyboardButton("🔄 Restart", callback_data="restart")])
    return InlineKeyboardMarkup(ordered_rows)


async def _fetch_session_profile(session: str, api_id: int | None, api_hash: str | None) -> dict:
    if not (api_id and api_hash):
        return {}

    from pyrogram import Client

    client = Client(
        name=f"profile_{abs(hash(session)) % 10_000}",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session,
        workdir=f"/tmp/profile_{abs(hash(session)) % 10_000}",
    )
    try:
        await client.start()
        me = await client.get_me()
        return {
            "id": getattr(me, "id", None),
            "first_name": getattr(me, "first_name", None),
            "last_name": getattr(me, "last_name", None),
            "username": getattr(me, "username", None),
            "phone_number": getattr(me, "phone_number", None),
        }
    except Exception:
        logging.debug("Unable to fetch session profile for display", exc_info=True)
        return {}
    finally:
        with contextlib.suppress(Exception):
            await client.stop()


async def _hydrate_session_profiles(context: ContextTypes.DEFAULT_TYPE, sessions: list[str]) -> dict[str, dict]:
    profile = profile_state(context)
    cache = profile.setdefault("session_profiles", {})
    missing = [session for session in sessions if session not in cache]

    api_id = flow_state(context).get("api_id") or profile.get("api_id") or config.API_ID
    api_hash = flow_state(context).get("api_hash") or profile.get("api_hash") or config.API_HASH

    for session in missing:
        cache[session] = await _fetch_session_profile(session, api_id, api_hash)

    return {session: cache.get(session, {}) for session in sessions}


def _format_session_label(meta: dict, index: int) -> str:
    name_parts = [meta.get("first_name"), meta.get("last_name")]
    name = " ".join(filter(None, name_parts)).strip()
    if not name:
        name = meta.get("username") or f"Session {index + 1}"

    user_id = meta.get("id")
    phone = meta.get("phone_number") or "Unknown"
    username = meta.get("username")

    line = f"{name}{f' ({user_id})' if user_id else ''} • 📞 {phone}"
    if username:
        line = f"{line} • @{username}"
    return line


async def _build_session_rows(context: ContextTypes.DEFAULT_TYPE, sessions: list[str]) -> list[str]:
    profiles = await _hydrate_session_profiles(context, sessions)
    return [_format_session_label(profiles.get(session, {}), idx) for idx, session in enumerate(sessions)]


async def _render_manage_sessions(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    status_line: str | None = None,
    remember_view: bool = True,
) -> None:
    sessions = await data_store.get_sessions()
    profile_state(context)["saved_sessions"] = list(sessions)

    if remember_view:
        set_view(context, "manage_sessions")

    selection = manage_selection(context)
    selection.intersection_update(sessions)
    set_session_order(context, "manage", sessions)

    buttons: list[list[InlineKeyboardButton]] = []
    if sessions:
        rows = await _build_session_rows(context, sessions)
        for idx, label in enumerate(rows):
            prefix = "✅" if sessions[idx] in selection else "⬜"
            buttons.append(
                [InlineKeyboardButton(f"{prefix} {label}", callback_data=f"manage:toggle:{idx}")]
            )

    buttons.append([InlineKeyboardButton("🗑 Remove Selected", callback_data="manage:remove")])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="manage:back")])

    lines = ["Manage Sessions", f"Total saved: {len(sessions)}"]
    if status_line:
        lines.append(status_line)
    lines.append("Toggle sessions below to select them for removal.")
    if not sessions:
        lines.append("No saved sessions available.")

    await _notify_user(update, "\n".join(lines), reply_markup=_stacked_markup(buttons))


async def _render_session_picker(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    sessions: list[str],
    *,
    remember_view: bool = True,
    message_prefix: str | None = None,
) -> None:
    if remember_view:
        set_view(context, "session_picker")

    selection = report_selection(context)
    if not selection:
        selection.update(sessions)
    else:
        selection.intersection_update(sessions)

    set_session_order(context, "report", sessions)
    rows = await _build_session_rows(context, sessions)

    buttons: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                f"{'✅' if session in selection else '⬜'} {rows[idx]}",
                callback_data=f"session_select:toggle:{idx}",
            )
        ]
        for idx, session in enumerate(sessions)
    ]

    buttons.append([InlineKeyboardButton("Done ☑️", callback_data="session_select:done")])
    buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="session_select:back")])
    buttons.append([InlineKeyboardButton("✖️ Cancel", callback_data="nav:cancel")])

    text = message_prefix or "Total sessions available: {count}".format(count=len(sessions))
    text = f"{text}\nSelect the sessions to use, then tap Done ☑️."

    await _notify_user(update, text, reply_markup=_stacked_markup(buttons))


def _format_sessions_for_copy(sessions: list[str], *, max_items: int = 10) -> str:
    lines = [f"<code>{escape(session)}</code>" for session in sessions[:max_items]]
    remaining = len(sessions) - max_items
    if remaining > 0:
        lines.append(f"…and {remaining} more session(s).")
    return "\n".join(lines)


async def _ensure_active_session(query, context: ContextTypes.DEFAULT_TYPE) -> bool:
    flow = flow_state(context)
    if flow.get("sessions"):
        return True

    profile = profile_state(context)
    profile["saved_sessions"] = await data_store.get_sessions()
    saved_sessions = profile.get("saved_sessions") or []

    if saved_sessions:
        flow["sessions"] = list(saved_sessions)
        return True

    await safe_edit_message(
        query,
        friendly_error("Please add a new session"),
        reply_markup=main_menu_keyboard(len(saved_sessions), active_session_count(context)),
    )
    return False


async def _validate_sessions_with_feedback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    sessions: list[str],
    *,
    api_id: int | None,
    api_hash: str | None,
    fallback_markup=None,
) -> list[str]:
    if not sessions:
        await _notify_user(
            update,
            friendly_error("No sessions provided. Please add sessions to continue."),
            reply_markup=fallback_markup,
        )
        return []

    if not (api_id and api_hash):
        await _notify_user(
            update,
            friendly_error("API credentials are missing. Please restart and set API ID/API Hash."),
            reply_markup=fallback_markup,
        )
        return []

    try:
        valid, invalid = await validate_sessions(api_id, api_hash, sessions)
    except Exception:
        logging.exception("Session validation failed")
        await _notify_user(
            update,
            friendly_error("Unable to validate sessions right now. Please try again."),
            reply_markup=fallback_markup,
        )
        return []

    if invalid:
        removed = await data_store.remove_sessions(invalid)
        card = render_card(
            "Session validation",
            [
                f"Removed {len(invalid)} invalid session(s).",
                f"{len(valid)} valid session(s) remain.",
            ],
            [f"Pruned from storage: {removed}"],
        )
        await _notify_user(
            update,
            f"<pre>{card}</pre>",
            parse_mode=ParseMode.HTML,
            reply_markup=fallback_markup,
        )

    if not valid:
        await _notify_user(
            update,
            friendly_error("All provided sessions are invalid. Please add new sessions."),
            reply_markup=fallback_markup,
        )
        return []

    return valid


async def _with_resolver_client(context: ContextTypes.DEFAULT_TYPE, callback):
    """Start a lightweight Pyrogram client for target resolution and cleanup automatically."""

    from pyrogram import Client

    flow = flow_state(context)
    profile = profile_state(context)

    api_id = flow.get("api_id") or profile.get("api_id") or config.API_ID
    api_hash = flow.get("api_hash") or profile.get("api_hash") or config.API_HASH
    sessions = flow.get("sessions") or []

    if not api_id or not api_hash or not sessions:
        raise RuntimeError("Missing API credentials or sessions for resolution")

    session = sessions[0]
    client = Client(
        name=f"resolver_{abs(hash(session)) % 10_000}",
        api_id=api_id,
        api_hash=api_hash,
        session_string=session,
        workdir=f"/tmp/resolver_{abs(hash(session)) % 10_000}",
    )
    try:
        await client.start()
        return await callback(client)
    finally:
        try:
            await client.stop()
        except Exception:
            pass


async def _join_target_with_client(client, parsed_link, status_callback, *, max_attempts: int = 3):
    from pyrogram.errors import FloodWait, RPCError, UserAlreadyParticipant

    me = await client.get_me()
    join_target = (
        parsed_link.normalized
        if getattr(parsed_link, "kind", None) == "invite_hash"
        else getattr(parsed_link, "username", None) or getattr(parsed_link, "normalized", None)
    )
    join_target = join_target.lstrip("@") if isinstance(join_target, str) else join_target

    for attempt in range(1, max_attempts + 1):
        try:
            await client.join_chat(join_target)
            chat = await client.get_chat(join_target)
            member = await client.get_chat_member(chat.id, me.id)
            return {"ok": True, "chat": chat, "member": member, "attempt": attempt}
        except UserAlreadyParticipant:
            chat = await client.get_chat(join_target)
            member = await client.get_chat_member(chat.id, me.id)
            return {"ok": True, "chat": chat, "member": member, "attempt": attempt, "already": True}
        except FloodWait as exc:
            wait_seconds = int(getattr(exc, "value", 0) or 0)
            jitter = random.uniform(1, 3)
            await status_callback(
                f"⚠️ FloodWait: Telegram rate limited this client. Wait {wait_seconds} seconds, then I'll retry automatically. "
                f"(attempt {attempt}/{max_attempts})"
            )
            if attempt >= max_attempts:
                return {
                    "ok": False,
                    "error": "flood_wait",
                    "wait_seconds": wait_seconds,
                    "detail": str(exc),
                }
            await asyncio.sleep(wait_seconds + jitter)
            continue
        except RPCError as exc:
            return {"ok": False, "error": exc.__class__.__name__, "detail": str(exc)}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": exc.__class__.__name__, "detail": str(exc)}

    return {"ok": False, "error": "exhausted"}


def _attach_invite(spec, invite_link: str | None):
    if not invite_link or spec.invite_link:
        return spec

    invite_hash_match = None
    if "+" in invite_link:
        invite_hash_match = invite_link.split("+")[-1]
    else:
        invite_hash_match = invite_link.rsplit("/", 1)[-1]

    return spec.__class__(
        raw=spec.raw,
        normalized=spec.normalized,
        kind="invite" if spec.kind == "username" else spec.kind,
        username=spec.username,
        numeric_id=spec.numeric_id,
        invite_hash=invite_hash_match,
        invite_link=invite_link,
        message_id=spec.message_id,
        internal_id=spec.internal_id,
    )


async def _join_and_report(update: Update, context: ContextTypes.DEFAULT_TYPE, link_text: str):
    try:
        parsed_link = parse_access_link(link_text)
    except Exception:
        await update.effective_message.reply_text(
            "Please send a valid chat access link (https://t.me/+invite, https://t.me/joinchat/code, or @username).",
            reply_markup=navigation_keyboard(),
        )
        return None

    async def _status(message: str):
        await update.effective_message.reply_text(message, reply_markup=navigation_keyboard())

    async def _runner(client):
        return await _join_target_with_client(client, parsed_link, _status)

    try:
        result = await _with_resolver_client(context, _runner)
    except Exception:
        logging.exception("Join flow failed")
        await update.effective_message.reply_text(
            friendly_error("Unable to join this link with the provided session."),
            reply_markup=navigation_keyboard(),
        )
        return None

    if not result or not result.get("ok"):
        wait_seconds = result.get("wait_seconds") if result else None
        detail = result.get("detail") if result else None
        await update.effective_message.reply_text(
            friendly_error(
                "Join failed. "
                + (
                    f"FloodWait {wait_seconds}s. Please wait and resend." if result and result.get("error") == "flood_wait" else "Please verify the link or try another session."
                )
            ),
            reply_markup=navigation_keyboard(),
        )
        if detail:
            logging.warning("Join failure detail: %s", detail)
        return None

    chat = result.get("chat")
    if chat:
        flow_state(context)["invite_link"] = parsed_link.normalized if parsed_link.kind == "invite_hash" else None
        title = getattr(chat, "title", None) or getattr(chat, "first_name", None) or parsed_link.username or "chat"
        await update.effective_message.reply_text(
            f"✅ Joined successfully: {title} ({getattr(chat, 'id', 'unknown')})",
            reply_markup=navigation_keyboard(),
        )

    return parsed_link


def _format_target_details(details) -> str:
    lines = ["🎯 <b>Target Details</b>"]
    if details.title:
        lines.append(f"Name: {escape(details.title)}")
    if details.username:
        lines.append(f"Username: @{escape(details.username)}")
    if details.id:
        lines.append(f"ID: <code>{details.id}</code>")
    if details.type:
        lines.append(f"Type: {escape(details.type)}{' (private)' if details.private else ''}")
    if details.members:
        lines.append(f"Members/Subscribers: {details.members}")
    if details.description:
        lines.append(f"About: {escape(details.description[:140])}")
    flags = []
    for flag, label in (
        (details.is_bot, "bot"),
        (details.is_verified, "verified"),
        (details.is_scam, "scam"),
        (details.is_fake, "fake"),
    ):
        if flag:
            flags.append(label)
    if flags:
        lines.append(f"Flags: {', '.join(flags)}")
    return "\n".join(lines)


async def _resolve_and_preview_target(update: Update, context: ContextTypes.DEFAULT_TYPE, target_text: str) -> bool:
    """Join private chats when needed and show target details."""

    from pyrogram.errors import (  # type: ignore
        ChannelPrivate,
        ChatIdInvalid,
        FloodWait as PyroFloodWait,
        MessageIdInvalid,
        PeerIdInvalid,
    )

    invite_link = flow_state(context).get("invite_link")
    try:
        spec = _attach_invite(parse_target(target_text), invite_link)
    except Exception:
        await _notify_user(
            update,
            friendly_error("I could not understand this link. Please send a valid Telegram message link."),
            reply_markup=navigation_keyboard(),
        )
        return False

    status_message = await update.effective_message.reply_text(
        "🔍 Resolving target…", reply_markup=navigation_keyboard()
    )

    expected_chat_id = (
        int(f"-100{spec.internal_id}")
        if spec.kind == "internal_message" and spec.internal_id is not None
        else None
    )
    debug_lines: list[str] = []
    if spec.kind == "internal_message":
        debug_lines.extend(
            [
                "Link type: private (/c/)",
                f"internal_id: {spec.internal_id}",
                f"expected_chat_id: {expected_chat_id}",
                f"message_id: {spec.message_id}",
            ]
        )

    async def _set_status(
        join: str | None = None,
        fetch: str | None = None,
        message: str | None = None,
        footer: str | None = None,
        extra_debug: list[str] | None = None,
    ):
        lines = ["<b>Target resolution</b>"]
        if join:
            lines.append(f"• {escape(join)}")
        if fetch:
            lines.append(f"• {escape(fetch)}")
        if message:
            lines.append(f"• {escape(message)}")
        if footer:
            lines.append(footer)

        debug_block = debug_lines + (extra_debug or [])
        if debug_block:
            lines.append("")
            lines.append("<b>Debug</b>")
            lines.extend(escape(item) for item in debug_block)

        text = "\n".join(lines)
        try:
            await status_message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=navigation_keyboard())
        except Exception:
            logging.exception("Failed to update resolution status")

    async def _runner(client):
        join_info = None
        resolution = None
        details = None
        message_preview = None
        await _set_status(
            join="Joining chat..." if spec.requires_join else "Join step not required",
            fetch="Resolving chat...",
            message="Fetching message..." if spec.message_id else "Done!",
        )
        if spec.requires_join:
            join_info = await ensure_join_if_needed(client, spec)
            if not join_info.ok:
                return {"join": join_info, "error": join_info.reason or join_info.error}
            if join_info.chat_id:
                debug_lines.append(
                    f"joined_chat_id: {join_info.chat_id} ({join_info.title or 'unknown'})"
                )
            await _set_status(
                join="Joined chat",
                fetch="Resolving chat...",
                message="Fetching message..." if spec.message_id else "Done!",
            )

        resolution = await resolve_entity(client, spec)
        if not resolution.ok:
            return {"join": join_info, "resolution": resolution, "error": resolution.error}

        details = await fetch_target_details(client, resolution)
        await _set_status(
            join="Joined chat" if join_info else "Accessible",
            fetch="Chat resolved",
            message="Fetching message..." if spec.message_id else "Done!",
        )

        if details and details.id:
            members = details.members if details.members is not None else "?"
            debug_lines.append(
                f"resolved_chat_id: {details.id} ({details.title or 'unknown'}) members: {members}"
            )

        chat_ref = None
        if spec.message_id:
            try:
                if spec.kind == "internal_message" and spec.internal_id:
                    chat_ref = expected_chat_id
                elif resolution.chat_id:
                    chat_ref = resolution.chat_id
                elif spec.username:
                    chat_ref = spec.username
                await _set_status(
                    join="Joined chat" if join_info else "Accessible",
                    fetch="Chat resolved",
                    message="Fetching message...",
                )
                message = await client.get_messages(chat_ref, spec.message_id)
                if message is None:
                    return {"join": join_info, "resolution": resolution, "details": details, "error": "MESSAGE_NOT_FOUND"}
                preview_text = getattr(message, "text", None) or getattr(message, "caption", None)
                message_preview = (preview_text or "<no text>").strip()
                await _set_status(
                    join="Joined chat" if join_info else "Accessible",
                    fetch="Chat resolved",
                    message="Done!",
                )
            except MessageIdInvalid:
                return {"join": join_info, "resolution": resolution, "details": details, "error": "MESSAGE_ID_INVALID"}
            except (ChannelPrivate, PeerIdInvalid, ChatIdInvalid):
                return {
                    "join": join_info,
                    "resolution": resolution,
                    "details": details,
                    "error": "CHAT_ACCESS_DENIED",
                }
            except PyroFloodWait as exc:  # type: ignore[misc]
                return {
                    "join": join_info,
                    "resolution": resolution,
                    "details": details,
                    "error": "FLOOD_WAIT",
                    "retry_after": int(getattr(exc, "value", 0) or 0),
                }
            except Exception as exc:  # noqa: BLE001
                logging.exception("Failed to fetch message preview")
                return {"join": join_info, "resolution": resolution, "details": details, "error": exc.__class__.__name__}

        return {
            "join": join_info,
            "resolution": resolution,
            "details": details,
            "message_preview": message_preview,
            "chat_ref": chat_ref,
            "expected_chat_id": expected_chat_id,
        }

    try:
        result = await _with_resolver_client(context, _runner)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Target preview failed")
        await update.effective_message.reply_text(
            friendly_error("Unable to reach this target right now. Please verify the link and try again."),
            reply_markup=navigation_keyboard(),
        )
        return False

    if not result:
        await _set_status(footer=friendly_error("No result returned while resolving."))
        return False

    join_info = result.get("join")
    resolution = result.get("resolution")
    details = result.get("details")
    retry_after = result.get("retry_after")
    expected_chat_id = result.get("expected_chat_id") or expected_chat_id

    if join_info and getattr(join_info, "reason", None) == "flood_wait":
        wait_time = join_info.retry_after or 0
        await _set_status(
            footer=friendly_error(
                "Telegram rate-limited joining this chat.\nPlease try again after ~{wait_time} seconds.".format(
                    wait_time=wait_time or "a few"
                )
            )
        )
        return False

    if join_info and getattr(join_info, "reason", None) == "invite_mismatch":
        await _set_status(
            footer=friendly_error(
                "Invite link does not match this private message link’s chat.\n"
                f"Expected chat_id: {expected_chat_id}\n"
                f"Joined chat_id: {getattr(join_info, 'chat_id', 'unknown')}\n"
                "Please send invite link of the SAME chat."
            )
        )
        return False

    if join_info and not join_info.ok:
        await _set_status(footer=friendly_error("I could not join this chat. Please provide a valid invite or try another session."))
        return False

    if not resolution or not resolution.ok:
        logging.error(
            "TargetResolver failed resolution for %s (kind=%s, error=%s)",
            spec.raw,
            spec.kind,
            getattr(resolution, "error", None),
            stack_info=True,
        )
        error_text = "Could not resolve link/chat. Ensure I have access and the link is correct."
        if spec.kind == "internal_message" and getattr(resolution, "error", None) in {
            "PeerIdInvalid",
            "ChannelPrivate",
        }:
            error_text = (
                "This appears to be a private t.me/c link.\n"
                "Your account must be a member of the chat or join via invite link."
            )
        await _set_status(footer=friendly_error(error_text))
        return False

    error_code = result.get("error")
    if error_code == "MESSAGE_ID_INVALID":
        await _set_status(footer=friendly_error("Message deleted or inaccessible."))
        return False
    if error_code == "CHAT_ACCESS_DENIED":
        await _set_status(footer=friendly_error("No access — ensure bot account joined same private chat."))
        return False
    if error_code == "MESSAGE_NOT_FOUND":
        await _set_status(footer=friendly_error("Message deleted or inaccessible."))
        return False
    if error_code == "FLOOD_WAIT":
        await _set_status(
            footer=friendly_error(
                "Telegram rate-limited fetching this message."
                + (f" Please try again after ~{retry_after} seconds." if retry_after else "")
            )
        )
        return False
    if error_code:
        await _set_status(footer=friendly_error("Unable to fetch message preview."))
        return False

    details = details or TargetDetails(type=None, title=None, id=None, username=None, members=None, private=False)
    preview_lines = [_format_target_details(details)]
    preview_lines.insert(0, "✅ Done!")
    if spec.message_id:
        preview_lines.append("")
        preview_lines.append(f"Message ID: <code>{spec.message_id}</code>")
        chat_ref = result.get("chat_ref") or getattr(resolution, "chat_id", None)
        if chat_ref:
            preview_lines.append(f"Chat reference: <code>{chat_ref}</code>")
        message_preview = result.get("message_preview")
        if message_preview:
            snippet = escape(message_preview[:280])
            preview_lines.append(f"Preview:\n<blockquote>{snippet}</blockquote>")

    await status_message.edit_text(
        "\n".join(preview_lines),
        parse_mode=ParseMode.HTML,
        reply_markup=navigation_keyboard(show_back=False),
    )
    return True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    reset_user_context(context, update.effective_user.id if update.effective_user else None)
    if update.effective_user and is_owner(update.effective_user.id):
        await _send_owner_welcome(update, context)
    await _send_restart_menu(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(HELP_MESSAGE, parse_mode=ParseMode.MARKDOWN)

async def _notify_user(
    update: Update,
    text: str,
    *,
    reply_markup=None,
    parse_mode=None,
):
    if update.callback_query:
        return await safe_edit_message(
            update.callback_query, text, reply_markup=reply_markup, parse_mode=parse_mode
        )
    return await update.effective_message.reply_text(
        text, reply_markup=reply_markup, parse_mode=parse_mode
    )


async def uptime_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    snapshot = process_health()
    uptime_text = format_duration(snapshot["uptime_seconds"])
    message = (
        f"⏱ Uptime: {uptime_text}\n"
        f"🕒 Server time: {snapshot['server_time']}\n"
        f"🔖 Version: {snapshot['version']}\n"
        f"⚙️ CPU: {snapshot['cpu_percent']:.1f}%\n"
        f"🧠 Memory: {snapshot['memory_mb']:.1f} MB"
    )
    await update.effective_message.reply_text(message)


async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    sent_at = time.perf_counter()
    message = await update.effective_message.reply_text("Pinging Telegram…")
    latency_ms = (time.perf_counter() - sent_at) * 1000

    snapshot = process_health()
    await message.edit_text(
        "\n".join(
            [
                f"🏓 Pong! {latency_ms:.0f} ms",
                f"⚙️ CPU: {snapshot['cpu_percent']:.1f}%",
                f"🧠 Memory: {snapshot['memory_mb']:.1f} MB",
                f"⏱ Uptime: {format_duration(snapshot['uptime_seconds'])}",
            ]
        )
    )


async def _send_restart_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    profile = profile_state(context)
    profile["saved_sessions"] = await data_store.get_sessions()
    greeting = render_greeting()
    ui_state(context)["history"] = []
    set_view(context, "main_menu", replace=True)
    markup = _main_menu_markup(context)

    if update.callback_query:
        query = update.callback_query
        await query.answer("Restarted.")
        await safe_edit_message(query, f"<pre>{greeting}</pre>", parse_mode=ParseMode.HTML, reply_markup=markup)
    else:
        await update.effective_message.reply_text(
            f"<pre>{greeting}</pre>",
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )


async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Any user can request a full process restart via "/restart bot" for reliability.
    if getattr(context, "args", None) and context.args and context.args[0].lower() == "bot":
        logging.info("Restart requested by %s (%s)", update.effective_user, update.effective_user.id if update.effective_user else "unknown")
        await update.effective_message.reply_text("Restarting… I will be back shortly for everyone.")
        SchedulerManager.shutdown()
        context.bot_data["restart_requested"] = True
        shutdown_event = context.bot_data.get("shutdown_event")
        if shutdown_event:
            shutdown_event.set()
        else:
            os.execv(sys.executable, [sys.executable, *sys.argv])
        return

    reset_user_context(context, update.effective_user.id if update.effective_user else None)
    await _send_restart_menu(update, context)


async def restart_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    reset_user_context(context, update.effective_user.id if update.effective_user else None)
    await _send_restart_menu(update, context)
    return ConversationHandler.END


REASON_PROMPT = (
    "Select a report type via the buttons below (Spam, Child abuse, Pornography,"
    " Violence, Illegal content, Copyright, Other)."
)


def _reason_label(reason_code: int | None) -> str:
    if reason_code is None:
        return "Not set"
    return REASON_LABELS.get(reason_code, str(reason_code))


async def show_sessions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _render_saved_summary(update, context)


async def handle_action_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if query.data == "action:start":
        return await start_report(update, context)
    if query.data == "action:add":
        await safe_edit_message(query, f"Send {MIN_SESSIONS}-{MAX_SESSIONS} Pyrogram session strings, one per line.")
        return ADD_SESSIONS
    if query.data == "action:help":
        await safe_edit_message(
            query,
            HELP_MESSAGE,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_menu_keyboard(saved_session_count(context), active_session_count(context)),
        )
        return ConversationHandler.END
    if query.data == "action:sessions":
        await _render_saved_summary(update, context)
        return ConversationHandler.END
    return ConversationHandler.END


async def handle_saved_navigation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    previous = pop_view(context) or "main_menu"
    if previous == "saved_summary":
        previous = pop_view(context) or "main_menu"

    if previous == "main_menu":
        await _send_restart_menu(update, context)
        return ConversationHandler.END

    if previous == "manage_sessions":
        await _render_manage_sessions(update, context, remember_view=False)
        return ConversationHandler.END

    await _send_restart_menu(update, context)
    return ConversationHandler.END


async def handle_navigation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if query.data == "nav:back":
        await safe_edit_message(query, "What are you reporting?", reply_markup=target_kind_keyboard())
        return TARGET_KIND

    reset_user_context(context, update.effective_user.id if update.effective_user else None)
    profile = profile_state(context)
    profile["saved_sessions"] = await data_store.get_sessions()
    await safe_edit_message(
        query,
        "Canceled. Use /report to start again.",
        reply_markup=main_menu_keyboard(len(profile.get("saved_sessions", [])), active_session_count(context)),
    )
    return ConversationHandler.END


async def handle_report_again(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    last_config = context.user_data.get("last_report_config") or {}
    profile = profile_state(context)
    profile["saved_sessions"] = await data_store.get_sessions()

    if not last_config.get("sessions") and not profile.get("saved_sessions"):
        await safe_edit_message(
            query,
            friendly_error("No previous sessions available. Please add sessions to start reporting."),
            reply_markup=main_menu_keyboard(saved_session_count(context), active_session_count(context)),
        )
        return ConversationHandler.END

    flow = reset_flow_state(context)
    flow["sessions"] = list(last_config.get("sessions") or profile.get("saved_sessions") or [])

    api_id = last_config.get("api_id") or profile.get("api_id") or config.API_ID
    api_hash = last_config.get("api_hash") or profile.get("api_hash") or config.API_HASH

    if api_id:
        flow["api_id"] = api_id
    if api_hash:
        flow["api_hash"] = api_hash

    valid_sessions = await _validate_sessions_with_feedback(
        update,
        context,
        flow["sessions"],
        api_id=api_id,
        api_hash=api_hash,
        fallback_markup=main_menu_keyboard(saved_session_count(context), active_session_count(context)),
    )
    if not valid_sessions:
        return ConversationHandler.END

    flow["sessions"] = valid_sessions

    await safe_edit_message(
        query,
        "Reusing your previous sessions. What are you reporting?",
        reply_markup=target_kind_keyboard(),
    )
    return TARGET_KIND


async def handle_status_chip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    _, status = query.data.split(":", maxsplit=1)
    if status == "saved":
        await _render_manage_sessions(update, context)
        return

    if status == "active":
        await query.answer("Loaded sessions shown above.", show_alert=False)
        return

    await query.answer("Live status indicators — you are already in the dark UI.", show_alert=False)


async def handle_manage_sessions_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    parts = query.data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    sessions = get_session_order(context, "manage")
    selection = manage_selection(context)

    if action == "toggle":
        try:
            index = int(parts[2])
        except (IndexError, ValueError):
            await query.answer("Unable to toggle this session.", show_alert=True)
            return ConversationHandler.END

        if index < 0 or index >= len(sessions):
            await query.answer("Unknown session.", show_alert=True)
            return ConversationHandler.END

        session_value = sessions[index]
        if session_value in selection:
            selection.discard(session_value)
        else:
            selection.add(session_value)

        await _render_manage_sessions(update, context, remember_view=False)
        return ConversationHandler.END

    if action == "remove":
        targets = [session for session in selection if session in sessions]
        if not targets:
            await query.answer("Select at least one session to remove.", show_alert=True)
            return ConversationHandler.END

        removed = await data_store.remove_sessions(targets)
        selection.difference_update(targets)
        await _render_manage_sessions(
            update,
            context,
            status_line=f"Removed {removed} session(s)." if removed else "No sessions removed.",
            remember_view=False,
        )
        return ConversationHandler.END

    if action == "back":
        previous = pop_view(context) or "main_menu"
        if previous == "saved_summary":
            await _render_saved_summary(update, context)
            return ConversationHandler.END

        await _send_restart_menu(update, context)
        return ConversationHandler.END

    return ConversationHandler.END


async def handle_session_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    profile = profile_state(context)
    profile["saved_sessions"] = await data_store.get_sessions()
    flow = flow_state(context)

    if "api_id" not in flow and profile.get("api_id"):
        flow["api_id"] = profile.get("api_id")
    if "api_hash" not in flow and profile.get("api_hash"):
        flow["api_hash"] = profile.get("api_hash")

    if query.data == "session_mode:reuse":
        saved_sessions = profile.get("saved_sessions", [])
        if not saved_sessions:
            await safe_edit_message(
                query,
                friendly_error("No saved sessions available. Please add new sessions to continue."),
                reply_markup=main_menu_keyboard(len(saved_sessions), active_session_count(context)),
            )
            return ConversationHandler.END

        report_selection(context).clear()
        await _render_session_picker(
            update,
            context,
            list(saved_sessions),
            message_prefix=f"Total sessions available: {len(saved_sessions)}",
        )
        return SESSION_PICK

    flow["sessions"] = []
    await safe_edit_message(
        query,
        f"Send between {MIN_SESSIONS} and {MAX_SESSIONS} Pyrogram session strings (one per line).",
    )
    return REPORT_SESSIONS


async def handle_session_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    sessions = get_session_order(context, "report")
    selection = report_selection(context)
    action = query.data.split(":", maxsplit=2)[1]

    if not sessions:
        await _send_restart_menu(update, context)
        return ConversationHandler.END

    if action == "toggle":
        try:
            index = int(query.data.split(":", maxsplit=2)[2])
        except (IndexError, ValueError):
            await query.answer("Unable to toggle this session.", show_alert=True)
            return SESSION_PICK

        if index >= len(sessions) or index < 0:
            await query.answer("Unknown session.", show_alert=True)
            return SESSION_PICK

        session_value = sessions[index]
        if session_value in selection:
            selection.discard(session_value)
        else:
            selection.add(session_value)

        await _render_session_picker(update, context, sessions, remember_view=False)
        return SESSION_PICK

    if action == "back":
        prompt = context.user_data.get("session_mode_prompt") or "Select a session mode to continue."
        set_view(context, "session_mode", replace=True)
        await safe_edit_message(query, prompt, reply_markup=session_mode_keyboard())
        return SESSION_MODE

    if action == "done":
        ordered_selection = [session for session in sessions if session in selection]
        if not ordered_selection:
            await query.answer("Select at least one session.", show_alert=True)
            return SESSION_PICK

        flow = flow_state(context)
        profile = profile_state(context)
        valid_sessions = await _validate_sessions_with_feedback(
            update,
            context,
            ordered_selection,
            api_id=flow.get("api_id") or profile.get("api_id") or config.API_ID,
            api_hash=flow.get("api_hash") or profile.get("api_hash") or config.API_HASH,
            fallback_markup=_main_menu_markup(context),
        )

        if not valid_sessions:
            return ConversationHandler.END

        flow["sessions"] = valid_sessions
        selection.intersection_update(valid_sessions)
        profile["saved_sessions"] = await data_store.get_sessions()
        session_preview = _format_sessions_for_copy(valid_sessions)
        set_view(context, "target_kind", replace=True)
        await safe_edit_message(
            query,
            f"Selected {len(valid_sessions)} session(s).\n\n{session_preview}\n\nWhat are you reporting?",
            reply_markup=target_kind_keyboard(),
            parse_mode=ParseMode.HTML,
        )
        return TARGET_KIND

    return ConversationHandler.END


async def start_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    profile = profile_state(context)
    flow = reset_flow_state(context)

    profile["saved_sessions"] = await data_store.get_sessions()

    saved_api_id = profile.get("api_id") or config.API_ID
    saved_api_hash = profile.get("api_hash") or config.API_HASH

    if saved_api_id and saved_api_hash:
        flow["api_id"] = saved_api_id
        flow["api_hash"] = saved_api_hash
        profile["api_id"] = saved_api_id
        profile["api_hash"] = saved_api_hash
        prompt = "Using your saved API credentials. Select a session mode to continue."
        context.user_data["session_mode_prompt"] = prompt
        set_view(context, "session_mode")
        await update.effective_message.reply_text(
            prompt,
            reply_markup=session_mode_keyboard(),
        )
        return SESSION_MODE

    await update.effective_message.reply_text("Enter your API ID (integer).")
    return API_ID_STATE


async def handle_api_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if not text.isdigit():
        await update.effective_message.reply_text("Please provide a valid integer API ID.")
        return API_ID_STATE

    api_id = int(text)
    flow_state(context)["api_id"] = api_id
    profile_state(context)["api_id"] = api_id

    await update.effective_message.reply_text("Enter your API Hash (keep it secret).")
    return API_HASH_STATE


async def handle_api_hash(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    api_hash = (update.message.text or "").strip()
    if len(api_hash) < 10:
        await update.effective_message.reply_text("API Hash seems too short. Please re-enter it.")
        return API_HASH_STATE

    flow_state(context)["api_hash"] = api_hash
    profile_state(context)["api_hash"] = api_hash

    await update.effective_message.reply_text(
        f"Send between {MIN_SESSIONS} and {MAX_SESSIONS} Pyrogram session strings (one per line), or type 'use saved' to reuse stored ones."
    )
    return REPORT_SESSIONS


async def handle_sessions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw_text = (update.message.text or "").strip()
    profile = profile_state(context)

    if raw_text.lower() in {"use saved", "use_saved"}:
        saved_sessions = profile.get("saved_sessions", [])
        if not saved_sessions:
            await update.effective_message.reply_text(
                friendly_error("No saved sessions available. Please enter new sessions."),
                reply_markup=main_menu_keyboard(len(saved_sessions), active_session_count(context)),
            )
            return ConversationHandler.END

        report_selection(context).clear()
        await _render_session_picker(
            update,
            context,
            list(saved_sessions),
            message_prefix=f"Total sessions available: {len(saved_sessions)}",
        )
        return SESSION_PICK

    sessions = session_strings_from_text(raw_text)
    if not (MIN_SESSIONS <= len(sessions) <= MAX_SESSIONS):
        await update.effective_message.reply_text(
            friendly_error(f"Please provide between {MIN_SESSIONS} and {MAX_SESSIONS} sessions."),
            reply_markup=main_menu_keyboard(saved_session_count(context), active_session_count(context)),
        )
        return REPORT_SESSIONS

    flow = flow_state(context)

    valid_sessions = await _validate_sessions_with_feedback(
        update,
        context,
        sessions,
        api_id=flow.get("api_id") or profile.get("api_id") or config.API_ID,
        api_hash=flow.get("api_hash") or profile.get("api_hash") or config.API_HASH,
        fallback_markup=main_menu_keyboard(saved_session_count(context), active_session_count(context)),
    )

    if not valid_sessions:
        return REPORT_SESSIONS

    added = await data_store.add_sessions(
        valid_sessions, added_by=update.effective_user.id if update.effective_user else None
    )
    if added:
        profile["saved_sessions"] = list({*(profile.get("saved_sessions") or []), *valid_sessions})

    flow["sessions"] = valid_sessions
    await update.effective_message.reply_text("What are you reporting?", reply_markup=target_kind_keyboard())
    return TARGET_KIND


async def handle_target_kind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if not await _ensure_active_session(query, context):
        return ConversationHandler.END

    if query.data == "kind:private":
        await safe_edit_message(
            query,
            "Send the private invite link (https://t.me/+code)",
            reply_markup=navigation_keyboard(show_back=False),
        )
        return PRIVATE_INVITE

    if query.data == "kind:public":
        await safe_edit_message(
            query,
            "Send the public message link (https://t.me/username/1234)",
            reply_markup=navigation_keyboard(show_back=False),
        )
        return PUBLIC_MESSAGE

    await safe_edit_message(query, "Send the story URL or username.", reply_markup=navigation_keyboard(show_back=False))
    return STORY_URL


async def handle_private_invite(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()

    parsed_link = await _join_and_report(update, context, text)
    if not parsed_link:
        return PRIVATE_INVITE

    await update.effective_message.reply_text(
        "Joined successfully. Now send the private message link (https://t.me/c/123456789/45)",
        reply_markup=navigation_keyboard(),
    )
    pending_link = flow_state(context).pop("pending_private_link", None)
    if pending_link:
        flow = flow_state(context)
        flow["targets"] = [pending_link]
        flow["target_kind"] = "private"
        if await _resolve_and_preview_target(update, context, pending_link):
            await update.effective_message.reply_text(REASON_PROMPT, reply_markup=reason_keyboard())
            return REPORT_REASON_TYPE

    return PRIVATE_MESSAGE


async def handle_private_message_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    try:
        parsed = parse_telegram_url(text)
    except Exception:
        await update.effective_message.reply_text(
            "Please send a valid private message link (https://t.me/c/123456789/45)",
            reply_markup=navigation_keyboard(),
        )
        return PRIVATE_MESSAGE

    if parsed.get("type") != "private_message":
        await update.effective_message.reply_text(
            "Please send a valid private message link (https://t.me/c/123456789/45)",
            reply_markup=navigation_keyboard(),
        )
        return PRIVATE_MESSAGE

    flow = flow_state(context)
    flow["targets"] = [text]
    flow["target_kind"] = "private"

    if not await _resolve_and_preview_target(update, context, text):
        return PRIVATE_MESSAGE

    await update.effective_message.reply_text(
        REASON_PROMPT, reply_markup=reason_keyboard()
    )
    return REPORT_REASON_TYPE


async def handle_public_message_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if not text:
        await update.effective_message.reply_text(
            "Send a valid public link or @username (https://t.me/username/1234)", reply_markup=navigation_keyboard()
        )
        return PUBLIC_MESSAGE

    message_link = maybe_parse_message_link(text)
    if message_link and getattr(message_link, "is_private", False):
        flow_state(context)["pending_private_link"] = text
        flow_state(context)["target_kind"] = "private"
        await update.effective_message.reply_text(
            "This is a private/internal message link. Please provide the chat invite link first.\n\n"
            "Send the private invite link (https://t.me/+code)",
            reply_markup=navigation_keyboard(show_back=False),
        )
        return PRIVATE_INVITE

    flow = flow_state(context)
    flow["targets"] = [text]
    flow["target_kind"] = "public"

    if not await _resolve_and_preview_target(update, context, text):
        return PUBLIC_MESSAGE

    await update.effective_message.reply_text(
        REASON_PROMPT, reply_markup=reason_keyboard()
    )
    return REPORT_REASON_TYPE


async def handle_story_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if not text:
        await update.effective_message.reply_text(
            "Send a valid story URL or username.", reply_markup=navigation_keyboard()
        )
        return STORY_URL
    await update.effective_message.reply_text(
        "NOT_SUPPORTED: profile/story URLs are not supported for reporting.",
        reply_markup=navigation_keyboard(),
    )
    return ConversationHandler.END


async def handle_report_urls(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    targets = parse_links(update.message.text or "")
    if not targets:
        await update.effective_message.reply_text("Please send at least one valid Telegram URL.")
        return REPORT_URLS

    flow_state(context)["targets"] = targets
    await update.effective_message.reply_text(
        REASON_PROMPT, reply_markup=reason_keyboard()
    )
    return REPORT_REASON_TYPE


async def handle_reason_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if not await _ensure_active_session(query, context):
        return ConversationHandler.END
    reason_code = int(query.data.split(":")[1])
    flow_state(context)["reason_code"] = reason_code

    await safe_edit_message(query, "Send a short reason for reporting (up to 5 lines).")
    return REPORT_MESSAGE


async def handle_reason_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    reasons = parse_reasons(update.message.text or "")
    if not reasons:
        await update.effective_message.reply_text("Please send at least one reason.")
        return REPORT_MESSAGE

    flow_state(context)["reasons"] = reasons
    await update.effective_message.reply_text(
        f"How many report requests? (min {MIN_REPORTS}, max {MAX_REPORTS}, or 'default' for {DEFAULT_REPORTS})"
    )
    return REPORT_COUNT


async def handle_report_count(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip().lower()
    if text in {"", "default"}:
        count = DEFAULT_REPORTS
    elif text.isdigit():
        count = int(text)
        if not (MIN_REPORTS <= count <= MAX_REPORTS):
            await update.effective_message.reply_text(
                friendly_error(f"Enter a number between {MIN_REPORTS} and {MAX_REPORTS}, or 'default'.")
            )
            return REPORT_COUNT
    else:
        await update.effective_message.reply_text(
            friendly_error(f"Enter a number between {MIN_REPORTS} and {MAX_REPORTS}, or 'default'.")
        )
        return REPORT_COUNT

    flow_state(context)["count"] = count

    flow = flow_state(context)
    summary = (
        f"Targets: {len(flow.get('targets', []))}\n"
        f"Reasons: {', '.join(flow.get('reasons', []))}\n"
        f"Report type: {_reason_label(flow.get('reason_code'))}\n"
        f"Total reports each: {flow.get('count')}\n"
        f"Session count: {len(flow.get('sessions', []))}"
    )

    await update.effective_message.reply_text(
        f"Confirm the report run?\n\n{summary}",
        reply_markup=add_restart_button(
            InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Start", callback_data="confirm:start")],
                    [InlineKeyboardButton("Cancel", callback_data="confirm:cancel")],
                ]
            )
        ),
    )
    return ConversationHandler.WAITING


async def handle_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    if not await _ensure_active_session(query, context):
        return ConversationHandler.END
    if query.data == "confirm:cancel":
        await safe_edit_message(query, "Canceled. Use /report to start over.")
        return ConversationHandler.END

    progress_message = await safe_edit_message(query, "Starting report…", reply_markup=None)

    job_data = deepcopy(flow_state(context))
    context.user_data["last_report_config"] = deepcopy(job_data)

    progress_state = {
        "joined": 0,
        "already": 0,
        "total": len(job_data.get("sessions", []) or []),
        "resolved": False,
    }

    stop_event = asyncio.Event()

    animation_task = context.application.create_task(
        run_progress_animation(
            context.bot,
            progress_message.chat_id,
            progress_message.message_id,
            stop_event,
            title="Processing report",
            details=lambda: progress_state,
        )
    )

    async def _status_hook(payload: dict) -> None:
        join_info = payload.get("join", {}) if isinstance(payload, dict) else {}
        progress_state["total"] = max(progress_state.get("total", 0), join_info.get("total", 0))
        progress_state["joined"] = join_info.get("joined", progress_state.get("joined", 0))

        client_states = (payload.get("report") or {}).get("clients", {}) if isinstance(payload, dict) else {}
        progress_state["already"] = len(
            [1 for info in client_states.values() if info.get("reason") == "already_participant"]
        )
        progress_state["resolved"] = bool((payload.get("target") or {}).get("validated"))

    try:
        summary = await run_report_job(
            query,
            context,
            job_data,
            status_message=progress_message,
            send_progress_updates=False,
            suppress_final_message=True,
            status_hook=_status_hook,
        )
        ok = summary.get("total_success", 0)
        fail = summary.get("total_failed", 0)
        total = ok + fail
        final_text = (
            "✅ Done!\n"
            f"Reports sent: {ok}/{total or 1}\n"
            f"Failed: {fail}\n"
            f"If failed: {summary.get('last_error') or 'None'}\n"
            f"(Joined: {progress_state.get('joined', 0)}, Already in: {progress_state.get('already', 0)})"
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("Reporting task failed")
        final_text = f"❌ Reporting failed: {exc}"
    finally:
        stop_event.set()
        with contextlib.suppress(Exception):
            await animation_task

    with contextlib.suppress(Exception):
        await context.bot.edit_message_text(
            chat_id=progress_message.chat_id,
            message_id=progress_message.message_id,
            text=final_text,
        )

    clear_report_state(context)
    return ConversationHandler.END


async def handle_add_sessions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.effective_message.reply_text(
        f"Send between {MIN_SESSIONS} and {MAX_SESSIONS} Pyrogram session strings (one per line)."
    )
    return ADD_SESSIONS


async def receive_added_sessions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    sessions = session_strings_from_text(update.message.text or "")
    if not (MIN_SESSIONS <= len(sessions) <= MAX_SESSIONS):
        await update.effective_message.reply_text(
            friendly_error(f"Please provide between {MIN_SESSIONS} and {MAX_SESSIONS} sessions.")
        )
        return ADD_SESSIONS

    added = await data_store.add_sessions(
        sessions, added_by=update.effective_user.id if update.effective_user else None
    )
    profile_state(context)["saved_sessions"] = (profile_state(context).get("saved_sessions") or []) + added
    await update.effective_message.reply_text(
        f"Stored {len(added)} new session(s). Total available: {len(await data_store.get_sessions())}."
    )
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.effective_message.reply_text("Canceled. Use /report to begin again.")
    reset_flow_state(context)
    return ConversationHandler.END


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.exception("Update %s caused error", update, exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        card = render_card("Unexpected error", ["Something went wrong. Please try again later."], [])
        await update.effective_message.reply_text(
            f"<pre>{card}</pre>",
            parse_mode=ParseMode.HTML,
            reply_markup=add_restart_button(None),
        )


__all__ = [
    "start",
    "help_command",
    "show_sessions",
    "handle_action_buttons",
    "handle_saved_navigation",
    "handle_status_chip",
    "handle_manage_sessions_action",
    "handle_session_mode",
    "handle_session_selection",
    "handle_report_again",
    "start_report",
    "handle_api_id",
    "handle_api_hash",
    "handle_sessions",
    "handle_target_kind",
    "handle_navigation",
    "handle_private_invite",
    "handle_private_message_link",
    "handle_public_message_link",
    "handle_story_url",
    "handle_report_urls",
    "handle_reason_type",
    "handle_reason_message",
    "handle_report_count",
    "handle_confirmation",
    "handle_add_sessions",
    "receive_added_sessions",
    "cancel",
    "error_handler",
    "uptime_command",
    "ping_command",
    "restart_command",
    "restart_callback",
]
