from __future__ import annotations

"""Command and callback handlers for the reporting bot."""

import asyncio
import contextlib
import logging
import uuid
from collections import deque
from datetime import datetime
from io import BytesIO
from time import monotonic
from typing import Callable, Tuple
import os
import tempfile

from pyrogram import Client, filters
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import FloodWait, RPCError, UserAlreadyParticipant
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

import config
from logging_utils import log_error, log_report_summary, log_user_start, send_log
from report import send_report
from session_bot import (
    SessionIdentity,
    extract_sessions_from_text,
    fetch_session_identity,
    prune_sessions,
    validate_session_string,
)
from state import QueueEntry, ReportQueue, StateManager
from sudo import is_owner
from ui import (
    REPORT_REASONS,
    owner_panel,
    queued_message,
    reason_keyboard,
    report_type_keyboard,
    sudo_panel,
)
from bot.utils import resolve_chat_id

# Create temp directory for clients
TEMP_BASE = os.path.join(tempfile.gettempdir(), "tgbot_reports")
os.makedirs(TEMP_BASE, exist_ok=True)


def _normalize_chat_id(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def register_handlers(app: Client, persistence, states: StateManager, queue: ReportQueue) -> None:
    """Register all command and callback handlers."""
    session_tokens: dict[str, str] = {}

    async def _ensure_admin(chat_id: int) -> bool:
        try:
            me = await app.get_me()
            member = await app.get_chat_member(chat_id, me.id)
            status = getattr(member, "status", "")
            return status in {
                ChatMemberStatus.ADMINISTRATOR,
                ChatMemberStatus.OWNER,
                "administrator",
                "creator",
            }
        except Exception:
            return False

    async def _wrap_errors(func: Callable, *args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as exc:
            logging.exception("Handler error")
            await log_error(app, await persistence.get_logs_group_id(), exc, config.OWNER_ID)

    async def _queue_error(exc: Exception) -> None:
        await log_error(app, await persistence.get_logs_group_id(), exc, config.OWNER_ID)

    queue.set_error_handler(_queue_error)

    async def _log_stage(stage: str, detail: str) -> None:
        await send_log(
            app,
            await persistence.get_logs_group_id(),
            f"🛰 {stage}\n{detail}",
        )

    async def _sessions_available() -> list[str]:
        sessions = await prune_sessions(persistence, announce=True)
        return sessions

    # ============== AGGRESSIVE FORCE REFRESH FUNCTION ==============
    async def _force_refresh_peer(client: Client, chat_id: int) -> tuple[bool, str]:
        """Force refresh a peer by aggressively clearing cache and re-resolving."""
        try:
            # Method 1: Nuclear option - clear ALL peer cache
            if hasattr(client, 'peer_cache'):
                cache_size = len(client.peer_cache)
                client.peer_cache = {}
                logging.info(f"🧹 Cleared complete peer cache ({cache_size} entries)")
            
            # Method 2: Clear storage caches
            if hasattr(client, 'storage'):
                try:
                    if hasattr(client.storage, 'peer_by_id'):
                        client.storage.peer_by_id.clear()
                    if hasattr(client.storage, 'peer_by_username'):
                        client.storage.peer_by_username.clear()
                    logging.info("🧹 Cleared storage peer caches")
                except:
                    pass
            
            # Method 3: Try multiple ID formats
            id_variations = [
                chat_id,  # Original: -1002637094216
                int(str(chat_id).replace("-100", "")),  # Without -100: 2637094216
                abs(chat_id),  # Absolute value
                str(chat_id),  # As string
                f"-100{str(chat_id).replace('-100', '')}",  # Reconstructed
            ]
            
            for attempt, id_var in enumerate(id_variations, 1):
                try:
                    logging.info(f"🔄 Attempt {attempt}: Trying with ID format: {id_var}")
                    chat = await client.get_chat(id_var)
                    logging.info(f"✅ Success with ID format {id_var}: {chat.title}")
                    return True, f"Success with ID format {id_var}"
                except Exception as e:
                    logging.warning(f"❌ Failed with ID format {id_var}: {e}")
            
            # Method 4: Scan dialogs thoroughly
            try:
                logging.info("🔄 Scanning dialogs for chat...")
                async for dialog in client.get_dialogs(limit=200):
                    if dialog.chat.id == chat_id:
                        logging.info(f"✅ Found chat in dialogs: {dialog.chat.title}")
                        # Now force a cache update by getting chat
                        chat = await client.get_chat(chat_id)
                        return True, f"Found in dialogs: {dialog.chat.title}"
            except Exception as e:
                logging.warning(f"❌ Dialog scan failed: {e}")
            
            # Method 5: Try to get any message from history
            try:
                logging.info("🔄 Trying to get chat history...")
                async for message in client.get_chat_history(chat_id, limit=1):
                    if message:
                        logging.info(f"✅ Found message in chat, cache updated")
                        return True, "Found via message history"
            except Exception as e:
                logging.warning(f"❌ Chat history failed: {e}")
            
            # Method 6: Try to resolve via get_chat with different approaches
            try:
                # Try to get chat info via get_chat with the naked ID
                naked_id = str(chat_id).replace("-100", "")
                logging.info(f"🔄 Trying to resolve via naked ID: {naked_id}")
                
                # Try as channel
                try:
                    chat = await client.get_chat(int(naked_id))
                    logging.info(f"✅ Resolved as channel with ID {naked_id}")
                    return True, f"Resolved as channel {naked_id}"
                except:
                    pass
                
                # Try as supergroup
                try:
                    chat = await client.get_chat(f"-100{naked_id}")
                    logging.info(f"✅ Resolved as supergroup with -100{naked_id}")
                    return True, f"Resolved as supergroup"
                except:
                    pass
                    
            except Exception as e:
                logging.warning(f"❌ Naked ID resolution failed: {e}")
            
            # Method 7: Try to join again (refreshes cache)
            try:
                invite_link = f"https://t.me/c/{str(chat_id).replace('-100', '')}"
                logging.info(f"🔄 Attempting to re-join via {invite_link}")
                await client.join_chat(invite_link)
                await asyncio.sleep(2)
                chat = await client.get_chat(chat_id)
                logging.info(f"✅ Successfully re-joined and accessed chat")
                return True, "Re-joined successfully"
            except UserAlreadyParticipant:
                # Already joined, but cache might be updated
                logging.info(f"✅ Already joined, cache should be updated")
                try:
                    chat = await client.get_chat(chat_id)
                    return True, "Already joined, cache updated"
                except:
                    pass
            except Exception as e:
                logging.warning(f"❌ Re-join failed: {e}")
            
            return False, "All refresh methods failed"
            
        except Exception as e:
            logging.error(f"Force refresh error: {e}")
            return False, f"Error: {str(e)}"

    # ============== FIXED RESOLVER FUNCTION - PROPERLY HANDLES PRIVATE CHATS ==============
    async def _resolve_target_across_sessions(
        target_link: str, sessions: list[str]
    ) -> tuple[int | str | None, list[str], str | None]:
        """Resolve the target chat id using any available session string.
        
        For private chats: Returns the numeric ID and ALL sessions (let client pool handle access)
        For public chats: Resolves username to ID using first session
        """
        
        last_error: str | None = None
        resolved_chat_id: int | str | None = None
        message_id: int | None = None

        # Parse the link first
        try:
            segments = target_link.rstrip("/").split("/")
            
            if "t.me/c/" in target_link:
                # PRIVATE CHAT - just extract the numeric ID and return ALL sessions
                channel_id = int(segments[-2])
                resolved_chat_id = int(f"-100{channel_id}")
                message_id = int(segments[-1])
                logging.info(f"🔍 Resolved private chat ID: {resolved_chat_id} for message: {message_id}")
                
                # Return ALL sessions - let client pool handle access with force refresh
                return resolved_chat_id, sessions, None
                
            elif "t.me/" in target_link and len(segments) >= 2:
                # PUBLIC CHAT - resolve username to ID
                username = segments[-2].lstrip("@")
                message_id = int(segments[-1])
                logging.info(f"🔍 Resolved public chat: @{username} for message: {message_id}")
                
                # Try to resolve username with first session
                for idx, session in enumerate(sessions[:1]):
                    client_name = f"resolver_{idx}_{uuid.uuid4().hex[:8]}"
                    workdir = os.path.join(TEMP_BASE, client_name)
                    os.makedirs(workdir, exist_ok=True)
                    
                    client = Client(
                        name=client_name,
                        api_id=config.API_ID,
                        api_hash=config.API_HASH,
                        session_string=session,
                        workdir=workdir,
                        in_memory=True
                    )
                    
                    try:
                        await client.start()
                        chat = await client.get_chat(username)
                        resolved_chat_id = chat.id
                        logging.info(f"✅ Resolved @{username} to {chat.id}")
                        return resolved_chat_id, sessions, None
                    except Exception as e:
                        last_error = str(e)
                        logging.warning(f"❌ Failed to resolve @{username}: {e}")
                    finally:
                        await client.stop()
                
                # If resolution failed, return username and all sessions
                return username, sessions, last_error or f"Could not resolve username {username}"
            else:
                return None, [], f"Invalid link format: {target_link}"
                
        except Exception as e:
            return None, [], f"Failed to parse link: {e}"

    async def _prompt_report_count(message: Message) -> None:
        await message.reply_text(
            (
                "How many reports do you want to send? "
                f"Send a number between {config.MIN_REPORTS} and {config.MAX_REPORTS}."
            )
        )

    async def _apply_report_count(message: Message, state, count: int) -> None:
        if count < config.MIN_REPORTS or count > config.MAX_REPORTS:
            await message.reply_text(
                f"Please choose a value between {config.MIN_REPORTS} and {config.MAX_REPORTS}."
            )
            return

        state.report_count = count
        await message.reply_text(f"✅ Will send {count} reports.")

        next_stage = state.next_stage_after_count or "awaiting_link"
        state.next_stage_after_count = None
        if next_stage == "awaiting_private_join":
            state.stage = "awaiting_private_join"
            await message.reply_text(
                "Send the private group/channel invite link or username so I can join with all sessions."
            )
            return
        if next_stage == "awaiting_link":
            state.stage = "awaiting_link"
            await message.reply_text(
                "Send the target message link (https://t.me/...) to report."
            )
            return
        if next_stage == "begin_report":
            await _begin_report(message, state)
            return

        state.stage = next_stage

    async def _is_sudo_user(user_id: int | None) -> bool:
        if user_id is None:
            return False
        if is_owner(user_id):
            return True
        sudo_users = await persistence.get_sudo_users()
        allowed = sudo_users or set(config.SUDO_USERS)
        return user_id in allowed

    async def _owner_guard(message: Message) -> bool:
        if not message.from_user or not is_owner(message.from_user.id):
            await message.reply_text("Only the owner can manage sudo users.")
            return False
        return True

    @app.on_message(filters.command("start"))
    async def start_handler(_: Client, message: Message) -> None:
        await _wrap_errors(_handle_start, message)

    async def _handle_start(message: Message) -> None:
        if not message.from_user:
            return

        user_id = message.from_user.id
        await persistence.add_known_chat(message.chat.id)
        await log_user_start(app, await persistence.get_logs_group_id(), message)

        if is_owner(user_id):
            await message.reply_text(
                "Welcome, Owner! Choose an action below.", reply_markup=owner_panel()
            )
            await _log_stage("Owner Start", "Owner opened start panel")
            return

        if await _is_sudo_user(user_id):
            await message.reply_text(
                "👋 Ready to report?", reply_markup=sudo_panel(message.from_user.id)
            )
            await _log_stage("Sudo Start", f"Sudo {user_id} opened start panel")
            return

        await message.reply_text(
            "🚫 You are not authorized to use this bot.\n"
            f"Contact the owner (ID: {config.OWNER_ID}) to request access."
        )
        await _log_stage("Unauthorized", f"User {user_id} attempted /start")

    @app.on_message(filters.command("addsudo"))
    async def add_sudo(_: Client, message: Message) -> None:
        await _wrap_errors(_handle_add_sudo, message)

    async def _handle_add_sudo(message: Message) -> None:
        if not await _owner_guard(message):
            return

        parts = (message.text or "").split(maxsplit=2)
        if len(parts) < 2 or not parts[1].isdigit():
            await message.reply_text("Usage: /addsudo <user_id> [username]")
            return

        user_id = int(parts[1])
        if is_owner(user_id):
            await message.reply_text("Owner already has access.")
            return
        sudo_users = await persistence.get_sudo_users()
        if user_id in sudo_users:
            await message.reply_text("User is already a sudo user.")
            return

        await persistence.add_sudo_user(user_id)
        label = parts[2] if len(parts) > 2 else str(user_id)
        await message.reply_text(f"Added {label} ({user_id}) to sudo users.")
        await _log_stage("Sudo Added", f"Owner added {user_id}")

    async def _handle_sudo_list(message: Message) -> None:
        if not await _owner_guard(message):
            return
        sudo_users = await persistence.get_sudo_users()
        if not sudo_users:
            await message.reply_text("No sudo users are configured.")
            return
        formatted = "\n".join([f"• `{uid}`" for uid in sorted(sudo_users)])
        await message.reply_text(f"Current sudo users:\n{formatted}", parse_mode="markdown")

    @app.on_message(filters.command("rmsudo"))
    async def remove_sudo(_: Client, message: Message) -> None:
        await _wrap_errors(_handle_remove_sudo, message)

    @app.on_message(filters.command("sudolist"))
    async def sudo_list(_: Client, message: Message) -> None:
        await _wrap_errors(_handle_sudo_list, message)

    @app.on_message(filters.command("set_session") & filters.group)
    async def set_session_group(_: Client, message: Message) -> None:
        await _wrap_errors(_handle_set_session_group, message)

    @app.on_message(filters.command("set_log") & filters.group)
    async def set_logs_group(_: Client, message: Message) -> None:
        await _wrap_errors(_handle_set_logs_group, message)

    @app.on_message(filters.command("broadcast"))
    async def broadcast(_: Client, message: Message) -> None:
        await _wrap_errors(_handle_broadcast, message)

    @app.on_message(filters.command("cache_stats"))
    async def cache_stats(_: Client, message: Message) -> None:
        await _wrap_errors(_handle_cache_stats, message)

    @app.on_message(filters.command("clear_cache"))
    async def clear_cache(_: Client, message: Message) -> None:
        await _wrap_errors(_handle_clear_cache, message)

    # ============== NEW CACHE MANAGEMENT COMMANDS ==============
    @app.on_message(filters.command("clear_pyrogram_cache"))
    async def clear_pyrogram_cache(_: Client, message: Message) -> None:
        """Clear Pyrogram's internal peer cache for all sessions."""
        if not await _owner_guard(message):
            return
        
        sessions = await _sessions_available()
        cleared = 0
        
        status_msg = await message.reply_text("🧹 Clearing peer caches...")
        
        for idx, session in enumerate(sessions[:5]):  # Test first 5 sessions
            client = Client(
                name=f"cache_clearer_{idx}",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                session_string=session,
                in_memory=True
            )
            
            try:
                await client.start()
                
                # Clear peer cache
                if hasattr(client, 'peer_cache'):
                    cache_size = len(client.peer_cache)
                    client.peer_cache = {}
                    cleared += cache_size
                    logging.info(f"✅ Cleared {cache_size} entries from session {idx} cache")
                else:
                    logging.info(f"⚠️ Session {idx} has no peer_cache attribute")
                    
            except Exception as e:
                logging.error(f"Error clearing cache for session {idx}: {e}")
            finally:
                await client.stop()
        
        await status_msg.edit_text(f"✅ Cleared peer cache for {cleared} entries across {min(5, len(sessions))} sessions")
        await _log_stage("Cache Clear", f"Cleared {cleared} peer cache entries")

    @app.on_message(filters.command("check_member"))
    async def check_member(_: Client, message: Message) -> None:
        """Check if sessions are members of a chat."""
        if not await _owner_guard(message):
            return
        
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await message.reply_text("Usage: /check_member <chat_link_or_username>")
            return
        
        target = parts[1].strip()
        sessions = await _sessions_available()
        
        if not sessions:
            await message.reply_text("No sessions available")
            return
        
        status_msg = await message.reply_text(f"🔍 Checking membership for {target}...")
        results = []
        
        # Parse target to get chat ID if possible
        try:
            # Try to extract chat ID from link
            if "t.me/c/" in target:
                segments = target.rstrip("/").split("/")
                channel_id = int(segments[-2])
                chat_id = int(f"-100{channel_id}")
            elif "t.me/" in target:
                username = target.split("/")[-2].lstrip("@")
                chat_id = username
            else:
                chat_id = target
        except:
            chat_id = target
        
        for idx, session in enumerate(sessions[:5]):
            client = Client(
                name=f"check_{idx}",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                session_string=session,
                in_memory=True
            )
            
            try:
                await client.start()
                me = await client.get_me()
                
                # Try to get chat
                try:
                    chat = await client.get_chat(chat_id)
                    
                    # Try to get member status
                    try:
                        member = await client.get_chat_member(chat.id, "me")
                        status = getattr(member, "status", "unknown")
                        results.append(f"✅ Session {idx} ({me.phone_number}): Member of {chat.title} (status: {status})")
                    except Exception as e:
                        results.append(f"⚠️ Session {idx} ({me.phone_number}): Can see chat but membership unknown - {e}")
                        
                except Exception as e:
                    results.append(f"❌ Session {idx} ({me.phone_number}): Cannot access - {e}")
                    
            except Exception as e:
                results.append(f"💥 Session {idx}: Failed - {e}")
            finally:
                await client.stop()
        
        await status_msg.edit_text("\n".join(results[:10]))  # Show max 10 results
        await _log_stage("Member Check", f"Checked membership for {target}")

    @app.on_message((filters.group) & (filters.text | filters.document))
    async def session_ingest(_: Client, message: Message) -> None:
        await _wrap_errors(_handle_session_ingest, message)

    async def _handle_cache_stats(message: Message) -> None:
        if not await _owner_guard(message):
            return
        
        from bot.peer_resolver import _failure_cache, _FAILURE_TTL
        from datetime import datetime, timezone
        
        stats = [
            f"📊 **Cache Statistics**",
            f"• Failed entries: {len(_failure_cache)}",
            f"• Failure TTL: {_FAILURE_TTL.total_seconds() / 60:.0f} minutes",
        ]
        
        if _failure_cache:
            stats.append("\n**Recent failures:**")
            for key, record in list(_failure_cache.items())[:5]:
                expires_in = (record.expires_at - datetime.now(timezone.utc)).total_seconds() / 60
                stats.append(f"• `{key}`: {record.reason} (expires in {expires_in:.1f} min)")
        
        await message.reply_text("\n".join(stats))
        await _log_stage("Cache Stats", f"Owner checked cache ({len(_failure_cache)} entries)")

    async def _handle_clear_cache(message: Message) -> None:
        if not await _owner_guard(message):
            return
        
        from bot.peer_resolver import _failure_cache
        
        count = len(_failure_cache)
        _failure_cache.clear()
        
        await message.reply_text(f"✅ Cleared {count} entries from failure cache.")
        await _log_stage("Cache Clear", f"Owner cleared failure cache ({count} entries)")

    async def _handle_remove_sudo(message: Message) -> None:
        if not await _owner_guard(message):
            return

        parts = (message.text or "").split(maxsplit=2)
        if len(parts) < 2 or not parts[1].isdigit():
            await message.reply_text("Usage: /rmsudo <user_id>")
            return

        user_id = int(parts[1])
        sudo_users = await persistence.get_sudo_users()
        if user_id not in sudo_users:
            await message.reply_text("User is not in the sudo list.")
            return

        await persistence.remove_sudo_user(user_id)
        await message.reply_text(f"Removed {user_id} from sudo users.")
        await _log_stage("Sudo Removed", f"Owner removed {user_id}")

    async def _handle_set_session_group(message: Message) -> None:
        if not await _owner_guard(message):
            return
        if not await _ensure_admin(message.chat.id):
            await message.reply_text("Please promote the bot to admin before setting this group.")
            return
        await persistence.save_session_group_id(message.chat.id)
        await message.reply_text(
            "✅ This group is now the session manager. Send session strings here to ingest them."
        )
        await _log_stage("Session Group Set", f"Owner set session group to {message.chat.id}")

    async def _handle_set_logs_group(message: Message) -> None:
        if not await _owner_guard(message):
            return
        if not await _ensure_admin(message.chat.id):
            await message.reply_text("Please promote the bot to admin before setting this group.")
            return
        await persistence.save_logs_group_id(message.chat.id)
        await message.reply_text("📝 Logs will now be sent to this group.")
        await _log_stage("Logs Group Set", f"Owner set logs group to {message.chat.id}")

    async def _handle_broadcast(message: Message) -> None:
        logs_group = await persistence.get_logs_group_id()
        if message.chat.id != logs_group:
            await message.reply_text("Broadcasts can only be sent from the logs group.")
            return
        if not await _is_sudo_user(getattr(message.from_user, "id", None)):
            await message.reply_text("You are not allowed to broadcast.")
            return

        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await message.reply_text("Usage: /broadcast <message>")
            return
        payload = parts[1]
        targets = await persistence.known_chats()
        success = 0
        failed = 0
        for chat_id in targets:
            try:
                await app.send_message(chat_id, payload)
                success += 1
            except Exception:
                failed += 1
        await message.reply_text(f"Broadcast sent. Success: {success}, Failed: {failed}")
        await _log_stage(
            "Broadcast",
            f"Broadcast from {message.from_user.id if message.from_user else 'unknown'} -> {success} ok / {failed} failed",
        )

    async def _handle_session_ingest(message: Message) -> None:
        session_group = await persistence.get_session_group_id()
        if not session_group or message.chat.id != session_group:
            return
        if not message.from_user or not is_owner(message.from_user.id):
            return

        text_parts = []
        if message.text:
            text_parts.append(message.text)
        if message.caption:
            text_parts.append(message.caption)

        if message.document:
            try:
                data = await message.download(in_memory=True)
                if isinstance(data, BytesIO):
                    data.seek(0)
                    text_parts.append(data.read().decode("utf-8", errors="ignore"))
            except Exception:
                await message.reply_text("Unable to read the document. Please send the session strings as text.")

        raw_text = "\n".join(filter(None, text_parts))
        sessions = list({s for s in extract_sessions_from_text(raw_text) if s})
        if not sessions:
            await message.reply_text("No session strings detected in this message.")
            return

        valid: list[str] = []
        invalid: list[str] = []
        for session in sessions:
            if await validate_session_string(session):
                valid.append(session)
            else:
                invalid.append(session)

        added = await persistence.add_sessions(valid, added_by=message.from_user.id) if valid else []
        total_saved = len(await persistence.get_sessions())
        summary = [f"Validated sessions: {len(valid)}"]
        if added:
            summary.append(f"Saved new sessions: {len(added)}")
        if invalid:
            summary.append(f"Invalid sessions: {len(invalid)}")
            await message.reply_text("Some session strings were invalid and were not saved.")

        await message.reply_text("\n".join(summary))
        await _log_stage(
            "Session Ingest",
            f"Owner saved {len(added)} sessions ({len(valid)} valid / {len(invalid)} invalid). Total stored: {total_saved}",
        )

    @app.on_callback_query(filters.regex(r"^sudo:start$"))
    async def start_report(_: Client, query: CallbackQuery) -> None:
        await _wrap_errors(_handle_start_report, query)

    @app.on_callback_query(filters.regex(r"^owner:manage$"))
    async def manage_sessions(_: Client, query: CallbackQuery) -> None:
        await _wrap_errors(_handle_owner_manage, query)

    async def _render_session_detail_rows(sessions: list[str]) -> tuple[str, InlineKeyboardMarkup]:
        session_tokens.clear()
        lines: list[str] = []
        buttons: list[list[InlineKeyboardButton]] = []
        for idx, session in enumerate(sessions, start=1):
            identity: SessionIdentity | None = await fetch_session_identity(session)
            name = identity.name if identity else "Unknown"
            username = identity.username if identity else None
            phone = identity.phone_number if identity else None
            parts = [f"{idx}. {name}"]
            if username:
                parts.append(f"@{username}")
            if phone:
                parts.append(phone)
            lines.append(" | ".join(parts))

            token = uuid.uuid4().hex[:12]
            session_tokens[token] = session
            buttons.append(
                [
                    InlineKeyboardButton(
                        f"❌ Remove {idx}", callback_data=f"owner:remove:{token}"
                    )
                ]
            )

        if not lines:
            lines.append("No valid sessions found after validation.")

        buttons.append([InlineKeyboardButton("🔄 Refresh", callback_data="owner:manage")])
        keyboard = InlineKeyboardMarkup(buttons)
        return "\n".join(lines), keyboard

    @app.on_callback_query(filters.regex(r"^owner:set_session_group$"))
    async def owner_session_hint(_: Client, query: CallbackQuery) -> None:
        await _wrap_errors(_handle_owner_session_hint, query)

    @app.on_callback_query(filters.regex(r"^owner:set_logs_group$"))
    async def owner_logs_hint(_: Client, query: CallbackQuery) -> None:
        await _wrap_errors(_handle_owner_logs_hint, query)

    async def _handle_start_report(query: CallbackQuery) -> None:
        if not query.message or not query.from_user:
            return
        if not await _is_sudo_user(query.from_user.id):
            await query.answer("Unauthorized", show_alert=True)
            return

        checking = await query.message.reply_text("🔎 Validating sessions, please wait...")
        live_sessions = await _sessions_available()
        if not live_sessions:
            if is_owner(query.from_user.id):
                await checking.edit_text(
                    "No sessions found. Please send session strings in the configured session manager group first."
                )
            else:
                await checking.edit_text("No sessions found. Please contact the bot owner.")
            return

        await _log_stage(
            "Start Report", f"User {query.from_user.id} checking in with {len(live_sessions)} sessions"
        )

        if queue.is_busy() and queue.active_user != query.from_user.id:
            position = queue.expected_position(query.from_user.id)
            notice = queued_message(position)
            if notice:
                await query.message.reply_text(notice)

        await _log_stage("Report Queue", f"User {query.from_user.id} position set")

        state = states.get(query.from_user.id)
        state.reset()
        state.stage = "type"
        await checking.edit_text(f"✅ Live sessions loaded: {len(live_sessions)}")
        await query.message.reply_text("Choose report visibility", reply_markup=report_type_keyboard())
        await query.answer()

    async def _handle_owner_manage(query: CallbackQuery) -> None:
        if not query.from_user or not is_owner(query.from_user.id):
            await query.answer("Owner only", show_alert=True)
            return
        checking = await query.message.reply_text("🔎 Checking saved sessions...")
        sessions = await _sessions_available()
        detail_text, keyboard = await _render_session_detail_rows(sessions)
        await checking.edit_text(
            f"Currently stored sessions: {len(sessions)}\n\n{detail_text}",
            reply_markup=keyboard,
        )
        await _log_stage("Owner Manage", f"Owner checked sessions ({len(sessions)})")
        await query.answer()

    @app.on_callback_query(filters.regex(r"^owner:remove:(?P<token>[A-Za-z0-9]+)$"))
    async def owner_remove_session(_: Client, query: CallbackQuery) -> None:
        await _wrap_errors(_handle_owner_remove_session, query)

    async def _handle_owner_remove_session(query: CallbackQuery) -> None:
        if not query.from_user or not is_owner(query.from_user.id):
            await query.answer("Owner only", show_alert=True)
            return

        token = query.matches[0].group("token") if query.matches else None
        session = session_tokens.get(token or "")
        if not session:
            await query.answer("Session mapping expired. Refresh the list.", show_alert=True)
            return

        removed = await persistence.remove_sessions([session])
        session_tokens.pop(token, None)
        if removed:
            await query.answer("Session removed", show_alert=True)
            await query.message.reply_text("✅ Session removed from storage.")
            remaining = len(await persistence.get_sessions())
            await _log_stage(
                "Session Removed",
                f"Owner removed a session. Remaining: {remaining}",
            )
        else:
            await query.answer("Session not found", show_alert=True)

    async def _handle_owner_session_hint(query: CallbackQuery) -> None:
        if not query.from_user or not is_owner(query.from_user.id):
            await query.answer("Owner only", show_alert=True)
            return
        await query.message.reply_text(
            "Send /set_session in the target group where you'll drop session strings."
        )
        await query.answer()

    async def _handle_owner_logs_hint(query: CallbackQuery) -> None:
        if not query.from_user or not is_owner(query.from_user.id):
            await query.answer("Owner only", show_alert=True)
            return
        await query.message.reply_text("Send /set_log in the logs group to start receiving updates.")
        await query.answer()

    @app.on_callback_query(filters.regex(r"^report:type:(public|private)$"))
    async def choose_type(_: Client, query: CallbackQuery) -> None:
        await _wrap_errors(_handle_type, query)

    async def _handle_type(query: CallbackQuery) -> None:
        if not query.from_user:
            return
        if not await _is_sudo_user(query.from_user.id):
            await query.answer("Unauthorized", show_alert=True)
            return
        state = states.get(query.from_user.id)
        if state.stage not in {"type", "idle"}:
            await query.answer()
            return
        state.report_type = query.data.split(":")[-1]
        state.next_stage_after_count = (
            "awaiting_private_join" if state.report_type == "private" else "awaiting_link"
        )
        state.stage = "awaiting_count"
        await _prompt_report_count(query.message)
        await _log_stage("Report Type", f"User {query.from_user.id} chose {state.report_type}")
        await query.answer()

    @app.on_callback_query(filters.regex(r"^report:reason:[a-z_]+$"))
    async def choose_reason(_: Client, query: CallbackQuery) -> None:
        await _wrap_errors(_handle_reason, query)

    @app.on_callback_query(filters.regex(r"^report:count:(\d+)$"))
    async def choose_count(_: Client, query: CallbackQuery) -> None:
        await _wrap_errors(_handle_count, query)

    async def _handle_reason(query: CallbackQuery) -> None:
        if not query.from_user:
            return
        if not await _is_sudo_user(query.from_user.id):
            await query.answer("Unauthorized", show_alert=True)
            return
        key = query.data.split(":")[-1]
        label, code = REPORT_REASONS.get(key, ("Other", 9))
        state = states.get(query.from_user.id)
        if key == "other":
            state.stage = "awaiting_reason_text"
            state.reason_code = 9
            state.reason_text = None
            state.next_stage_after_count = "begin_report"
            await query.message.reply_text("Please type the custom reason to submit with your report.")
            await query.answer()
            return

        state.reason_code = code
        state.reason_text = label
        state.next_stage_after_count = "begin_report"
        await query.answer(f"Reason set to {label}")
        await _log_stage("Report Reason", f"User {query.from_user.id} selected {label}")
        if state.report_count is None:
            state.stage = "awaiting_count"
            await _prompt_report_count(query.message)
        else:
            await _begin_report(query.message, state)

    async def _handle_count(query: CallbackQuery) -> None:
        if not query.from_user or not await _is_sudo_user(query.from_user.id):
            await query.answer("Unauthorized", show_alert=True)
            return
        state = states.get(query.from_user.id)
        if state.stage != "awaiting_count":
            await query.answer()
            return
        try:
            count = int(query.data.rsplit(":", 1)[-1])
        except ValueError:
            await query.answer("Invalid selection", show_alert=True)
            return

        await _apply_report_count(query.message, state, count)
        await query.answer()

    @app.on_message(
        filters.private
        & filters.text
        & ~filters.command(["start", "broadcast", "set_session", "set_log", "cache_stats", "clear_cache", "clear_pyrogram_cache", "check_member"])
    )
    async def text_router(_: Client, message: Message) -> None:
        await _wrap_errors(_handle_text, message)

    async def _handle_text(message: Message) -> None:
        if not message.from_user:
            return
        if not await _is_sudo_user(message.from_user.id):
            await message.reply_text("You are not authorized to use this bot.")
            await _log_stage("Unauthorized", f"User {message.from_user.id} attempted text routing")
            return

        state = states.get(message.from_user.id)

        if state.stage == "awaiting_private_join":
            invite = (message.text or "").strip()
            if not _is_valid_target(invite):
                await message.reply_text("Send a valid invite link or @username to continue.")
                return
            joined = await _join_sessions_to_chat(invite, message)
            if not joined:
                return
            state.stage = "awaiting_link"
            await message.reply_text("✅ Joined! Now send the message link to report.")
            return

        if state.stage == "awaiting_link":
            link = (message.text or "").strip()
            if not _is_valid_link(link):
                await message.reply_text("Send a valid https://t.me/ link.")
                return
            
            # Store the link, don't parse yet - let _resolve_target_across_sessions handle it
            state.target_link = link
            state.stage = "awaiting_reason"
            await message.reply_text("Choose a report reason", reply_markup=reason_keyboard())
            await _log_stage("Target Link", f"User {message.from_user.id} provided link {state.target_link}")
            return

        if state.stage == "awaiting_count":
            try:
                count = int(message.text.strip())
                await _apply_report_count(message, state, count)
            except ValueError:
                await message.reply_text(
                    (
                        "Please enter a valid number of reports "
                        f"between {config.MIN_REPORTS} and {config.MAX_REPORTS}."
                    )
                )
            return

        if state.stage == "awaiting_reason_text":
            state.reason_text = (message.text or "").strip()
            if not state.reason_text:
                await message.reply_text("Please type a custom reason.")
                return
            state.next_stage_after_count = "begin_report"
            await _log_stage("Custom Reason", f"User {message.from_user.id} provided custom reason")
            if state.report_count is None:
                state.stage = "awaiting_count"
                await _prompt_report_count(message)
            else:
                await _begin_report(message, state)
            return

        await message.reply_text("Use Start Report to begin a new report.")

    async def _begin_report(message: Message | None, state) -> None:
        if not message or not message.from_user:
            return
        if not state.target_link:
            await message.reply_text("Send the target link first.")
            return
        if not state.report_type:
            await message.reply_text("Choose Public or Private before proceeding.")
            return
        if state.reason_text is None:
            await message.reply_text("Please choose a reason first.")
            return

        state.stage = "queued"
        state.started_at = monotonic()

        if queue.is_busy() and queue.active_user != message.from_user.id:
            await message.reply_text("⏳ Please wait while another report is in progress.")
            notice = queued_message(queue.expected_position(message.from_user.id))
            if notice:
                await message.reply_text(notice)
                await _log_stage("Queue Notice", f"User {message.from_user.id} queued")

        async def notify_position(position: int) -> None:
            if position > 1:
                notice = queued_message(position)
                if notice:
                    await message.reply_text(notice)
                    await _log_stage("Queue Update", f"User {message.from_user.id} moved to {position}")

        entry = QueueEntry(
            message.from_user.id,
            job=lambda: _run_report_job(message, state),
            notify_position=notify_position,
        )
        await queue.enqueue(entry)
        await _log_stage("Report Enqueued", f"User {message.from_user.id} job queued")

    async def _run_report_job(message: Message, state) -> None:
        try:
            result = await _execute_report(message, state)
            success = result["any_success"]
            elapsed = monotonic() - state.started_at
            status = "Success" if success else "❌ Failed"
            summary_lines = [
                "📊 Report attempt summary:",
                f"- Report type: {'Private' if state.report_type == 'private' else 'Public'}",
                f"- Target link: {state.target_link}",
                f"- Requested attempts: {result['requested']}",
                f"- Total attempts: {result['attempted']}",
                f"- Successful attempts: {result['success_count']}",
                f"- Failed attempts: {result['failure_count']}",
                f"- Sessions available: {result['total_sessions']}",
                f"- Time taken: {elapsed:.1f}s",
            ]
            await message.reply_text("\n".join([f"Report completed. Status: {status}"] + summary_lines))
            await persistence.record_report(
                {
                    "user_id": message.from_user.id,
                    "target": state.target_link,
                    "reason": state.reason_text,
                    "success": success,
                    "elapsed": elapsed,
                }
            )
            await log_report_summary(
                app,
                await persistence.get_logs_group_id(),
                user=message.from_user,
                target=state.target_link or "",
                elapsed=elapsed,
                success=success,
            )
            await _log_stage(
                "Report Completed",
                f"User {message.from_user.id} -> {state.target_link} ({'success' if success else 'fail'})",
            )
        except Exception as exc:
            logging.exception("Report failed")
            await message.reply_text("Report failed due to an unexpected error.")
            await log_error(app, await persistence.get_logs_group_id(), exc, config.OWNER_ID)
        finally:
            states.reset(message.from_user.id)

    async def _execute_report(message: Message, state) -> dict:
        sessions = await prune_sessions(persistence)
        total_sessions = len(sessions)
        requested_count = max(
            config.MIN_REPORTS, min(state.report_count or config.MIN_REPORTS, config.MAX_REPORTS)
        )
        
        if not sessions:
            await message.reply_text("No valid sessions available.")
            return {
                "any_success": False,
                "success_count": 0,
                "failure_count": 0,
                "attempted": 0,
                "total_sessions": 0,
                "requested": requested_count,
            }
    
        # Parse message ID from link
        try:
            _, msg_id = _parse_link(state.target_link, state.report_type == "private")
        except ValueError:
            await message.reply_text("Invalid target link.")
            return {
                "any_success": False,
                "success_count": 0,
                "failure_count": 0,
                "attempted": 0,
                "total_sessions": total_sessions,
                "requested": requested_count,
            }
    
        # Resolve target with the fixed resolver - now returns ALL sessions for private chats
        resolved_chat_id, available_sessions, resolution_error = await _resolve_target_across_sessions(
            state.target_link, sessions
        )
        
        if resolved_chat_id is None:
            detail = f" Details: {resolution_error}" if resolution_error else ""
            await message.reply_text(
                "Unable to resolve the target for reporting. Please verify the link and try again." + detail
            )
            return {
                "any_success": False,
                "success_count": 0,
                "failure_count": 0,
                "attempted": 0,
                "total_sessions": total_sessions,
                "requested": requested_count,
            }
        
        # Use all sessions - the resolver now returns ALL sessions for private chats
        sessions = available_sessions or sessions
        total_sessions = len(sessions)
        
        logging.info(f"📊 Target resolved to: {resolved_chat_id} (type: {'private numeric ID' if isinstance(resolved_chat_id, int) else 'username'})")
    
        await _log_stage(
            "Report Started", f"User {message.from_user.id} executing with {len(sessions)} sessions"
        )
    
        started_at = datetime.utcnow()
        start_label = started_at.strftime("%Y-%m-%d %H:%M:%S UTC")
    
        reason_code = state.reason_code if state.reason_code is not None else 9
        reason_text = state.reason_text or "Report"
        success_any = False
        success_count = 0
        failure_count = 0
        attempted = 0
        progress_message: Message | None = None
        preparing_message: Message | None = None
        stop_preparing = asyncio.Event()
        
        # ============== IMPROVED CLIENT POOL CREATION WITH AGGRESSIVE FORCE REFRESH ==============
        # Create client pool with persistent connections
        client_pool = []
        failed_clients = []
        
        try:
            for idx, session in enumerate(sessions):
                client_name = f"report_client_{idx}_{uuid.uuid4().hex[:8]}"
                workdir = os.path.join(TEMP_BASE, client_name)
                os.makedirs(workdir, exist_ok=True)
                
                client = Client(
                    name=client_name,
                    api_id=config.API_ID,
                    api_hash=config.API_HASH,
                    session_string=session,
                    workdir=workdir,
                    in_memory=True
                )
                
                try:
                    await client.start()
                    
                    # Small delay to ensure connection is stable
                    await asyncio.sleep(0.5)
                    
                    # Get self to verify client is working
                    me = await client.get_me()
                    logging.info(f"✓ Client {idx} started as {me.phone_number or me.id}")
                    
                    # If resolved_chat_id is a string (username), resolve it now
                    current_chat_id = resolved_chat_id
                    if isinstance(resolved_chat_id, str):
                        try:
                            chat = await client.get_chat(resolved_chat_id)
                            current_chat_id = chat.id
                            logging.info(f"✅ Session {idx} resolved @{resolved_chat_id} to {current_chat_id}")
                        except Exception as e:
                            logging.warning(f"❌ Session {idx} cannot resolve @{resolved_chat_id}: {e}")
                            failed_clients.append((idx, f"Failed to resolve username: {e}"))
                            await client.stop()
                            continue
                    
                    # ===== AGGRESSIVE FORCE REFRESH FOR PRIVATE CHATS =====
                    access_granted = False
                    refresh_attempts = 0
                    last_error = ""
                    
                    while not access_granted and refresh_attempts < 3:
                        refresh_attempts += 1
                        try:
                            # Try aggressive force refresh
                            success, message_text = await _force_refresh_peer(client, current_chat_id)
                            
                            if success:
                                # Verify access
                                chat = await client.get_chat(current_chat_id)
                                
                                # Try to get the specific message
                                try:
                                    msg = await client.get_messages(chat.id, msg_id)
                                    logging.info(f"✅ Client {idx} can access message {msg_id}")
                                except Exception as msg_e:
                                    if "MESSAGE_ID_INVALID" in str(msg_e):
                                        logging.warning(f"⚠️ Message {msg_id} may be deleted, but chat access is OK")
                                    else:
                                        logging.warning(f"⚠️ Client {idx} can access chat but message error: {msg_e}")
                                
                                access_granted = True
                                client_pool.append(client)
                                logging.info(f"✅ Client {idx} added to pool: {message_text}")
                                break
                            else:
                                logging.warning(f"🔄 Attempt {refresh_attempts} failed: {message_text}")
                                last_error = message_text
                                await asyncio.sleep(2)
                                
                        except Exception as e:
                            error_str = str(e)
                            if "PEER_ID_INVALID" in error_str or "Peer id invalid" in error_str:
                                logging.warning(f"🔄 Attempt {refresh_attempts}: Peer invalid, retrying...")
                            else:
                                logging.warning(f"❌ Attempt {refresh_attempts} error: {e}")
                            last_error = str(e)
                            await asyncio.sleep(2)
                    
                    if not access_granted:
                        logging.warning(f"❌ Client {idx} failed after {refresh_attempts} attempts: {last_error}")
                        failed_clients.append((idx, last_error))
                        await client.stop()
                        
                except Exception as e:
                    logging.warning(f"✗ Client {idx} failed to start: {e}")
                    failed_clients.append((idx, str(e)))
                    try:
                        await client.stop()
                    except:
                        pass
            
            if not client_pool:
                error_msg = "No sessions could access the target chat.\n"
                if failed_clients:
                    error_msg += "\nReasons:\n"
                    for idx, reason in failed_clients[:3]:
                        error_msg += f"• Session {idx}: {reason}\n"
                await message.reply_text(error_msg)
                return {
                    "any_success": False,
                    "success_count": 0,
                    "failure_count": 0,
                    "attempted": 0,
                    "total_sessions": total_sessions,
                    "requested": requested_count,
                }
            
            total_sessions = len(client_pool)
            logging.info(f"✅ Created client pool with {total_sessions} working sessions")
            
            # ============== PROGRESS DISPLAY ==============
            spinner_frames = [
                "[■□□□□□□□□□]", "[■■□□□□□□□□]", "[■■■□□□□□□□]", "[■■■■□□□□□□]",
                "[■■■■■□□□□□]", "[■■■■■■□□□□]", "[■■■■■■■□□□]", "[■■■■■■■■□□]",
                "[■■■■■■■■■□]", "[■■■■■■■■■■]"
            ]
    
            async def _animate_preparing() -> None:
                nonlocal preparing_message
                frame_idx = 0
                if not preparing_message:
                    with contextlib.suppress(Exception):
                        preparing_message = await message.reply_text(
                            "⚙️ Initializing reporting engines...\n"
                            "<code>Booting secure uplink</code>"
                        )
    
                while not stop_preparing.is_set() and preparing_message:
                    frame = spinner_frames[frame_idx % len(spinner_frames)]
                    frame_idx += 1
                    with contextlib.suppress(Exception):
                        await preparing_message.edit_text(
                            f"⚙️ Preparing {len(client_pool)} sessions...\n"
                            f"<code>{frame} warming up</code>"
                        )
                    await asyncio.sleep(0.8)
    
            prepare_task = asyncio.create_task(_animate_preparing())
            failure_logs: deque[str] = deque(maxlen=5)
    
            def _record_failure(exc: Exception | str) -> None:
                reason = exc if isinstance(exc, str) else f"{type(exc).__name__}: {exc}"
                failure_logs.appendleft(f"Attempt {attempted + 1} failed due to {reason}")
    
            def _render_progress(status: str, end_label: str | None = None) -> str:
                progress_pct = 0 if requested_count == 0 else min(
                    100, int((attempted / requested_count) * 100)
                )
                bar_width = 20
                filled = min(bar_width, max(0, int(bar_width * progress_pct / 100)))
                bar = "█" * filled + "░" * (bar_width - filled)
                elapsed = int(monotonic() - state.started_at)
                mode = "Private Group/Channel" if state.report_type == "private" else "Public Group/Channel"
                lines = [
                    "💻 Live Attempts Panel",
                    "━━━━━━━━━━━━━━━━━━",
                    f"🛰️ Status: {status}",
                    f"🗂️ Report Type: {reason_text}",
                    f"📡 Group Type: {mode}",
                    f"🔗 Link: {state.target_link}",
                    f"🎯 Target Link: {state.target_link}",
                    f"🕒 Start: {start_label}",
                    f"⏱️ Elapsed: {elapsed}s",
                    f"📦 Sessions: {total_sessions}",
                    f"🧮 Requested: {requested_count}",
                    f"🚀 Attempts: {attempted}/{requested_count}",
                    f"✅ Successful: {success_count}",
                    f"❌ Failed: {failure_count}",
                    f"🛰️ Progress: [{bar}] {progress_pct}%",
                ]
                if failure_logs:
                    lines.append("❗ Recent failures:")
                    lines.extend(f"• {entry}" for entry in failure_logs)
                if end_label:
                    lines.append(f"🏁 End: {end_label}")
                lines.append("⚡ Keeping it sleek — edits are live and safe.")
                return "\n".join(lines)
    
            # Stop animation and show progress
            stop_preparing.set()
            with contextlib.suppress(Exception):
                await prepare_task
            
            progress_message = await message.reply_text(
                f"🚀 Reporting with {total_sessions} persistent sessions\n"
                f"📊 Target: {requested_count} attempts\n\n" +
                _render_progress("⚡ Running live...")
            )
    
            if preparing_message:
                with contextlib.suppress(Exception):
                    await preparing_message.delete()
    
            async def _update_progress(status: str, end_label: str | None = None) -> None:
                if not progress_message:
                    return
                with contextlib.suppress(Exception):
                    await progress_message.edit_text(_render_progress(status, end_label=end_label))
    
            # ============== MAIN REPORT LOOP ==============
            update_interval = 2
            
            while attempted < requested_count and client_pool:
                # Round-robin through the client pool
                client = client_pool[attempted % len(client_pool)]
                
                try:
                    # Send report using persistent client
                    ok = await send_report(
                        client, resolved_chat_id if isinstance(resolved_chat_id, int) else current_chat_id, 
                        msg_id, reason_code, reason_text
                    )
                    
                    if ok:
                        success_any = True
                        success_count += 1
                    else:
                        failure_count += 1
                        _record_failure("Report returned unsuccessful")
                        
                except FloodWait as fw:
                    delay = getattr(fw, "value", 1)
                    _record_failure(f"Flood wait {delay}s")
                    
                    # Wait and retry once
                    await asyncio.sleep(delay)
                    try:
                        ok = await send_report(
                            client, resolved_chat_id if isinstance(resolved_chat_id, int) else current_chat_id,
                            msg_id, reason_code, reason_text
                        )
                        if ok:
                            success_any = True
                            success_count += 1
                        else:
                            failure_count += 1
                    except Exception as e:
                        failure_count += 1
                        _record_failure(f"Retry failed: {e}")
                        
                except Exception as exc:
                    failure_count += 1
                    _record_failure(exc)
                    
                    # If client fails, remove it from pool
                    if "Peer id invalid" in str(exc) or "not a member" in str(exc).lower():
                        logging.warning(f"Removing failed client from pool")
                        client_pool.remove(client)
                        try:
                            await client.stop()
                        except:
                            pass
                        
                finally:
                    attempted += 1
                    if attempted % update_interval == 0 or attempted == requested_count:
                        await _update_progress("⚡ Running live...")
                    
                    # Small delay between reports
                    await asyncio.sleep(1.2)
    
            final_label = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            final_status = "✅ Completed" if success_any else "❌ Completed"
            await _update_progress(final_status, end_label=final_label)
            
        finally:
            # Clean up all persistent clients
            logging.info(f"Cleaning up {len(client_pool)} clients...")
            for client in client_pool:
                try:
                    await client.stop()
                except:
                    pass
    
        return {
            "any_success": success_any,
            "success_count": success_count,
            "failure_count": failure_count,
            "attempted": attempted,
            "total_sessions": total_sessions,
            "requested": requested_count,
        }

    async def _join_sessions_to_chat(target: str, message: Message) -> bool:
        sessions = await _sessions_available()
        if not sessions:
            await message.reply_text("No sessions available. Contact the owner to add them first.")
            return False

        # Check if it's an invite link or username
        is_invite = "joinchat" in target or "+" in target or "t.me/+" in target
        
        joined = 0
        failed = 0
        already_joined = 0
        failure_reasons: deque[str] = deque(maxlen=5)
        
        for idx, session in enumerate(sessions):
            client_name = f"joiner_{idx}_{uuid.uuid4().hex[:8]}"
            workdir = os.path.join(TEMP_BASE, client_name)
            os.makedirs(workdir, exist_ok=True)
            
            client = Client(
                name=client_name,
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                session_string=session,
                workdir=workdir,
                in_memory=True
            )
            
            try:
                await client.start()
                
                if is_invite:
                    # Handle invite link
                    try:
                        await client.join_chat(target)
                        joined += 1
                        await asyncio.sleep(1)
                    except UserAlreadyParticipant:
                        already_joined += 1
                    except Exception as e:
                        failed += 1
                        failure_reasons.append(f"Session {idx + 1}: {type(e).__name__} - {e}")
                else:
                    # Try to join via username
                    try:
                        # First check if already in chat
                        try:
                            member = await client.get_chat_member(target, "me")
                            status = getattr(member, "status", "")
                            if status not in {ChatMemberStatus.KICKED, "kicked", "left"}:
                                already_joined += 1
                                continue
                        except:
                            pass
                        
                        # Try to join
                        await client.join_chat(target)
                        joined += 1
                        await asyncio.sleep(1)
                        
                    except UserAlreadyParticipant:
                        already_joined += 1
                    except Exception as e:
                        failed += 1
                        failure_reasons.append(f"Session {idx + 1}: {type(e).__name__} - {e}")
                        
            except Exception as exc:
                failed += 1
                failure_reasons.append(f"Session {idx + 1}: {type(exc).__name__} - {exc}")
            finally:
                with contextlib.suppress(Exception):
                    await client.stop()

        if joined or already_joined:
            total_ready = joined + already_joined
            details = f"(joined: {joined}, already in: {already_joined}, failed: {failed})"
            await message.reply_text(
                f"🤝 Access confirmed for {total_ready}/{len(sessions)} sessions {details}."
            )
            await _log_stage(
                "Private Join",
                f"User {message.from_user.id} joined {target} with {joined} sessions, "
                f"{already_joined} already present, {failed} failed"
            )
            return True

        detail_lines = ["Could not join the target with any session. Please verify the link."]
        if failed:
            detail_lines.append(f"❌ Failed joins: {failed}/{len(sessions)} sessions.")
        if failure_reasons:
            detail_lines.append("Recent errors:")
            detail_lines.extend(f"• {reason}" for reason in failure_reasons)

        await message.reply_text("\n".join(detail_lines))
        await _log_stage(
            "Private Join Failed",
            f"User {message.from_user.id} failed to join {target} ({failed}/{len(sessions)} failed)"
        )
        return False


# ============== HELPER FUNCTIONS (OUTSIDE register_handlers) ==============
def _is_valid_target(text: str) -> bool:
    value = (text or "").strip()
    return value.startswith("https://t.me/") or value.startswith("t.me/") or value.startswith("@")


def _is_valid_link(link: str) -> bool:
    cleaned = (link or "").strip()
    return cleaned.startswith("https://t.me/") or cleaned.startswith("t.me/")


def _parse_link(link: str, is_private: bool) -> Tuple[str | int, int]:
    cleaned = link.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "").strip("/")
    parts = [part for part in cleaned.split("/") if part]
    if len(parts) < 2:
        raise ValueError("Invalid link")

    if is_private:
        if parts[0] == "c":
            if len(parts) < 3:
                raise ValueError("Invalid private link")
            chat_id = int(f"-100{parts[1]}")
            message_id = int(parts[2])
            return chat_id, message_id

        if not parts[0].isdigit():
            raise ValueError("Invalid private link")
        chat_id = int(f"-100{parts[0]}")
        message_id = int(parts[1])
        return chat_id, message_id

    if parts[0] == "c" and len(parts) >= 3:
        chat_id = int(f"-100{parts[1]}")
        message_id = int(parts[2])
    else:
        chat_id = parts[0].lstrip("@")
        message_id = int(parts[1])
    return chat_id, message_id