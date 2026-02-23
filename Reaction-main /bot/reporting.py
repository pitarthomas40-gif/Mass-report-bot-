from __future__ import annotations

import asyncio
import itertools
import logging
from copy import deepcopy
from datetime import datetime, timezone
from html import escape as html_escape
from typing import TYPE_CHECKING, Awaitable, Callable, Iterable

import config
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from bot.chat_access import resolve_chat_safe
from bot.constants import DEFAULT_REPORTS
from bot.dependencies import API_HASH, API_ID, data_store, ensure_pyrogram_creds
from bot.error_mapper import map_pyrogram_error
from bot.health import format_duration
from bot.state import reset_user_context
from bot.target_resolver import ensure_join_if_needed, fetch_target_details, parse_target, resolve_entity
from bot.ui import render_card, report_again_keyboard
from bot.utils import validate_sessions
from report import report_profile_photo

if TYPE_CHECKING:
    # Keep type information for editors without importing Pyrogram's sync wrapper
    # during module import. Pyrogram's top-level import path currently touches
    # ``asyncio.get_event_loop`` at import time, which raises under Python 3.14
    # when no loop exists yet. Delaying the import until we are already inside a
    # running event loop keeps startup stable on Heroku.
    from pyrogram.client import Client
    from pyrogram.errors import (
        BadRequest,
        FloodWait,
        PeerIdInvalid,
        RPCError,
        UsernameInvalid,
        UsernameNotOccupied,
        UserDeactivated,
    )


def _session_label(session: str) -> str:
    return f"session_{abs(hash(session)) % 10_000}" if session else "session_unknown"


StatusCallback = Callable[[dict], Awaitable[None]]


class SessionPool:
    """Manage Pyrogram client lifecycles with timeouts and bookkeeping."""

    def __init__(self, api_id: int, api_hash: str, *, start_timeout: int = 25, stop_timeout: int = 10,
                 start_concurrency: int = 10) -> None:
        self.api_id = api_id
        self.api_hash = api_hash
        self.start_timeout = max(5, start_timeout)
        self.stop_timeout = max(3, stop_timeout)
        self.start_concurrency = max(1, start_concurrency)

    async def start(self, sessions: list[str]) -> tuple[list["Client"], dict["Client", str], int, set[str]]:
        from pyrogram.client import Client
        from pyrogram.errors import UserDeactivated

        semaphore = asyncio.Semaphore(self.start_concurrency)
        clients: list[Client] = []
        client_session_map: dict[Client, str] = {}
        failed_sessions = 0
        invalid_sessions: set[str] = set()

        async def _start_single(idx: int, session: str) -> None:
            nonlocal failed_sessions
            client = Client(
                name=f"reporter_{idx}",
                api_id=self.api_id,
                api_hash=self.api_hash,
                session_string=session,
                workdir=f"/tmp/report_session_{idx}",
            )

            async with semaphore:
                try:
                    await asyncio.wait_for(client.start(), timeout=self.start_timeout)
                except UserDeactivated:
                    failed_sessions += 1
                    invalid_sessions.add(session)
                    logging.warning("Session %s is deactivated; skipping and removing.", _session_label(session))
                    return
                except Exception:
                    failed_sessions += 1
                    logging.exception("Failed to start client %s during reporting", client.name)
                    return

            clients.append(client)
            client_session_map[client] = session

        await asyncio.gather(*[_start_single(idx, session) for idx, session in enumerate(sessions)])
        return clients, client_session_map, failed_sessions, invalid_sessions

    async def close(self, clients: Iterable["Client"]) -> None:
        for client in clients:
            try:
                await asyncio.wait_for(client.stop(), timeout=self.stop_timeout)
            except Exception:
                logging.exception("Failed to stop client %s", getattr(client, "name", "unknown"))


async def run_report_job(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    job_data: dict,
    *,
    status_message=None,
    send_progress_updates: bool = True,
    suppress_final_message: bool = False,
    status_hook=None,
) -> dict:
    user = query.from_user
    chat_id = query.message.chat_id

    targets = job_data.get("targets", [])
    reasons = job_data.get("reasons", [])
    count = job_data.get("count", DEFAULT_REPORTS)
    sessions = job_data.get("sessions", [])
    api_id = job_data.get("api_id")
    api_hash = job_data.get("api_hash")
    reason_code = job_data.get("reason_code", 9)
    reason_cycle = job_data.get("reason_cycle", False)
    request_timeout = int(job_data.get("request_timeout", 25))

    if send_progress_updates:
        status_message = status_message or await context.bot.send_message(chat_id=chat_id, text="Preparing clients…")

    if sessions:
        try:
            await data_store.add_sessions(sessions, added_by=user.id if user else None)
        except Exception:
            logging.exception("Failed to persist sessions before reporting")

    messages = []
    total_success = 0
    total_failed = 0
    total_sessions_started = 0
    total_sessions_failed = 0
    halted = False
    last_error: str | None = None
    overall_started = datetime.now(timezone.utc)

    try:
        for target in targets:
            started = datetime.now(timezone.utc)

            async def _update_status(payload: dict) -> None:
                """
                IMPORTANT:
                - Telegram MARKDOWN is very strict and breaks when dynamic text contains _, *, [, ], (, ), etc.
                - Use HTML parse mode + escape dynamic parts to prevent:
                  "Can't parse entities: can't find end of the entity..."
                """
                sections: list[str] = []

                setup_lines = ["<b>Setup</b>"]
                join_state = payload.get("join", {})
                if join_state:
                    join_completed = join_state.get("completed")
                    join_errors = join_state.get("errors", 0)
                    join_status = "✅" if join_completed and join_errors == 0 else "⚠️" if join_completed else "⏳"
                    status_label = "all clients joined" if join_errors == 0 else "join attempts"
                    setup_lines.append(
                        f"- join: {join_status} {html_escape(status_label)} "
                        f"({join_state.get('joined', 0)}/{join_state.get('total', 0)})"
                    )
                    last_join_reason = join_state.get("last_reason")
                    if last_join_reason:
                        setup_lines.append(f"  last: {html_escape(str(last_join_reason))}")

                target_state = payload.get("target", {})
                if target_state:
                    validated = target_state.get("validated")
                    setup_lines.append(
                        f"- target: {'✅' if validated else '⏳'} "
                        f"{html_escape(str(target_state.get('summary', 'validating…')))}"
                    )
                    if target_state.get("error"):
                        setup_lines.append(f"  error: {html_escape(str(target_state['error']))}")

                sections.append("\n".join(setup_lines))

                report_state = payload.get("report", {})
                report_lines = ["", "<b>Reporting</b>"]
                if report_state:
                    report_lines.append(
                        f"- total reports: ✅ {report_state.get('success', 0)} | ❌ {report_state.get('failed', 0)}"
                    )
                    for client_label, info in sorted(report_state.get("clients", {}).items()):
                        status = info.get("status", "⏳")
                        reason = info.get("reason")
                        retry = info.get("retry_after")
                        detail = f"{status}" if status else "⏳"
                        if retry:
                            detail += f" (retry in {int(retry)}s)"
                        if reason:
                            detail += f": {html_escape(str(reason))}"
                        report_lines.append(
                            f"- {html_escape(str(client_label))}: {detail} (count={info.get('success', 0)})"
                        )

                sections.append("\n".join(report_lines))

                text = "\n".join(sections)

                if status_hook:
                    try:
                        await status_hook(payload)
                    except Exception:
                        logging.exception("Status hook failed")

                if send_progress_updates and status_message:
                    try:
                        await status_message.edit_text(
                            text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                    except Exception:
                        logging.exception("Failed to update status message")

            try:
                summary = await perform_reporting(
                    target,
                    reasons,
                    count,
                    sessions,
                    api_id=api_id,
                    api_hash=api_hash,
                    reason_code=reason_code,
                    reason_cycle=reason_cycle,
                    request_timeout=request_timeout,
                    invite_link=job_data.get("invite_link"),
                    status_callback=_update_status,
                )
            except Exception as exc:  # pragma: no cover - runtime safety
                logging.exception("Failed to complete reporting job for target '%s'", target)
                summary = {"success": 0, "failed": 0, "halted": True, "error": str(exc)}

            ended = datetime.now(timezone.utc)
            sessions_used = summary.get("sessions_started", len(sessions))
            duration = format_duration(ended - started)
            auto_stop_reason = None
            if summary.get("halted") and summary.get("error"):
                auto_stop_reason = summary.get("error")
            messages.append(
                "\n".join(
                    [
                        f"Target: {target}",
                        f"Reasons: {', '.join(reasons)}",
                        f"Requested: {count}",
                        f"Sessions used: {sessions_used}",
                        f"Success: {summary['success']} | Failed: {summary['failed']}",
                        f"Stopped early: {'Yes' if summary.get('halted') else 'No'}",
                        f"Duration: {duration}",
                        f"Error: {summary.get('error', 'None')}",
                        f"Auto-stop reason: {auto_stop_reason or 'None'}",
                        f"Started: {started.isoformat()}",
                        f"Ended: {ended.isoformat()}",
                    ]
                )
            )

            await data_store.record_report(
                {
                    "user_id": user.id if user else None,
                    "target": target,
                    "reasons": reasons,
                    "requested": count,
                    "sessions": sessions_used,
                    "success": summary["success"],
                    "failed": summary["failed"],
                    "started_at": started,
                    "ended_at": ended,
                    "halted": summary.get("halted", False),
                }
            )

            total_success += summary.get("success", 0)
            total_failed += summary.get("failed", 0)
            total_sessions_started += summary.get("sessions_started", 0)
            total_sessions_failed += summary.get("sessions_failed", 0)
            last_error = summary.get("error") or last_error
            halted = halted or summary.get("halted", False)

            if summary.get("halted"):
                break
    except asyncio.CancelledError:  # pragma: no cover - application shutdown
        logging.info("Report job cancelled during shutdown")
        card = render_card("Report cancelled", ["The reporting task was cancelled."], [])
        if not suppress_final_message:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"<pre>{card}</pre>",
                parse_mode=ParseMode.HTML,
                reply_markup=report_again_keyboard(),
            )
        reset_user_context(context, user.id if user else None)
        return {
            "total_success": total_success,
            "total_failed": total_failed,
            "sessions_started": total_sessions_started,
            "sessions_failed": total_sessions_failed,
            "halted": True,
            "last_error": "cancelled",
            "duration": format_duration(datetime.now(timezone.utc) - overall_started),
        }

    body_lines: list[str] = []
    for msg in messages:
        body_lines.extend(msg.splitlines())
        body_lines.append("")

    if body_lines:
        body_lines = body_lines[:-1]
    else:
        body_lines = ["No report output generated."]

    total_duration = format_duration(datetime.now(timezone.utc) - overall_started)
    footer = [
        f"Total success: {total_success} | failed: {total_failed}",
        f"Sessions started: {total_sessions_started} | failed/removed: {total_sessions_failed}",
        f"Total time: {total_duration}",
    ]
    if last_error:
        footer.append(f"Last error: {last_error}")

    title = "Report halted" if halted else "Report completed"
    card = render_card(title.title(), body_lines, footer)

    if not suppress_final_message:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"<pre>{card}</pre>",
            parse_mode=ParseMode.HTML,
            reply_markup=report_again_keyboard(),
        )

    reset_user_context(context, user.id if user else None)
    return {
        "total_success": total_success,
        "total_failed": total_failed,
        "sessions_started": total_sessions_started,
        "sessions_failed": total_sessions_failed,
        "halted": halted,
        "last_error": last_error,
        "duration": total_duration,
    }


async def perform_reporting(
    target: str,
    reasons: Iterable[str],
    total: int,
    sessions: list[str],
    *,
    api_id: int | None,
    api_hash: str | None,
    reason_code: int = 9,
    reason_cycle: bool = False,
    max_concurrency: int = 25,
    request_timeout: int = 25,
    invite_link: str | None = None,
    status_callback: StatusCallback | None = None,
) -> dict:
    """Send repeated report requests with bounded concurrency."""
    # Import Pyrogram lazily so we avoid its sync wrapper touching the default
    # event loop during module import. Python 3.14 tightened ``get_event_loop``
    # semantics, so we only import once we know an event loop is already
    # running (inside an async function owned by our single asyncio.run entry).
    from pyrogram.client import Client
    from pyrogram.errors import (
        BadRequest,
        FloodWait,
        PeerIdInvalid,
        RPCError,
        UsernameInvalid,
        UsernameNotOccupied,
        UserDeactivated,
    )

    if not (api_id and api_hash):
        ensure_pyrogram_creds()
        api_id = API_ID
        api_hash = API_HASH

    sessions_to_use = list(sessions)
    invalid_sessions: set[str] = set()

    try:
        valid_sessions, invalid_sessions = await validate_sessions(api_id, api_hash, sessions_to_use)
        invalid_sessions = set(invalid_sessions)
    except Exception:
        valid_sessions, invalid_sessions = sessions_to_use, set()

    if invalid_sessions:
        removed = await data_store.remove_sessions(invalid_sessions)
        logging.warning(
            "Pruned %s invalid session(s) before run (removed %s persisted entries).",
            len(invalid_sessions),
            removed,
        )

    pool = SessionPool(api_id, api_hash, start_timeout=request_timeout)
    clients, client_session_map, failed_sessions, newly_invalid = await pool.start(valid_sessions)
    invalid_sessions |= newly_invalid

    if invalid_sessions:
        await data_store.remove_sessions(invalid_sessions)

    if not clients:
        return {"success": 0, "failed": 0, "halted": True, "error": "No sessions could be started"}

    reason_text = "; ".join(reasons)[:512] or "No reason provided"

    try:
        target_spec = parse_target(target)
        if invite_link and not target_spec.invite_link:
            # Preserve the original normalized form but attach the invite link/hash for joining.
            invite_hash_match = None
            if "+" in invite_link:
                invite_hash_match = invite_link.split("+")[-1]
            else:
                invite_hash_match = invite_link.rsplit("/", 1)[-1]
            target_spec = target_spec.__class__(
                raw=target_spec.raw,
                normalized=target_spec.normalized,
                kind="invite" if target_spec.kind == "username" else target_spec.kind,
                username=target_spec.username,
                numeric_id=target_spec.numeric_id,
                invite_hash=invite_hash_match,
                invite_link=invite_link,
                message_id=target_spec.message_id,
                internal_id=target_spec.internal_id,
            )

        normalized_label = target_spec.username or target_spec.normalized or target_spec.raw

        async def _push_status(payload: dict) -> None:
            if status_callback:
                try:
                    await status_callback(payload)
                except Exception:
                    logging.exception("Status callback failed")

        join_progress: dict[str, dict] = {}
        joined = 0
        joined_clients: list[Client] = []
        join_total = len(clients)
        max_join_attempts = 2
        for client in clients:
            join_progress[client.name] = {"status": "JOINING", "success": 0, "reason": None, "retry_after": None}

        if target_spec.requires_join:
            await _push_status({"join": {"total": join_total, "joined": joined, "errors": 0}})
            for client in clients:
                attempts = 0
                while attempts < max_join_attempts:
                    attempts += 1
                    try:
                        join_result = await asyncio.wait_for(
                            ensure_join_if_needed(client, target_spec), timeout=request_timeout
                        )
                        if join_result.ok:
                            joined += 1
                            joined_clients.append(client)
                            join_progress[client.name].update(
                                {"status": "SUCCESS", "success": 1, "reason": join_result.reason}
                            )
                            break
                        join_progress[client.name].update({"status": "FAILED", "reason": join_result.reason})
                        if attempts >= max_join_attempts:
                            break
                    except asyncio.TimeoutError:
                        join_progress[client.name].update(
                            {"status": "TIMEOUT", "reason": "join_timeout", "retry_after": None}
                        )
                        break
                    except Exception as exc:  # noqa: BLE001
                        code, detail, wait_seconds = map_pyrogram_error(exc)
                        join_progress[client.name].update(
                            {"status": code, "reason": detail, "retry_after": wait_seconds}
                        )
                        await _push_status(
                            {
                                "join": {
                                    "total": join_total,
                                    "joined": joined,
                                    "errors": len(
                                        [c for c in join_progress.values() if c.get("status") not in {"SUCCESS", "JOINING"}]
                                    ),
                                    "last_reason": f"{code}: {detail}",
                                },
                                "report": {"clients": join_progress},
                            }
                        )
                        if code == "FLOOD_WAIT" and attempts < max_join_attempts and wait_seconds:
                            await asyncio.sleep(wait_seconds)
                            continue
                        break

            join_errors = len([c for c in join_progress.values() if c.get("status") not in {"SUCCESS", "JOINING"}])
            await _push_status(
                {
                    "join": {
                        "total": join_total,
                        "joined": joined,
                        "errors": join_errors,
                        "completed": True,
                        "last_reason": None,
                    },
                    "report": {"clients": join_progress},
                }
            )
            if joined_clients:
                clients = joined_clients
            else:
                return {
                    "success": 0,
                    "failed": 0,
                    "halted": True,
                    "error": "Join step failed for all clients",
                    "sessions_started": join_total,
                    "sessions_failed": failed_sessions,
                }

        resolved_chat_id: int | None = None

        await _push_status(
            {"target": {"validated": False, "summary": "validating target"}, "report": {"clients": join_progress}}
        )

        resolution_errors: list[str | None] = []
        resolved = None
        resolver_client = None
        for client in clients:
            try:
                resolved = await asyncio.wait_for(resolve_entity(client, target_spec), timeout=request_timeout)
            except asyncio.TimeoutError:
                resolution_errors.append("TARGET_TIMEOUT")
                continue

            if resolved.ok and resolved.chat_id:
                resolved_chat_id = int(resolved.chat_id)
                resolver_client = client
                break

            resolution_errors.append(resolved.error)
            if resolved and resolved.error in {"PeerIdInvalid", "ChannelPrivate"}:
                continue
            break

        if not resolved or not (resolved.ok and resolved_chat_id):
            return {
                "success": 0,
                "failed": 0,
                "halted": True,
                "error": resolved.error if resolved else ",".join(filter(None, resolution_errors)) or "UNRESOLVED_TARGET",
            }

        if target_spec.message_id:
            client_for_message = resolver_client or (clients[0] if clients else None)
            try:
                if client_for_message is None:
                    raise RuntimeError("No client available to fetch target message")

                message = await asyncio.wait_for(
                    client_for_message.get_messages(resolved_chat_id, target_spec.message_id), timeout=request_timeout
                )
                if message is None:
                    return {"success": 0, "failed": 0, "halted": True, "error": "MESSAGE_NOT_FOUND"}
                resolved_chat_id = int(getattr(getattr(message, "chat", None), "id", resolved_chat_id))
            except Exception as exc:  # noqa: BLE001
                code, detail, _ = map_pyrogram_error(exc)
                await _push_status(
                    {
                        "target": {
                            "validated": False,
                            "summary": "target validation failed",
                            "error": f"{code}: {detail}",
                        }
                    }
                )
                return {"success": 0, "failed": 0, "halted": True, "error": f"{code}: {detail}"}

        await _push_status(
            {
                "target": {
                    "validated": True,
                    "summary": f"chat: {resolved_chat_id}, msg_id: {target_spec.message_id or 'n/a'}",
                },
                "report": {"clients": join_progress},
            }
        )

        if target_spec.requires_join:
            for client in clients:
                try:
                    join_result = await asyncio.wait_for(
                        ensure_join_if_needed(client, target_spec), timeout=request_timeout
                    )
                except asyncio.TimeoutError:
                    logging.warning(
                        "Join attempt timed out for %s via %s", target, client.name
                    )
                    continue
                if not join_result.ok:
                    logging.warning(
                        "Join attempt failed for %s via %s: %s",
                        target,
                        client.name,
                        join_result.reason or join_result.error,
                    )

        success = 0
        failed = 0

        halted = False
        invalidated_mid_run: set[str] = set()

        client_progress = {client.name: {"success": 0, "status": "READY", "reason": None} for client in clients}

        async def report_once(client: Client, reason_choice: int | object) -> bool:
            nonlocal halted
            try:
                chat, access_error = await resolve_chat_safe(
                    client, resolved_chat_id, invite_link=target_spec.invite_link
                )
                if chat is None:
                    status = (access_error or {}).get("status")
                    detail = (access_error or {}).get("detail")
                    logging.info(
                        "Skipping report for %s via %s due to access issue: %s (%s)",
                        target,
                        client.name,
                        status,
                        detail,
                    )
                    return False

                result = await report_profile_photo(
                    client, resolved_chat_id, reason=reason_choice, reason_text=reason_text
                )
                if result:
                    client_progress[client.name]["success"] += 1
                    client_progress[client.name]["status"] = "SUCCESS"
                return result
            except FloodWait as fw:
                wait_for = getattr(fw, "value", 1)
                logging.warning("Flood wait %ss while reporting %s via %s", wait_for, target, client.name)
                await asyncio.sleep(wait_for)
                try:
                    result = await report_profile_photo(
                        client, resolved_chat_id, reason=reason_choice, reason_text=reason_text
                    )
                    if result:
                        client_progress[client.name]["success"] += 1
                        client_progress[client.name]["status"] = "SUCCESS"
                    return result
                except Exception:
                    logging.exception("Retry after flood wait failed for %s via %s", target, client.name)
                    return False
            except UserDeactivated:
                session_string = client_session_map.get(client)
                if session_string:
                    invalidated_mid_run.add(session_string)
                    await data_store.remove_sessions([session_string])
                    logging.warning(
                        "Session %s deactivated mid-run; marking as invalid.",
                        _session_label(session_string),
                    )
                return False
            except (PeerIdInvalid, UsernameInvalid, UsernameNotOccupied) as exc:
                logging.error(
                    "Peer not resolvable while reporting %s (normalized %s) via %s: %s",
                    target,
                    normalized_label,
                    client.name,
                    exc.__class__.__name__,
                )
                return False
            except (BadRequest, RPCError) as exc:
                halted = True
                logging.error(
                    "Halting report run due to RPC/BadRequest error for %s via %s: %s",
                    target,
                    client.name,
                    exc,
                )
                return False

        worker_count = max(1, min(max_concurrency, total, len(clients)))
        queue: asyncio.Queue[tuple[Client, int | object]] = asyncio.Queue()

        reason_sequence: list[int | object] = [reason_code]
        if reason_cycle:
            reason_sequence = list(range(0, 10))
        reason_iter = itertools.cycle(reason_sequence)

        for _ in range(total):
            queue.put_nowait((clients[_ % len(clients)], next(reason_iter)))

        async def worker() -> None:
            nonlocal success, failed, halted
            while True:
                if halted:
                    while not queue.empty():
                        try:
                            queue.get_nowait()
                            queue.task_done()
                        except asyncio.QueueEmpty:
                            break
                    break

                try:
                    client, chosen_reason = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                try:
                    result = await asyncio.wait_for(
                        report_once(client, chosen_reason), timeout=request_timeout
                    )
                    if result:
                        success += 1
                        client_progress[client.name]["reason"] = None
                    else:
                        failed += 1
                except asyncio.TimeoutError:
                    failed += 1
                    client_progress[client.name].update(
                        {"status": "TIMEOUT", "reason": "request_timeout", "retry_after": None}
                    )
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    code, detail, retry_after = map_pyrogram_error(exc)
                    client_progress[client.name].update(
                        {"status": code, "reason": detail, "retry_after": retry_after}
                    )
                    logging.exception("Unexpected error while reporting via %s", getattr(client, "name", "unknown"))
                finally:
                    await _push_status(
                        {
                            "target": {
                                "validated": True,
                                "summary": f"chat: {resolved_chat_id}, msg_id: {target_spec.message_id or 'n/a'}",
                            },
                            "report": {
                                "success": success,
                                "failed": failed,
                                "clients": client_progress,
                            },
                        }
                    )
                    queue.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
        await queue.join()
        await asyncio.gather(*workers)

        if invalidated_mid_run:
            await data_store.remove_sessions(invalidated_mid_run)

        return {
            "success": success,
            "failed": failed,
            "halted": halted,
            "sessions_started": len(clients),
            "sessions_failed": failed_sessions,
        }

    except ValueError as exc:
        return {"success": 0, "failed": 0, "halted": False, "error": str(exc)}
    finally:
        await pool.close(clients)


__all__ = ["run_report_job", "perform_reporting"]
