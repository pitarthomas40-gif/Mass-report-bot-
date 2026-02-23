from __future__ import annotations

import asyncio
import logging
import contextlib

import httpx
from telegram.error import NetworkError, TimedOut
from telegram.ext import (
    AIORateLimiter,
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest

from bot.constants import (
    ADD_SESSIONS,
    API_HASH_STATE,
    API_ID_STATE,
    PRIVATE_INVITE,
    PRIVATE_MESSAGE,
    PUBLIC_MESSAGE,
    REPORT_COUNT,
    REPORT_MESSAGE,
    REPORT_REASON_TYPE,
    REPORT_SESSIONS,
    SESSION_PICK,
    REPORT_URLS,
    SESSION_MODE,
    STORY_URL,
    TARGET_KIND,
)
from bot.dependencies import ensure_token
from sudo import addsudo_command, auth_guard, rmsudo_command, sudolist_command
from bot.handlers import (
    cancel,
    error_handler,
    handle_action_buttons,
    handle_add_sessions,
    handle_api_hash,
    handle_api_id,
    handle_confirmation,
    handle_navigation,
    handle_private_invite,
    handle_private_message_link,
    handle_public_message_link,
    handle_reason_message,
    handle_reason_type,
    handle_report_again,
    handle_report_count,
    handle_report_urls,
    handle_manage_sessions_action,
    handle_session_selection,
    handle_session_mode,
    handle_sessions,
    handle_saved_navigation,
    handle_status_chip,
    handle_story_url,
    handle_target_kind,
    help_command,
    ping_command,
    restart_command,
    restart_callback,
    receive_added_sessions,
    show_sessions,
    start,
    start_report,
    uptime_command,
)

DEFAULT_POLL_TIMEOUT = 30  # Keep getUpdates sockets alive longer to avoid ReadTimeout during shutdown.


def build_app() -> Application:
    # Configure HTTPXRequest directly so timeouts are set in one place. The builder
    # level ``get_updates_*_timeout`` helpers are incompatible with a custom
    # request instance in PTB v20+, which caused startup crashes.
    request = HTTPXRequest(
        connect_timeout=5,  # Fast failure when Heroku restarts the dyno.
        # Align the underlying HTTP read timeout with the long polling timeout.
        read_timeout=DEFAULT_POLL_TIMEOUT,
        write_timeout=20,
        pool_timeout=5,
    )

    application = (
        ApplicationBuilder()
        .token(ensure_token())
        .rate_limiter(AIORateLimiter())
        .concurrent_updates(True)
        # Explicit request settings avoid httpx.ReadTimeout seen during shutdown/restart.
        .request(request)
        .build()
    )

    nav_handler = CallbackQueryHandler(handle_navigation, pattern=r"^nav:")

    report_conversation = ConversationHandler(
        entry_points=[
            CommandHandler("report", start_report),
            CallbackQueryHandler(handle_action_buttons, pattern=r"^action:"),
            CallbackQueryHandler(handle_session_mode, pattern=r"^session_mode:"),
            CallbackQueryHandler(handle_report_again, pattern=r"^report_again$"),
        ],
        states={
            API_ID_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_api_id), nav_handler],
            API_HASH_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_api_hash), nav_handler],
            REPORT_SESSIONS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_sessions), nav_handler],
            SESSION_MODE: [CallbackQueryHandler(handle_session_mode, pattern=r"^session_mode:"), nav_handler],
            SESSION_PICK: [CallbackQueryHandler(handle_session_selection, pattern=r"^session_select:"), nav_handler],
            TARGET_KIND: [CallbackQueryHandler(handle_target_kind, pattern=r"^kind:"), nav_handler],
            REPORT_URLS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_report_urls), nav_handler],
            PRIVATE_INVITE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_private_invite), nav_handler],
            PRIVATE_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_private_message_link), nav_handler],
            PUBLIC_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_public_message_link), nav_handler],
            STORY_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_story_url), nav_handler],
            REPORT_REASON_TYPE: [CallbackQueryHandler(handle_reason_type, pattern=r"^reason:"), nav_handler],
            REPORT_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_reason_message), nav_handler],
            REPORT_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_report_count), nav_handler],
            ADD_SESSIONS: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_added_sessions), nav_handler],
            ConversationHandler.WAITING: [CallbackQueryHandler(handle_confirmation, pattern=r"^confirm:"), nav_handler],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
        # Track the conversation per-user (not per-message) so callback query
        # updates from inline buttons stay inside the flow. PTB emits a warning
        # when per_message=True with CallbackQueryHandler entries, so we make
        # the conversation user-scoped and message-agnostic here.
        per_message=False,
        per_chat=False,
        per_user=True,
    )

    add_sessions_conv = ConversationHandler(
        entry_points=[CommandHandler("addsessions", handle_add_sessions)],
        states={ADD_SESSIONS: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_added_sessions)]},
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
        # Keep callback query navigation working by tracking per-user only.
        per_message=False,
        per_chat=False,
        per_user=True,
    )

    application.add_handler(MessageHandler(filters.ALL, auth_guard), group=-1)
    application.add_handler(CallbackQueryHandler(auth_guard), group=-1)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("uptime", uptime_command))
    application.add_handler(CommandHandler("ping", ping_command))
    application.add_handler(CommandHandler("restart", restart_command))
    application.add_handler(CommandHandler("addsudo", addsudo_command))
    application.add_handler(CommandHandler("rmsudo", rmsudo_command))
    application.add_handler(CommandHandler("sudolist", sudolist_command))
    application.add_handler(CommandHandler("sessions", show_sessions))
    application.add_handler(add_sessions_conv)
    application.add_handler(report_conversation)
    application.add_handler(CallbackQueryHandler(restart_callback, pattern=r"^restart$"))
    application.add_handler(CallbackQueryHandler(handle_manage_sessions_action, pattern=r"^manage:"))
    application.add_handler(CallbackQueryHandler(handle_saved_navigation, pattern=r"^saved:"))
    application.add_handler(CallbackQueryHandler(handle_session_mode, pattern=r"^session_mode:"), group=1)
    application.add_handler(CallbackQueryHandler(handle_session_selection, pattern=r"^session_select:"), group=1)
    application.add_handler(CallbackQueryHandler(handle_status_chip, pattern=r"^status:"))
    application.add_handler(CallbackQueryHandler(handle_confirmation, pattern=r"^confirm:"))

    application.add_error_handler(error_handler)
    return application


async def run_polling(application: Application, shutdown_event: asyncio.Event) -> None:
    """Run the bot until ``shutdown_event`` is set."""

    backoff_seconds = 1
    # Application lifecycle is managed explicitly so every coroutine is awaited
    # and the single asyncio loop owned by ``asyncio.run`` stays in control. This
    # avoids "shutdown was never awaited" warnings and prevents closing a loop
    # that is still running.
    while not shutdown_event.is_set():
        try:
            logging.info("Bot starting polling cycle.")
            await application.initialize()
            await application.start()
            await application.updater.start_polling(
                timeout=DEFAULT_POLL_TIMEOUT,
                drop_pending_updates=True,  # Avoid re-processing updates after Heroku restarts.
            )

            logging.info("Bot started and polling.")
            backoff_seconds = 1
            await shutdown_event.wait()
        except asyncio.CancelledError:
            raise
        except (NetworkError, TimedOut, httpx.ReadTimeout) as exc:
            # Avoid noisy stack traces on transient timeouts observed during dyno restarts.
            logging.warning("Telegram network error: %s. Retrying in %s seconds.", exc, backoff_seconds)
        except Exception:
            logging.exception("Polling crashed unexpectedly. Retrying in %s seconds.", backoff_seconds)
        finally:
            try:
                with contextlib.suppress(TimedOut, httpx.ReadTimeout):
                    await application.updater.stop()
                await application.stop()
                await application.shutdown()
            except Exception:
                logging.exception("Error while shutting down application components")

        if shutdown_event.is_set():
            logging.info("Shutdown event set; exiting polling loop.")
            break

        await asyncio.sleep(backoff_seconds)
        backoff_seconds = min(backoff_seconds * 2, 60)


__all__ = ["build_app", "run_polling"]
