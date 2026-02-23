#!/usr/bin/env python3
"""Entry point for the session-based reporting bot."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import signal

from pyrogram import idle
from pyrogram.errors import ApiIdInvalid, BadRequest

import config
from handlers import register_handlers
from session_bot import create_bot

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s: %(message)s")


async def start_bot() -> None:
    app, persistence, states, queue = create_bot()

    sudo_users = await persistence.get_sudo_users()
    config.SUDO_USERS = set(sudo_users)  # type: ignore[attr-defined]
    
    register_handlers(app, persistence, states, queue)

    try:
        await app.start()
    except ApiIdInvalid:
        logging.error(
            "Telegram rejected API_ID/API_HASH. Set valid values from https://my.telegram.org; "
            "session strings alone cannot be used without working API credentials."
        )
        return
    except BadRequest as exc:
        logging.error("Failed to start bot: %s", exc)
        return

    logging.info("Bot started and ready.")

    shutdown_event = asyncio.Event()

    def _graceful_stop(*_args) -> None:
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for signame in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signame, _graceful_stop)

    waiters = [asyncio.create_task(shutdown_event.wait()), asyncio.create_task(idle())]
    await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)

    for waiter in waiters:
        if not waiter.done():
            waiter.cancel()

    await app.stop()
    await persistence.close()


def main() -> None:
    asyncio.run(start_bot())


if __name__ == "__main__":
    main()
