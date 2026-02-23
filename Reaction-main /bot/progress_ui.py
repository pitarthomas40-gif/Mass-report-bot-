from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from telegram.error import BadRequest

SpinnerFrame = tuple[str, str]


def _progress_bar(pct: int, width: int = 10) -> str:
    pct = max(0, min(100, pct))
    filled = int(width * (pct / 100))
    return "■" * filled + "□" * max(0, width - filled)


async def run_progress_animation(
    bot,
    chat_id: int,
    message_id: int,
    stop_event: asyncio.Event,
    *,
    title: str = "Processing…",
    details: Callable[[], dict[str, Any]] | None = None,
    interval: float = 0.7,
) -> None:
    """Animate a single message to show progress without spamming edits."""

    frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    steps: list[SpinnerFrame] = [
        ("🔌", "Connecting sessions"),
        ("🛡️", "Validating target"),
        ("📨", "Submitting reports"),
        ("✅", "Finalizing"),
    ]

    idx = 0
    last_text: str | None = None

    while not stop_event.is_set():
        frame = frames[idx % len(frames)]
        prefix, step_label = steps[(idx // len(frames)) % len(steps)]
        pct = min(95, int((idx % (len(frames) * len(steps))) / (len(frames) * len(steps)) * 100))
        bar = _progress_bar(pct)

        detail_state = details() if details else {}
        joined = detail_state.get("joined", 0)
        already = detail_state.get("already", 0)
        failed = detail_state.get("failed", 0)
        total = detail_state.get("total", 0)
        resolved = detail_state.get("resolved", False)

        text = (
            f"{frame} {title}\n"
            f"Step: {step_label} {prefix}\n"
            f"Progress: [{bar}] {pct}%\n"
            f"Sessions: ok {joined + already}/{total} | failed {failed}\n"
            f"Members: joined {joined}, already in {already}\n"
            f"Target: {'✅' if resolved else '…'}"
        )

        if text != last_text:
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
                last_text = text
            except BadRequest as exc:
                if "Message is not modified" in str(exc):
                    last_text = text
                else:
                    logging.debug("Progress animation edit skipped: %s", exc)
            except Exception:  # noqa: BLE001
                logging.debug("Progress animation edit failed", exc_info=True)

        idx += 1
        await asyncio.sleep(interval)

    # Final clean-up frame to avoid leaving spinner running
    if last_text:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=last_text)
        except Exception:  # noqa: BLE001
            logging.debug("Progress animation cleanup failed", exc_info=True)


__all__ = ["run_progress_animation"]
