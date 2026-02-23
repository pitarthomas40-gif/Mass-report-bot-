from __future__ import annotations

"""User state and queue management for the reporting bot."""

import asyncio
from dataclasses import dataclass, field
import logging
from time import monotonic
from typing import Awaitable, Callable, Dict, Optional


@dataclass
class UserState:
    """Conversation state for a user."""

    stage: str = "idle"
    report_type: Optional[str] = None
    target_link: Optional[str] = None
    reason_code: Optional[int] = None
    reason_text: Optional[str] = None
    report_count: Optional[int] = None  # ✅ NEW: Number of reports user wants
    next_stage_after_count: Optional[str] = None
    started_at: float = field(default_factory=monotonic)

    def reset(self) -> None:
        self.stage = "idle"
        self.report_type = None
        self.target_link = None
        self.reason_code = None
        self.reason_text = None
        self.report_count = None  # ✅ Reset on new session
        self.next_stage_after_count = None
        self.started_at = monotonic()


class StateManager:
    """Manage UserState instances keyed by user id."""

    def __init__(self) -> None:
        self._states: Dict[int, UserState] = {}

    def get(self, user_id: int) -> UserState:
        state = self._states.setdefault(user_id, UserState())
        return state

    def reset(self, user_id: int) -> None:
        self._states[user_id] = UserState()


class QueueEntry:
    """Item waiting to be processed by the sequential queue."""

    def __init__(
        self,
        user_id: int,
        job: Callable[[], Awaitable[None]],
        notify_position: Optional[Callable[[int], Awaitable[None]]] = None,
    ) -> None:
        self.user_id = user_id
        self.job = job
        self.notify_position = notify_position


class ReportQueue:
    """FIFO queue ensuring only one report executes at a time."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[QueueEntry] = asyncio.Queue()
        self._worker: Optional[asyncio.Task] = None
        self._active_user: Optional[int] = None
        self._on_error: Optional[Callable[[Exception], Awaitable[None]]] = None

    @property
    def active_user(self) -> Optional[int]:
        return self._active_user

    def set_error_handler(self, handler: Callable[[Exception], Awaitable[None]]) -> None:
        """Register a coroutine to run when a queued job raises."""
        self._on_error = handler

    def expected_position(self, user_id: int) -> int:
        # position is 1-based; include active job when not same user
        offset = 1 if self._active_user and self._active_user != user_id else 0
        return self._queue.qsize() + offset + 1

    async def enqueue(self, entry: QueueEntry) -> int:
        position = self.expected_position(entry.user_id)
        await self._queue.put(entry)
        if entry.notify_position:
            await entry.notify_position(position)
        if not self._worker or self._worker.done():
            self._worker = asyncio.create_task(self._run())
        return position

    async def _run(self) -> None:
        while not self._queue.empty():
            entry = await self._queue.get()
            self._active_user = entry.user_id
            try:
                await entry.job()
            except Exception as exc:
                logging.exception("Queue worker error")
                if self._on_error:
                    await self._on_error(exc)
            finally:
                self._active_user = None
                self._queue.task_done()

    def is_busy(self) -> bool:
        return bool(self._active_user) or not self._queue.empty()
