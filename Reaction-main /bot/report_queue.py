from __future__ import annotations

"""Simple sequential queue per user for reporting tasks."""

import asyncio
from collections import defaultdict, deque
from typing import Awaitable, Callable


class ReportQueue:
    def __init__(self) -> None:
        self._queues: dict[int, deque[Callable[[], Awaitable]]] = defaultdict(deque)
        self._locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)

    def is_running(self, user_id: int) -> bool:
        return self._locks[user_id].locked()

    async def enqueue(self, user_id: int, job: Callable[[], Awaitable]) -> None:
        self._queues[user_id].append(job)
        await self._process_queue(user_id)

    async def _process_queue(self, user_id: int) -> None:
        lock = self._locks[user_id]
        if lock.locked():
            return

        async with lock:
            while self._queues[user_id]:
                job = self._queues[user_id][0]
                try:
                    await job()
                finally:
                    self._queues[user_id].popleft()


__all__ = ["ReportQueue"]
