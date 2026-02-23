from __future__ import annotations

import datetime as dt
import os
import time
from typing import TypedDict

import psutil


PROCESS_START_TIME = time.monotonic()


class HealthSnapshot(TypedDict):
    uptime_seconds: float
    cpu_percent: float
    memory_mb: float
    server_time: str
    version: str


def uptime_seconds() -> float:
    return max(0.0, time.monotonic() - PROCESS_START_TIME)


def format_duration(seconds: float | dt.timedelta) -> str:
    if isinstance(seconds, dt.timedelta):
        seconds = seconds.total_seconds()

    seconds_int = int(seconds)
    days, remainder = divmod(seconds_int, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)

    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours or parts:
        parts.append(f"{hours}h")
    if minutes or parts:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


def get_version_label() -> str:
    return (
        os.getenv("SOURCE_VERSION")
        or os.getenv("HEROKU_RELEASE_VERSION")
        or os.getenv("GIT_REV")
        or os.getenv("COMMIT_HASH")
        or "unknown"
    )


def process_health() -> HealthSnapshot:
    process = psutil.Process()
    memory_mb = process.memory_info().rss / (1024 * 1024)
    cpu_percent = process.cpu_percent(interval=None)

    return {
        "uptime_seconds": uptime_seconds(),
        "cpu_percent": cpu_percent,
        "memory_mb": memory_mb,
        "server_time": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "version": get_version_label(),
    }


__all__ = ["process_health", "format_duration", "uptime_seconds", "get_version_label", "HealthSnapshot"]
