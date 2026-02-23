from __future__ import annotations

import asyncio

from telegram.ext import ContextTypes


def profile_state(context: ContextTypes.DEFAULT_TYPE) -> dict:
    return context.user_data.setdefault("profile", {})


def flow_state(context: ContextTypes.DEFAULT_TYPE) -> dict:
    return context.user_data.setdefault("flow", {})


def reset_flow_state(context: ContextTypes.DEFAULT_TYPE) -> dict:
    context.user_data["flow"] = {}
    return context.user_data["flow"]


def clear_report_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove conversation-specific report data so fresh runs start cleanly."""
    context.user_data.pop("flow", None)
    context.user_data.pop("report", None)


def reset_user_context(context: ContextTypes.DEFAULT_TYPE, user_id: int | None = None) -> None:
    """Clear any per-user report context and cancel running tasks."""

    task = context.user_data.pop("active_report_task", None)
    if isinstance(task, asyncio.Task) and not task.done():
        task.cancel()

    clear_report_state(context)
    context.user_data.pop("ui_state", None)


def saved_session_count(context: ContextTypes.DEFAULT_TYPE) -> int:
    return len(profile_state(context).get("saved_sessions", []))


def active_session_count(context: ContextTypes.DEFAULT_TYPE) -> int:
    return len(flow_state(context).get("sessions", []))


def ui_state(context: ContextTypes.DEFAULT_TYPE) -> dict:
    return context.user_data.setdefault("ui_state", {"history": []})


def set_view(context: ContextTypes.DEFAULT_TYPE, view: str, *, replace: bool = False) -> None:
    state = ui_state(context)
    if replace:
        state["current_view"] = view
        return

    current = state.get("current_view")
    if current:
        state.setdefault("history", []).append(current)
    state["current_view"] = view


def pop_view(context: ContextTypes.DEFAULT_TYPE) -> str | None:
    state = ui_state(context)
    history = state.get("history") or []
    if not history:
        return None

    previous = history.pop()
    state["current_view"] = previous
    return previous


def manage_selection(context: ContextTypes.DEFAULT_TYPE) -> set:
    return ui_state(context).setdefault("manage_selection", set())


def report_selection(context: ContextTypes.DEFAULT_TYPE) -> set:
    return ui_state(context).setdefault("report_selection", set())


def set_session_order(context: ContextTypes.DEFAULT_TYPE, key: str, sessions: list[str]) -> None:
    ui_state(context)[f"{key}_order"] = list(sessions)


def get_session_order(context: ContextTypes.DEFAULT_TYPE, key: str) -> list[str]:
    return list(ui_state(context).get(f"{key}_order", []))

__all__ = [
    "profile_state",
    "flow_state",
    "reset_flow_state",
    "clear_report_state",
    "reset_user_context",
    "saved_session_count",
    "active_session_count",
    "ui_state",
    "set_view",
    "pop_view",
    "manage_selection",
    "report_selection",
    "set_session_order",
    "get_session_order",
]
