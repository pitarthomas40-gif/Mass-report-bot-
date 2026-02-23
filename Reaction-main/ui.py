from __future__ import annotations

"""Inline keyboard builders for the reporting bot."""

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

import config

REPORT_REASONS = {
    "spam": ("Spam", 0),
    "violence": ("Violence", 1),
    "pornography": ("Pornography", 2),
    "child": ("Child Abuse", 3),
    "copyright": ("Copyright", 4),
    "fake": ("Fake", 6),
    "other": ("Other", 9),
}


def owner_panel(_: int | None = None) -> InlineKeyboardMarkup:
    """Owner dashboard with management shortcuts."""
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Start Report", callback_data="sudo:start")],
            [InlineKeyboardButton("✅ Manage Sessions", callback_data="owner:manage")],
            [InlineKeyboardButton("➕ Set Session Group", callback_data="owner:set_session_group")],
            [InlineKeyboardButton("📝 Set Logs Group", callback_data="owner:set_logs_group")],
        ]
    )


def sudo_panel(_: int) -> InlineKeyboardMarkup:
    """Panel for sudo users to begin a report."""
    return InlineKeyboardMarkup([[InlineKeyboardButton("Start Report", callback_data="sudo:start")]])


def report_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Public", callback_data="report:type:public")],
            [InlineKeyboardButton("Private", callback_data="report:type:private")],
        ]
    )


def reason_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for key, (label, _code) in REPORT_REASONS.items():
        rows.append([InlineKeyboardButton(label, callback_data=f"report:reason:{key}")])
    return InlineKeyboardMarkup(rows)


def report_count_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    # Offer a small range of options within the configured bounds.
    for value in (config.MIN_REPORTS, (config.MIN_REPORTS + config.MAX_REPORTS) // 2, config.MAX_REPORTS):
        buttons.append([InlineKeyboardButton(f"{value} Reports", callback_data=f"report:count:{value}")])
    return InlineKeyboardMarkup(buttons)


def queued_message(position: int) -> str:
    """User-facing queue notification."""
    if position <= 1:
        return ""
    return (
        "Another report is in progress.\n"
        f"You are #{position} in the queue. Please wait."
    )
