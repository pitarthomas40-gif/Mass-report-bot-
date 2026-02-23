from __future__ import annotations

"""Role helpers for owner and sudo users."""

import config


def is_owner(user_id: int | None) -> bool:
    return bool(user_id) and config.OWNER_ID is not None and user_id == config.OWNER_ID


def is_sudo(user_id: int | None) -> bool:
    if user_id is None:
        return False
    if is_owner(user_id):
        return True
    return user_id in config.SUDO_USERS

