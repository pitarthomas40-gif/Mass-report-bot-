# session_bot.py

from __future__ import annotations

"""Pyrogram client builder and session utilities."""

import contextlib
import logging
from dataclasses import dataclass
import re
import uuid
from typing import Iterable, Tuple

from pyrogram import Client
from pyrogram.errors import FloodWait, RPCError

import config
from bot.dependencies import ensure_pyrogram_creds, ensure_token
from state import ReportQueue, StateManager
from storage import build_datastore

_SESSION_ALLOWED_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _looks_like_session_string(session: str) -> bool:
    s = (session or "").strip()
    if len(s) < 64:
        return False
    # Common Pyrogram string sessions are urlsafe base64-ish.
    return bool(_SESSION_ALLOWED_RE.match(s))


async def validate_session_string(session: str) -> bool:
    """
    Ensure a Pyrogram session string can start and access basic info.

    Notes:
      - We validate by actually connecting (start + get_me).
      - Use in_memory=True + unique name to avoid SQLite/session file conflicts.
      - FloodWait is treated as "temporarily blocked" (likely valid), not invalid.
    """
    s = (session or "").strip()
    if not _looks_like_session_string(s):
        return False

    client = Client(
        name=f"validator_{uuid.uuid4().hex}",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=s,
        in_memory=True,
        no_updates=True,
    )

    try:
        await client.start()
        await client.get_me()
        return True
    except FloodWait as e:
        # Why: FloodWait can happen even for valid sessions; marking invalid would delete good sessions.
        logging.warning("Session validation hit FloodWait(%ss); treating as valid for now.", getattr(e, "value", "?"))
        return True
    except RPCError as e:
        logging.warning("Session validation RPCError: %s", e.__class__.__name__)
        return False
    except Exception as e:  # noqa: BLE001
        logging.warning("Session validation failed: %s", e.__class__.__name__)
        return False
    finally:
        with contextlib.suppress(Exception):
            await client.stop()


async def validate_sessions(sessions: Iterable[str]) -> Tuple[list[str], list[str]]:
    valid: list[str] = []
    invalid: list[str] = []
    for session in sessions:
        if await validate_session_string(session):
            valid.append(session)
        else:
            invalid.append(session)
    return valid, invalid


async def prune_sessions(persistence, *, announce: bool = False) -> list[str]:
    """Remove invalid sessions and return the surviving ones."""
    sessions = await persistence.get_sessions()
    if not sessions:
        return []
    valid, invalid = await validate_sessions(sessions)
    if invalid:
        await persistence.remove_sessions(invalid)
        if announce:
            logging.warning("Removed %s invalid sessions", len(invalid))
    return valid


@dataclass
class SessionIdentity:
    """Minimal identity details for a Pyrogram session."""

    session: str
    name: str
    username: str | None
    phone_number: str | None


async def fetch_session_identity(session: str) -> SessionIdentity | None:
    """Return the user identity fields for a session string, if accessible."""

    s = (session or "").strip()
    if not _looks_like_session_string(s):
        return None

    client = Client(
        name=f"identity_{uuid.uuid4().hex}",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=s,
        in_memory=True,
        no_updates=True,
    )

    try:
        await client.start()
        me = await client.get_me()
        display_name = " ".join(filter(None, [getattr(me, "first_name", ""), getattr(me, "last_name", "")]))
        display_name = display_name.strip() or "Unknown"
        username = getattr(me, "username", None)
        phone_number = getattr(me, "phone_number", None)
        return SessionIdentity(session=s, name=display_name, username=username, phone_number=phone_number)
    except FloodWait as e:
        logging.warning("Session identity lookup hit FloodWait(%ss); skipping detail fetch.", getattr(e, "value", "?"))
        return SessionIdentity(session=s, name="FloodWait", username=None, phone_number=None)
    except RPCError as e:
        logging.warning("Session identity RPCError: %s", e.__class__.__name__)
        return None
    except Exception as e:  # noqa: BLE001
        logging.warning("Session identity lookup failed: %s", e.__class__.__name__)
        return None
    finally:
        with contextlib.suppress(Exception):
            await client.stop()


def extract_sessions_from_text(text: str) -> list[str]:
    """Parse potential session strings from raw text."""
    candidates = [part.strip() for part in (text or "").split() if len(part.strip()) > 50]
    return [candidate for candidate in candidates if is_session_string(candidate)]


def is_session_string(text: str) -> bool:
    """Heuristically determine if text looks like a Pyrogram session string."""
    return bool(text and (":" in text or len(text) > 80))


def create_bot() -> tuple[Client, object, StateManager, ReportQueue]:
    ensure_token()
    ensure_pyrogram_creds()

    persistence = build_datastore(config.MONGO_URI)
    queue = ReportQueue()
    states = StateManager()

    app = Client(
        "reaction-reporter",
        bot_token=config.BOT_TOKEN,
        api_id=config.API_ID,
        api_hash=config.API_HASH,
    )

    return app, persistence, states, queue
