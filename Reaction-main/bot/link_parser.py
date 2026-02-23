from __future__ import annotations

"""Utilities for validating and normalizing Telegram links/usernames for joins."""

import re
from dataclasses import dataclass
from typing import Literal
from urllib.parse import parse_qs, urlparse


@dataclass(frozen=True, slots=True)
class ParsedTelegramLink:
    type: Literal["invite", "public"]
    normalized_url: str
    invite_hash: str | None = None
    username: str | None = None


@dataclass(frozen=True, slots=True)
class ParsedAccessLink:
    """Normalized chat access link accepted by the unified flow.

    Only two shapes are allowed:
    - ``invite_hash`` for private joins (``https://t.me/+<hash>`` or ``joinchat/<hash>``)
    - ``username`` for public joins (``https://t.me/<username>`` or ``@username``)

    Message links or other paths are rejected up-front so invite hashes are
    never routed through username resolution.
    """

    kind: Literal["invite_hash", "public_username"]
    normalized: str
    invite_hash: str | None = None
    username: str | None = None


@dataclass(frozen=True, slots=True)
class ParsedMessageLink:
    raw: str
    normalized_url: str
    chat_ref: str | int
    message_id: int
    is_private: bool
    username: str | None = None
    internal_id: int | None = None


_TRAILING_PUNCTUATION = ",.;)]}>'\""


def _clean_input(text: str) -> str:
    cleaned = (text or "").strip()
    cleaned = cleaned.rstrip(_TRAILING_PUNCTUATION)
    return cleaned


def _ensure_scheme(text: str) -> str:
    if text.startswith("http") or text.startswith("tg"):
        return text
    return f"https://{text}"


def _parse_invite_hash_from_url(parsed) -> str | None:
    path_parts = [p for p in parsed.path.split("/") if p]
    if parsed.scheme == "tg" and parsed.netloc.startswith("join"):
        invite = parse_qs(parsed.query).get("invite")
        if invite and invite[0]:
            return invite[0]
    if path_parts:
        first = path_parts[0]
        if first.startswith("+"):
            return first.lstrip("+") or None
        if first.lower() == "joinchat" and len(path_parts) >= 2:
            return path_parts[1] or None
    return None


def parse_access_link(raw: str) -> ParsedAccessLink:
    """Parse chat access link for unified join flow.

    Accepted inputs:
    - https://t.me/+<hash>
    - https://t.me/joinchat/<hash>
    - @username or https://t.me/<username>

    Message links (with a message id), t.me/c internal links, or any other
    extra path segments are rejected to avoid misrouting invite hashes through
    username resolution.
    """

    cleaned = _clean_input(raw)
    if not cleaned:
        raise ValueError("Empty chat access link")

    if " " in cleaned:
        raise ValueError("Links or usernames cannot contain spaces")

    if cleaned.startswith("+"):
        invite_hash = cleaned.lstrip("+")
        if not invite_hash:
            raise ValueError("Invite hash is missing")
        return ParsedAccessLink(
            kind="invite_hash",
            normalized=f"https://t.me/+{invite_hash}",
            invite_hash=invite_hash,
        )

    if cleaned.startswith("@"):
        username = cleaned.lstrip("@")
        if not username:
            raise ValueError("Username is missing")
        return ParsedAccessLink(
            kind="public_username",
            normalized=f"https://t.me/{username}",
            username=username,
        )

    parsed = urlparse(_ensure_scheme(cleaned))
    path_parts = [p for p in parsed.path.split("/") if p]

    invite_hash = _parse_invite_hash_from_url(parsed)
    if invite_hash:
        return ParsedAccessLink(
            kind="invite_hash",
            normalized=f"https://t.me/+{invite_hash}",
            invite_hash=invite_hash,
        )

    if parsed.netloc and parsed.netloc.endswith("t.me"):
        if not path_parts:
            raise ValueError("The t.me link is missing a username")

        if len(path_parts) > 1:
            raise ValueError("Message links are not allowed here; send chat link only")

        username = path_parts[0].lstrip("@")
        if not username:
            raise ValueError("Username is missing")
        return ParsedAccessLink(
            kind="public_username",
            normalized=f"https://t.me/{username}",
            username=username,
        )

    # Bare username fallback
    return ParsedAccessLink(
        kind="public_username",
        normalized=f"https://t.me/{cleaned.lstrip('@')}",
        username=cleaned.lstrip("@"),
    )


def parse_join_target(raw: str) -> ParsedTelegramLink:
    """Parse a user supplied link/username into a joinable target.

    Accepted inputs:
    - https://t.me/+hash
    - https://t.me/joinchat/hash
    - tg://join?invite=hash
    - +hash (invite hash)
    - @username, username, https://t.me/username
    - Message links (https://t.me/username/123) are treated as public usernames
    """

    cleaned = _clean_input(raw)
    if not cleaned:
        raise ValueError("Empty link or username")
    if " " in cleaned:
        raise ValueError("Usernames or invites cannot contain spaces")

    if cleaned.startswith("+"):
        invite_hash = cleaned.lstrip("+")
        if not invite_hash:
            raise ValueError("Invite hash is missing")
        return ParsedTelegramLink(
            type="invite",
            normalized_url=f"https://t.me/+{invite_hash}",
            invite_hash=invite_hash,
        )

    if cleaned.startswith("@"):
        username = cleaned.lstrip("@")
        if not username:
            raise ValueError("Username is missing")
        return ParsedTelegramLink(
            type="public",
            normalized_url=f"https://t.me/{username}",
            username=username,
        )

    parsed = urlparse(_ensure_scheme(cleaned))
    invite_hash = _parse_invite_hash_from_url(parsed)
    if invite_hash:
        return ParsedTelegramLink(
            type="invite",
            normalized_url=f"https://t.me/+{invite_hash}",
            invite_hash=invite_hash,
        )

    if parsed.netloc and parsed.netloc.endswith("t.me"):
        path_parts = [p for p in parsed.path.split("/") if p]
        if not path_parts:
            raise ValueError("The t.me link is missing a username")
        username = path_parts[0].lstrip("@")
        if not username:
            raise ValueError("Username is missing")
        # Allow message/story suffixes; we only care about the username for joins
        username = re.sub(r"[^A-Za-z0-9_]+$", "", username)
        return ParsedTelegramLink(
            type="public",
            normalized_url=f"https://t.me/{username}",
            username=username,
        )

    if cleaned:
        return ParsedTelegramLink(
            type="public",
            normalized_url=f"https://t.me/{cleaned.lstrip('@')}",
            username=cleaned.lstrip("@"),
        )

    raise ValueError("Unsupported link or username")


def parse_message_link(raw: str) -> ParsedMessageLink:
    """Parse a Telegram message link into chat/message identifiers.

    Supports public (``https://t.me/<username>/<msg_id>``) and private
    (``https://t.me/c/<internal_id>/<msg_id>``) message links, with or without
    schemes or trailing query strings.
    """

    cleaned = _clean_input(raw)
    if not cleaned:
        raise ValueError("Empty link")

    parsed = urlparse(_ensure_scheme(cleaned))
    if not parsed.netloc or not parsed.netloc.endswith("t.me"):
        raise ValueError("Not a Telegram link")

    path_parts = [p for p in parsed.path.split("/") if p]
    if len(path_parts) < 2:
        raise ValueError("Message link missing components")

    def _extract_message_id(raw_part: str) -> int:
        digits = "".join(ch for ch in raw_part if ch.isdigit())
        if not digits:
            raise ValueError("Message id is missing")
        return int(digits)

    if path_parts[0].lower() == "c":
        if len(path_parts) < 3:
            raise ValueError("Private message link is incomplete")
        internal_id_part = path_parts[1]
        if not internal_id_part.isdigit():
            raise ValueError("Private link has non-numeric chat id")
        internal_id = int(internal_id_part)
        message_id = _extract_message_id(path_parts[2])
        normalized = f"https://t.me/c/{internal_id}/{message_id}"
        return ParsedMessageLink(
            raw=raw,
            normalized_url=normalized,
            chat_ref=int(f"-100{internal_id}"),
            message_id=message_id,
            is_private=True,
            internal_id=internal_id,
        )

    username = path_parts[0].lstrip("@")
    if not username:
        raise ValueError("Username missing from message link")
    message_id = _extract_message_id(path_parts[1])
    normalized = f"https://t.me/{username}/{message_id}"
    return ParsedMessageLink(
        raw=raw,
        normalized_url=normalized,
        chat_ref=username,
        message_id=message_id,
        is_private=False,
        username=username,
    )


def maybe_parse_message_link(raw: str) -> ParsedMessageLink | None:
    try:
        return parse_message_link(raw)
    except Exception:
        return None


def maybe_parse_access_link(raw: str) -> ParsedAccessLink | None:
    try:
        return parse_access_link(raw)
    except Exception:
        return None


def maybe_parse_join_target(raw: str) -> ParsedTelegramLink | None:
    try:
        return parse_join_target(raw)
    except Exception:
        return None


__all__ = [
    "ParsedTelegramLink",
    "ParsedMessageLink",
    "ParsedAccessLink",
    "parse_join_target",
    "maybe_parse_join_target",
    "parse_access_link",
    "maybe_parse_access_link",
    "parse_message_link",
    "maybe_parse_message_link",
]
