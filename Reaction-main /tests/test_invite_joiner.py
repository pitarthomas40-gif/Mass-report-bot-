import asyncio
import unittest
from unittest.mock import patch

from pyrogram.errors import ChatAdminRequired, ChannelPrivate, FloodWait, InviteHashInvalid, PeerFlood, UserAlreadyParticipant

from bot.invite_joiner import _extract_invite_hash, join_by_invite


class StubClient:
    def __init__(self, actions):
        self.actions = actions
        self.calls = 0
        self.name = "stub"

    async def join_chat(self, invite_link):
        if self.calls >= len(self.actions):
            return None
        action = self.actions[self.calls]
        self.calls += 1
        if isinstance(action, Exception):
            raise action
        return action


class ExtractInviteHashTest(unittest.TestCase):
    def test_valid_invite_variants(self) -> None:
        cases = {
            "https://t.me/+AbCd": "AbCd",
            "https://t.me/joinchat/efgh": "efgh",
            "t.me/+1234": "1234",
            "+qwerty": "qwerty",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(_extract_invite_hash(raw), expected)

    def test_invalid_links(self) -> None:
        for raw in ["", "https://example.com/test", "t.me/username", "http://t.me/joinchat"]:
            with self.subTest(raw=raw):
                self.assertIsNone(_extract_invite_hash(raw))


class JoinByInviteTest(unittest.IsolatedAsyncioTestCase):
    async def test_join_success_after_flood_wait(self) -> None:
        client = StubClient([FloodWait(1, None), None])
        async def no_sleep(*_: object, **__: object) -> None:
            return None

        with patch("asyncio.sleep", new=no_sleep):
            with patch("random.uniform", return_value=0):
                result = await join_by_invite(client, "https://t.me/+hash")
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "JOINED")
        self.assertEqual(client.calls, 2)

    async def test_rate_limited_after_retries(self) -> None:
        client = StubClient([FloodWait(1, None), FloodWait(1, None), FloodWait(1, None)])
        async def no_sleep(*_: object, **__: object) -> None:
            return None

        with patch("asyncio.sleep", new=no_sleep):
            with patch("random.uniform", return_value=0):
                result = await join_by_invite(client, "+hash")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "VALID_BUT_RATE_LIMITED")
        self.assertEqual(result["wait_seconds"], 1)

    async def test_invalid_invite(self) -> None:
        client = StubClient([InviteHashInvalid(None)])
        result = await join_by_invite(client, "https://t.me/+bad")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "INVALID_LINK")

    async def test_already_joined(self) -> None:
        client = StubClient([UserAlreadyParticipant(None)])
        result = await join_by_invite(client, "https://t.me/joinchat/hash")
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "ALREADY_JOINED")

    async def test_private_or_no_access(self) -> None:
        client = StubClient([ChannelPrivate(None)])
        result = await join_by_invite(client, "https://t.me/+hash")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "NO_ACCESS_OR_PRIVATE")

        client = StubClient([ChatAdminRequired(None)])
        result = await join_by_invite(client, "https://t.me/+hash")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "NO_ACCESS_OR_PRIVATE")

    async def test_peer_flood_rate_limit(self) -> None:
        client = StubClient([PeerFlood(None)])
        result = await join_by_invite(client, "https://t.me/+hash")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "VALID_BUT_RATE_LIMITED")

    async def test_invalid_link_parse(self) -> None:
        client = StubClient([])
        result = await join_by_invite(client, "https://example.com/foo")
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "INVALID_LINK")


if __name__ == "__main__":
    unittest.main()
