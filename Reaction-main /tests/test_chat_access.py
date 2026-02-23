import asyncio
import unittest
from unittest.mock import patch

from pyrogram.errors import FloodWait, PeerIdInvalid, UserAlreadyParticipant

from bot.chat_access import join_by_invite_safe, resolve_chat_safe


class StubChat:
    def __init__(self, chat_id: int) -> None:
        self.id = chat_id


class ResolverClient:
    def __init__(self, actions):
        self.actions = actions
        self.calls = 0
        self.name = "resolver"

    async def get_chat(self, chat_identifier):
        if self.calls >= len(self.actions):
            return StubChat(int(chat_identifier))
        action = self.actions[self.calls]
        self.calls += 1
        if isinstance(action, Exception):
            raise action
        return action

    async def join_chat(self, invite_link):
        return None


class InviteClient:
    def __init__(self, actions):
        self.actions = actions
        self.calls = 0
        self.name = "inviter"

    async def join_chat(self, invite_link):
        action = self.actions[self.calls]
        self.calls += 1
        if isinstance(action, Exception):
            raise action
        return action


class ChatAccessTest(unittest.IsolatedAsyncioTestCase):
    async def test_resolve_chat_valid(self) -> None:
        client = ResolverClient([StubChat(-100123)])
        chat, error = await resolve_chat_safe(client, -100123)
        self.assertIsInstance(chat, StubChat)
        self.assertIsNone(error)

    async def test_resolve_inaccessible_chat(self) -> None:
        client = ResolverClient([PeerIdInvalid(None)])
        chat, error = await resolve_chat_safe(client, -100999)
        self.assertIsNone(chat)
        self.assertEqual(error["status"], "inaccessible_chat")

    async def test_join_by_invite_handles_floodwait(self) -> None:
        client = InviteClient([FloodWait(1, None), UserAlreadyParticipant(None)])

        async def no_sleep(*_: object, **__: object) -> None:
            return None

        with patch("asyncio.sleep", new=no_sleep):
            with patch("random.uniform", return_value=0):
                result = await join_by_invite_safe(client, "https://t.me/+hash")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "ALREADY_JOINED")
        self.assertEqual(client.calls, 2)


if __name__ == "__main__":
    unittest.main()
