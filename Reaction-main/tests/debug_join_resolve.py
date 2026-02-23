"""Debug script to simulate join-first-then-resolve across multiple sessions.

This uses lightweight fake Pyrogram clients and stubbed error classes so it can
run without Telegram access. It demonstrates one client failing with
PeerIdInvalid while another succeeds after joining.
"""

import asyncio
import sys
import types

# Stub pyrogram.errors for the resolver code
errors_mod = types.ModuleType("pyrogram.errors")

class RPCError(Exception):
    pass

class FloodWait(Exception):
    def __init__(self, value: int = 0) -> None:
        super().__init__(value)
        self.value = value

class PeerIdInvalid(RPCError):
    pass

class ChannelPrivate(RPCError):
    pass

class UserAlreadyParticipant(RPCError):
    pass

errors_mod.RPCError = RPCError
errors_mod.FloodWait = FloodWait
errors_mod.PeerIdInvalid = PeerIdInvalid
errors_mod.ChannelPrivate = ChannelPrivate
errors_mod.UserAlreadyParticipant = UserAlreadyParticipant

pyrogram_mod = types.ModuleType("pyrogram")
pyrogram_mod.errors = errors_mod
sys.modules.setdefault("pyrogram", pyrogram_mod)
sys.modules.setdefault("pyrogram.errors", errors_mod)

from bot import report_target_resolver as resolver  # noqa: E402


class FakeChat:
    def __init__(self, chat_id: int, title: str) -> None:
        self.id = chat_id
        self.title = title


class FakeClient:
    def __init__(self, name: str, joinable: bool, resolves: bool) -> None:
        self.name = name
        self._joinable = joinable
        self._resolves = resolves
        self._joined = False

    async def join_chat(self, target: str):
        if not self._joinable:
            raise ChannelPrivate("cannot join")
        self._joined = True
        return FakeChat(-100123, "joined")

    async def get_chat(self, target):
        if not self._resolves:
            raise PeerIdInvalid("not a member")
        return FakeChat(-100123, "example")


async def main() -> None:
    clients = [FakeClient("session_a", joinable=False, resolves=False), FakeClient("session_b", joinable=True, resolves=True)]
    result = await resolver.resolve_report_target(clients, "https://t.me/example/42", allow_join=True)
    print("Resolution result:", result)


if __name__ == "__main__":
    asyncio.run(main())
