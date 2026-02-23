import unittest

from bot.utils import _refresh_dialogs, normalize_target, resolve_target_peer


class NormalizeTargetTest(unittest.TestCase):
    def test_username_variants(self) -> None:
        inputs = [
            "https://t.me/itzdhruv",
            "t.me/itzdhruv",
            "@itzdhruv",
            "itzdhruv",
        ]

        for text in inputs:
            with self.subTest(text=text):
                normalized, details = normalize_target(text)
                self.assertEqual(normalized, "itzdhruv")
                self.assertIn(details.get("type"), {"username", "public_message", "numeric_id"})


class RefreshDialogsTest(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_dialogs_consumes_async_generator(self) -> None:
        class DummyClient:
            def __init__(self) -> None:
                self._dialogs_refreshed = False
                self.iterated = 0

            async def get_dialogs(self):
                for i in range(3):
                    self.iterated += 1
                    yield i

        client = DummyClient()

        await _refresh_dialogs(client)

        self.assertTrue(client._dialogs_refreshed)
        self.assertEqual(client.iterated, 3)


class ResolveTargetPeerTest(unittest.IsolatedAsyncioTestCase):
    async def test_numeric_channel_id_normalization(self) -> None:
        normalized, details = normalize_target("-1003212957686")
        self.assertEqual(details.get("type"), "numeric_id")
        self.assertIsInstance(details.get("id"), int)
        self.assertEqual(int(normalized), details.get("id"))

    async def test_invalid_targets_fail_gracefully(self) -> None:
        class DummyClient:
            def __init__(self) -> None:
                self._dialogs_refreshed = False

            async def get_dialogs(self):
                if getattr(self, "_dialogs_refreshed", False):
                    return
                for i in range(1):
                    yield i

            async def resolve_peer(self, value):  # type: ignore[no-untyped-def]
                raise ValueError(f"Peer id invalid: {value}")

        client = DummyClient()

        with self.assertRaises(Exception) as ctx:
            await resolve_target_peer(client, "--100abc")

        self.assertIn("invalid", str(ctx.exception).lower())




if __name__ == "__main__":
    unittest.main()
