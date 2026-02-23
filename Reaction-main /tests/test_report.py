import unittest

from report import _resolve_peer_for_report


class ResolvePeerForReportTest(unittest.IsolatedAsyncioTestCase):
    async def test_numeric_string_chat_id_resolves_as_int(self) -> None:
        observed = {}

        class DummyClient:
            async def resolve_peer(self, value):  # type: ignore[no-untyped-def]
                observed["value"] = value
                return value

        client = DummyClient()

        peer = await _resolve_peer_for_report(client, "-1001234567890")

        self.assertEqual(peer, -1001234567890)
        self.assertEqual(observed["value"], -1001234567890)

    async def test_non_username_error_wraps_value_error(self) -> None:
        class DummyClient:
            async def resolve_peer(self, value):  # type: ignore[no-untyped-def]
                raise ValueError(f"Peer id invalid: {value}")

        client = DummyClient()

        with self.assertRaisesRegex(Exception, "Invalid target for reporting"):
            await _resolve_peer_for_report(client, "-100bad")


if __name__ == "__main__":
    unittest.main()
