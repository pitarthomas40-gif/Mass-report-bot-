import unittest

from bot.peer_resolver import NormalizedPeerInput, NormalizedTarget, normalize_input, normalize_telegram_target


class NormalizeTelegramTargetTest(unittest.TestCase):
    def test_username_and_message_links(self) -> None:
        cases = {
            "https://t.me/NotYourskillernxt": ("NotYourskillernxt", "username", None, True),
            "t.me/ExampleUser/42": ("ExampleUser", "message", 42, True),
            "@anotheruser": ("anotheruser", "username", None, True),
            "plainusername": ("plainusername", "username", None, True),
            "https://t.me/example/123/extra": ("example", "message", 123, True),
            "https://T.ME/MixedCase": ("MixedCase", "username", None, True),
        }

        for url, (username, kind, message_id, supported) in cases.items():
            with self.subTest(url=url):
                normalized = normalize_telegram_target(url)
                self.assertEqual(normalized.username, username)
                self.assertEqual(normalized.kind, kind)
                self.assertEqual(normalized.message_id, message_id)
                self.assertEqual(normalized.supported, supported)

    def test_matrix_expectations(self) -> None:
        matrix: list[tuple[str, NormalizedTarget]] = [
            (
                "https://t.me/NotYourskillernxt",
                NormalizedTarget(raw="https://t.me/NotYourskillernxt", username="NotYourskillernxt", kind="username"),
            ),
            (
                "t.me/ExampleUser/42",
                NormalizedTarget(raw="t.me/ExampleUser/42", username="ExampleUser", kind="message", message_id=42),
            ),
            (
                "https://t.me/+abcdEFGH1234",
                NormalizedTarget(
                    raw="https://t.me/+abcdEFGH1234", username=None, kind="invite", invite="+abcdEFGH1234", supported=False
                ),
            ),
            (
                "https://t.me/joinchat/abcdEFGH1234",
                NormalizedTarget(
                    raw="https://t.me/joinchat/abcdEFGH1234", username=None, kind="invite", invite="abcdEFGH1234", supported=False
                ),
            ),
        ]

        for url, expected in matrix:
            with self.subTest(url=url):
                normalized = normalize_telegram_target(url)
                self.assertEqual(normalized.username, expected.username)
                self.assertEqual(normalized.kind, expected.kind)
                self.assertEqual(normalized.message_id, expected.message_id)
                self.assertEqual(normalized.invite, expected.invite)
                self.assertEqual(normalized.supported, expected.supported)

    def test_invite_links_marked_unsupported(self) -> None:
        cases = [
            "https://t.me/+abcdEFGH1234",
            "https://t.me/joinchat/abcdEFGH1234",
        ]

        for url in cases:
            with self.subTest(url=url):
                normalized = normalize_telegram_target(url)
                self.assertIsNone(normalized.username)
                self.assertFalse(normalized.supported)
                self.assertEqual(normalized.kind, "invite")


class NormalizeInputTest(unittest.TestCase):
    def test_username_and_numeric_inputs(self) -> None:
        cases: list[tuple[str, NormalizedPeerInput]] = [
            (
                "  @UserName  ",
                NormalizedPeerInput(
                    raw="@UserName",
                    normalized="UserName",
                    kind="username",
                    username="UserName",
                    numeric_id=None,
                    invite=None,
                ),
            ),
            (
                "https://t.me/Example",
                NormalizedPeerInput(
                    raw="https://t.me/Example",
                    normalized="Example",
                    kind="username",
                    username="Example",
                    numeric_id=None,
                    invite=None,
                ),
            ),
            (
                "-1001234567890",
                NormalizedPeerInput(
                    raw="-1001234567890",
                    normalized="-1001234567890",
                    kind="numeric",
                    username=None,
                    numeric_id=-1001234567890,
                    invite=None,
                ),
            ),
            (
                "t.me/+abcdef",
                NormalizedPeerInput(
                    raw="t.me/+abcdef",
                    normalized="+abcdef",
                    kind="invite",
                    username=None,
                    numeric_id=None,
                    invite="+abcdef",
                ),
            ),
        ]

        for raw, expected in cases:
            with self.subTest(raw=raw):
                normalized = normalize_input(raw)
                self.assertEqual(normalized.normalized, expected.normalized)
                self.assertEqual(normalized.kind, expected.kind)
                self.assertEqual(normalized.username, expected.username)
                self.assertEqual(normalized.numeric_id, expected.numeric_id)
                self.assertEqual(normalized.invite, expected.invite)


if __name__ == "__main__":
    unittest.main()
