import unittest

from bot.target_resolver import TargetSpec, parse_target


class ParseTargetTest(unittest.TestCase):
    def test_username_and_numeric(self) -> None:
        cases = {
            "@UserName": TargetSpec(raw="@UserName", normalized="UserName", kind="username", username="UserName"),
            "-100123456": TargetSpec(raw="-100123456", normalized="-100123456", kind="numeric", numeric_id=-100123456),
            "https://t.me/example": TargetSpec(raw="https://t.me/example", normalized="example", kind="username", username="example"),
        }

        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                parsed = parse_target(raw)
                self.assertEqual(parsed.normalized, expected.normalized)
                self.assertEqual(parsed.kind, expected.kind)
                self.assertEqual(parsed.username, expected.username)
                self.assertEqual(parsed.numeric_id, expected.numeric_id)

    def test_invite_links(self) -> None:
        cases = [
            "https://t.me/+abcdEFGH1234",
            "https://t.me/joinchat/abcdEFGH1234",
        ]
        for raw in cases:
            with self.subTest(raw=raw):
                parsed = parse_target(raw)
                self.assertEqual(parsed.kind, "invite")
                self.assertTrue(parsed.invite_hash)
                self.assertTrue(parsed.invite_link)

    def test_message_links_and_internal_c(self) -> None:
        parsed_public = parse_target("https://t.me/example/42")
        self.assertEqual(parsed_public.kind, "message")
        self.assertEqual(parsed_public.username, "example")
        self.assertEqual(parsed_public.message_id, 42)

        parsed_internal = parse_target("https://t.me/c/12345/99")
        self.assertEqual(parsed_internal.kind, "internal_message")
        self.assertEqual(parsed_internal.internal_id, 12345)
        self.assertEqual(parsed_internal.message_id, 99)
        self.assertEqual(parsed_internal.normalized, "c/12345")

    def test_invalid_inputs_raise(self) -> None:
        with self.assertRaises(ValueError):
            parse_target("")
        with self.assertRaises(ValueError):
            parse_target("https://t.me/")

    def test_numeric_target_with_query(self) -> None:
        parsed = parse_target("-1003212957686?start=bot")
        self.assertEqual(parsed.kind, "numeric")
        self.assertEqual(parsed.numeric_id, -1003212957686)


if __name__ == "__main__":
    unittest.main()
