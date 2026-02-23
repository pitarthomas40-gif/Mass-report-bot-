import pytest

from bot.link_parser import ParsedTelegramLink, maybe_parse_join_target, parse_join_target


@pytest.mark.parametrize(
    "text, expected_type, invite_hash, username",
    [
        ("https://t.me/+OJ_9RbucSzhjMGVl", "invite", "OJ_9RbucSzhjMGVl", None),
        ("https://t.me/joinchat/OJ_9RbucSzhjMGVl", "invite", "OJ_9RbucSzhjMGVl", None),
        ("tg://join?invite=OJ_9RbucSzhjMGVl", "invite", "OJ_9RbucSzhjMGVl", None),
        ("+OJ_9RbucSzhjMGVl", "invite", "OJ_9RbucSzhjMGVl", None),
        ("https://t.me/+abc123.", "invite", "abc123", None),
        ("@publicchannel", "public", None, "publicchannel"),
        ("https://t.me/channel_name", "public", None, "channel_name"),
        ("t.me/channel_name/", "public", None, "channel_name"),
        ("https://t.me/channel_name/12345", "public", None, "channel_name"),
        ("channel_name", "public", None, "channel_name"),
        ("tg://join?invite=hashvalue)", "invite", "hashvalue", None),
        ("https://t.me/joinchat/hashvalue,", "invite", "hashvalue", None),
    ],
)
def test_parse_join_target_variants(text, expected_type, invite_hash, username):
    parsed = parse_join_target(text)
    assert parsed.type == expected_type
    assert parsed.invite_hash == invite_hash
    assert parsed.username == username
    assert parsed.normalized_url.startswith("https://t.me/")


def test_invalid_link_returns_none():
    assert maybe_parse_join_target("not a link") is None
    assert maybe_parse_join_target("") is None


def test_invite_normalization_strips_trailing_punctuation():
    parsed = parse_join_target("https://t.me/+abc123,,,")
    assert parsed.invite_hash == "abc123"
    assert parsed.normalized_url == "https://t.me/+abc123"
