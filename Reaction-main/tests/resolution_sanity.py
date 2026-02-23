"""Manual sanity checks for target normalization and caching."""

from __future__ import annotations

from bot.target_resolver import parse_target

SAMPLE_TARGETS = [
    "@example",
    "https://t.me/example/123",
    "https://t.me/+inviteHash",
    "-1001234567890",
]


def main() -> None:
    for raw in SAMPLE_TARGETS:
        spec = parse_target(raw)
        print(
            {
                "raw": raw,
                "kind": spec.kind,
                "normalized": spec.normalized,
                "username": spec.username,
                "invite": spec.invite_link,
            }
        )


if __name__ == "__main__":
    main()
