from __future__ import annotations

MENU_LIVE_STATUS = "Live"

(
    API_ID_STATE,
    API_HASH_STATE,
    REPORT_SESSIONS,
    TARGET_KIND,
    REPORT_URLS,
    REPORT_REASON_TYPE,
    REPORT_MESSAGE,
    REPORT_COUNT,
    SESSION_MODE,
    SESSION_PICK,
) = range(10)
ADD_SESSIONS = 10
PRIVATE_INVITE = 11
PRIVATE_MESSAGE = 12
PUBLIC_MESSAGE = 13
STORY_URL = 14
SESSION_PICK = 15

DEFAULT_REPORTS = 5000
MIN_REPORTS = 500
MAX_REPORTS = 7000
MIN_SESSIONS = 1
MAX_SESSIONS = 500

REASON_LABELS = {
    0: "Spam",
    1: "Violence",
    2: "Pornography",
    3: "Child abuse",
    4: "Copyright",
    5: "Geo irrelevant",
    6: "Fake account",
    7: "Illegal drugs",
    8: "Personal details",
    9: "Other",
}

__all__ = [
    "MENU_LIVE_STATUS",
    "API_ID_STATE",
    "API_HASH_STATE",
    "REPORT_SESSIONS",
    "TARGET_KIND",
    "REPORT_URLS",
    "REPORT_REASON_TYPE",
    "REPORT_MESSAGE",
    "REPORT_COUNT",
    "SESSION_MODE",
    "SESSION_PICK",
    "ADD_SESSIONS",
    "PRIVATE_INVITE",
    "PRIVATE_MESSAGE",
    "PUBLIC_MESSAGE",
    "STORY_URL",
    "DEFAULT_REPORTS",
    "MIN_REPORTS",
    "MAX_REPORTS",
    "MIN_SESSIONS",
    "MAX_SESSIONS",
    "REASON_LABELS",
]
