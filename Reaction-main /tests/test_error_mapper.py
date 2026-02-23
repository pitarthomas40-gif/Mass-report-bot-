import types

from bot.error_mapper import map_pyrogram_error


def test_map_pyrogram_error_unknown():
    code, detail, retry = map_pyrogram_error(Exception("oops"))
    assert code == "UNKNOWN_ERROR"
    assert "oops" in detail
    assert retry is None


def test_map_pyrogram_error_floodwait():
    Flood = type("FloodWait", (), {"value": 5})
    exc = Flood()
    code, detail, retry = map_pyrogram_error(exc)
    assert code == "FLOOD_WAIT"
    assert retry == 5
