import struct
import time

from app.execution.angel import AngelFeed, totp_now


def test_totp_rfc6238_vector(monkeypatch):
    # RFC 6238 SHA1 test vector: secret "12345678901234567890", t=59 -> 94287082 (8 digits)
    import base64
    secret = base64.b32encode(b"12345678901234567890").decode()
    monkeypatch.setattr(time, "time", lambda: 59)
    assert totp_now(secret, digits=8) == "94287082"
    assert len(totp_now(secret)) == 6


def test_ws_ltp_packet_parsing():
    f = AngelFeed()
    f._by_token["3045"] = "SBIN"
    pkt = struct.pack("<bb25sqqq", 1, 1, b"3045", 7, 1700000000000, 81234)  # 812.34 INR in paise
    assert len(pkt) == 51
    f._on_binary(pkt)
    assert f.ltp["SBIN"] == 812.34
    assert f.fresh("SBIN") == 812.34


def test_ws_ignores_unknown_and_short_packets():
    f = AngelFeed()
    f._on_binary(b"\x01\x01abc")
    f._on_binary(struct.pack("<bb25sqqq", 1, 1, b"999", 1, 1, 100))
    assert f.ltp == {}
