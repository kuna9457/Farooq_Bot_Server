from app.config import get_settings
from app.data.market_data import MarketData


def _with(monkeypatch, **kw):
    s = get_settings()
    for k in ("angel_api_key", "upstox_api_key", "upstox_access_token"):
        monkeypatch.setattr(s, k, kw.get(k, ""))


def test_falls_back_to_yfinance_without_keys(monkeypatch):
    _with(monkeypatch)
    for name in ("angel", "upstox", "yfinance", "bogus"):
        m = MarketData()
        m.set_provider(name)
        assert m.provider.name == "yfinance"


def test_uses_broker_when_key_present(monkeypatch):
    _with(monkeypatch, angel_api_key="k")
    m = MarketData(); m.set_provider("angel"); assert m.provider.name == "angel"
    _with(monkeypatch, upstox_api_key="k")
    m = MarketData(); m.set_provider("upstox"); assert m.provider.name == "upstox"
