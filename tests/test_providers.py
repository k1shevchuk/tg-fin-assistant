import re
from datetime import datetime, timedelta, timezone

import pytest
import responses

from app import providers
from app.config import settings


@pytest.fixture(autouse=True)
def clear_provider_caches():
    providers._KEY_RATE_CACHE = None
    providers._INDEX_CACHE.clear()
    providers._SECURITY_CACHE.clear()
    providers._BOARD_CACHE.clear()
    providers._HISTORY_CACHE.clear()
    providers._QUOTE_CACHE.clear()
    responses.reset()
    yield
    providers._KEY_RATE_CACHE = None
    providers._INDEX_CACHE.clear()
    providers._SECURITY_CACHE.clear()
    providers._BOARD_CACHE.clear()
    providers._HISTORY_CACHE.clear()
    providers._QUOTE_CACHE.clear()
    responses.reset()


@responses.activate
def test_resolve_source_detects_moex_board():
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/securities/SBER\.json\?iss\.meta=off&iss\.only=boards"
        ),
        json={
            "boards": {
                "columns": ["boardid", "is_traded", "market", "engine"],
                "data": [["TQBR", 1, "shares", "stock"]],
            }
        },
    )

    route = providers.resolve_source("SBER")

    assert route.name == "MOEX"
    assert route.board == "TQBR"
    assert route.market == "shares"
    assert route.is_traded is True


def test_resolve_source_aggregator_for_delisted():
    route = providers.resolve_source("YNDX")
    assert route.name == "AGGREGATOR"
    assert route.reason == "moex_delisting_announced"
    assert route.symbol.endswith("YNDX.US")


@responses.activate
def test_get_quote_moex_returns_price_and_metadata():
    boards_payload = {
        "boards": {
            "columns": ["boardid", "is_traded", "market", "engine"],
            "data": [["TQBR", 1, "shares", "stock"]],
        }
    }
    securities_payload = {
        "securities": {
            "columns": ["SECID", "FACEUNIT", "LOTSIZE"],
            "data": [["SBER", "SUR", 10]],
        },
        "boards": boards_payload["boards"],
    }
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/securities/SBER\.json\?iss\.meta=off&iss\.only=boards"
        ),
        json=boards_payload,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://iss\.moex\.com/iss/securities/SBER\.json\?iss\.meta=off"),
        json=securities_payload,
    )
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SBER\.json.*"
        ),
        json={
            "marketdata": {
                "columns": [
                    "BOARDID",
                    "LAST",
                    "VOLTODAY",
                    "VALTODAY",
                    "LASTCHANGEPRCNT",
                    "SYSTIME",
                    "LOTSIZE",
                ],
                "data": [
                    [
                        "TQBR",
                        250.5,
                        123456,
                        4567890,
                        1.25,
                        "2024-10-01 12:00:00",
                        10,
                    ]
                ],
            }
        },
    )

    quote = providers.get_quote("SBER")

    assert quote.source == "MOEX"
    assert quote.board == "TQBR"
    assert quote.price == pytest.approx(250.5)
    assert quote.currency == "RUB"
    assert quote.lot == 10
    assert quote.volume == pytest.approx(123456)
    assert quote.value == pytest.approx(4567890)
    assert quote.change == pytest.approx(1.25)
    assert quote.ts_utc is not None


@responses.activate
def test_get_quote_falls_back_to_daily_close():
    boards_payload = {
        "boards": {
            "columns": ["boardid", "is_traded", "market", "engine"],
            "data": [["TQBR", 1, "shares", "stock"]],
        }
    }
    securities_payload = {
        "securities": {
            "columns": ["SECID", "FACEUNIT", "LOTSIZE"],
            "data": [["SBER", "SUR", 10]],
        },
        "boards": boards_payload["boards"],
    }
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/securities/SBER\.json\?iss\.meta=off&iss\.only=boards"
        ),
        json=boards_payload,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://iss\.moex\.com/iss/securities/SBER\.json\?iss\.meta=off"),
        json=securities_payload,
    )
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SBER\.json.*"
        ),
        json={
            "marketdata": {
                "columns": ["BOARDID", "SYSTIME"],
                "data": [["TQBR", "2024-10-01 12:00:00"]],
            }
        },
    )
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities/SBER\.json.*"
        ),
        json={
            "history": {
                "columns": ["TRADEDATE", "CLOSE"],
                "data": [["2024-10-01", 248.0]],
            }
        },
    )

    quote = providers.get_quote("SBER")

    assert quote.price == pytest.approx(248.0)
    assert quote.reason == "eod_close_fallback"


@responses.activate
def test_get_quote_aggregator_without_keys(monkeypatch):
    monkeypatch.delenv("TWELVEDATA_API_KEY", raising=False)
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    monkeypatch.setattr(settings, "TWELVEDATA_API_KEY", "")
    monkeypatch.setattr(settings, "FINNHUB_API_KEY", "")

    responses.add(
        responses.GET,
        re.compile(r"https://api\.twelvedata\.com/quote.*"),
        status=401,
        json={"status": "error", "code": 401, "message": "Unauthorized"},
    )

    def _fail_finnhub(*args, **kwargs):
        raise AssertionError("Finnhub should not be called")

    monkeypatch.setattr(providers, "_fetch_finnhub_quote", _fail_finnhub)

    route = providers.resolve_source("YNDX")

    monkeypatch.setattr(settings, "TWELVEDATA_API_KEY", "invalid-key")
    with pytest.raises(providers.AggregatorAuthError):
        providers._fetch_twelvedata_quote("YNDX", route)

    monkeypatch.setattr(settings, "TWELVEDATA_API_KEY", "")

    quote = providers.get_quote("YNDX")

    assert quote.source == "TWELVEDATA"
    assert quote.price is None
    assert quote.reason == "missing_api_key"
    assert quote.context == "moex_delisting_announced"
    assert len(responses.calls) == 1


@responses.activate
def test_get_quote_aggregator_twelvedata_unauthorized(monkeypatch):
    monkeypatch.setattr(providers.settings, "TWELVEDATA_API_KEY", "bad-key")
    monkeypatch.setattr(providers.settings, "FINNHUB_API_KEY", "fallback-key")

    responses.add(
        responses.GET,
        re.compile(r"https://api\.twelvedata\.com/quote.*"),
        status=401,
        json={"status": "error", "code": 401, "message": "Unauthorized"},
    )

    quote = providers.get_quote("YNDX")

    assert quote.source == "TWELVEDATA"
    assert quote.reason == "missing_api_key"
    assert quote.price is None
    assert quote.context == "moex_delisting_announced"
    assert len(responses.calls) == 1


@responses.activate
def test_resolve_source_prefers_mtqr_for_fx():
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/securities/FXIT\.json\?iss\.meta=off&iss\.only=boards"
        ),
        json={
            "boards": {
                "columns": ["boardid", "is_traded", "market", "engine"],
                "data": [
                    ["TQTF", 0, "shares", "stock"],
                    ["MTQR", 1, "shares", "otc"],
                ],
            }
        },
    )

    route = providers.resolve_source("FXIT")

    assert route.name == "MOEX"
    assert route.board == "MTQR"
    assert route.market == "shares"
    assert route.engine == "otc"
    assert route.is_traded is True


@responses.activate
def test_get_quote_fx_uses_mtqr_board():
    boards_payload = {
        "boards": {
            "columns": ["boardid", "is_traded", "market", "engine"],
            "data": [
                ["TQTF", 0, "shares", "stock"],
                ["MTQR", 1, "shares", "otc"],
            ],
        }
    }
    securities_payload = {
        "securities": {
            "columns": ["SECID", "FACEUNIT", "LOTSIZE"],
            "data": [["FXIT", "SUR", 1]],
        },
        "boards": boards_payload["boards"],
    }
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/securities/FXIT\.json\?iss\.meta=off&iss\.only=boards"
        ),
        json=boards_payload,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://iss\.moex\.com/iss/securities/FXIT\.json\?iss\.meta=off"),
        json=securities_payload,
    )
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/engines/otc/markets/shares/boards/MTQR/securities/FXIT\.json.*"
        ),
        json={
            "marketdata": {
                "columns": ["BOARDID", "LAST", "SYSTIME", "LOTSIZE"],
                "data": [["MTQR", 132.45, "2024-10-01 10:15:00", 1]],
            }
        },
    )

    quote = providers.get_quote("FXIT")

    assert quote.source == "MOEX"
    assert quote.board == "MTQR"
    assert quote.price == pytest.approx(132.45)
    assert quote.currency == "RUB"
    assert quote.reason is None
    assert all("twelvedata" not in call.request.url.lower() for call in responses.calls)


@responses.activate
def test_get_quote_fx_history_when_inactive():
    boards_payload = {
        "boards": {
            "columns": ["boardid", "is_traded", "market", "engine"],
            "data": [
                ["TQTF", 0, "shares", "stock"],
                ["MTQR", 0, "shares", "otc"],
            ],
        }
    }
    securities_payload = {
        "securities": {
            "columns": ["SECID", "FACEUNIT", "LOTSIZE"],
            "data": [["FXGD", "SUR", 1]],
        },
        "boards": boards_payload["boards"],
    }
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/securities/FXGD\.json\?iss\.meta=off&iss\.only=boards"
        ),
        json=boards_payload,
    )
    responses.add(
        responses.GET,
        re.compile(r"https://iss\.moex\.com/iss/securities/FXGD\.json\?iss\.meta=off"),
        json=securities_payload,
    )
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/history/engines/otc/markets/shares/boards/MTQR/securities/FXGD\.json.*"
        ),
        json={
            "history": {
                "columns": ["TRADEDATE", "CLOSE"],
                "data": [["2024-09-30", 105.67]],
            }
        },
    )

    quote = providers.get_quote("FXGD")

    assert quote.source == "MOEX"
    assert quote.board == "MTQR"
    assert quote.price == pytest.approx(105.67)
    assert quote.currency == "RUB"
    assert quote.reason == "stale_price"
    assert quote.context == "no_active_trading_on_moex"
    assert all("twelvedata" not in call.request.url.lower() for call in responses.calls)


def test_get_key_rate_fetches_ruonia(monkeypatch):
    def fake_fetch(url, params=None):
        assert "ruonia" in url
        assert params == {"iss.meta": "off", "limit": 1}
        return {
            "ruonia": [
                {"TRADEDATE": "2024-10-01", "RUONIA": "13.50"},
            ]
        }

    monkeypatch.setattr(providers, "_fetch_moex_tables", fake_fetch)
    monkeypatch.setattr(providers, "_fetch_cbr_key_rate", lambda: None)
    monkeypatch.setattr(providers.settings, "KEY_RATE_FALLBACK", None, raising=False)

    rate = providers.get_key_rate()

    assert pytest.approx(rate, rel=1e-6) == 0.135

    providers.get_key_rate()


@responses.activate
def test_get_key_rate_falls_back_to_cbr():
    responses.add(
        responses.GET,
        re.compile(
            r"https://iss\.moex\.com/iss/statistics/engines/stock/markets/bonds/ruonia\.json(\?.*)?"
        ),
        status=404,
        json={},
    )
    responses.add(
        responses.GET,
        "https://www.cbr-xml-daily.ru/daily_json.js",
        json={"KeyRate": "16"},
    )

    rate = providers.get_key_rate()

    assert pytest.approx(rate, rel=1e-6) == 0.16
    assert len(responses.calls) == 2


@responses.activate
def test_get_index_value_parses_payload():
    responses.add(
        responses.GET,
        "https://iss.moex.com/iss/statistics/engines/stock/markets/index/securities/IMOEX.json",
        json={
            "securities": [
                {"CURRENTVALUE": "3210.5"},
            ]
        },
    )
    value = providers.get_index_value("IMOEX")
    assert value == pytest.approx(3210.5)
    providers.get_index_value("imoex")
    assert len(responses.calls) == 1


@responses.activate
def test_get_security_history_aggregates_rows():
    responses.add(
        responses.GET,
        re.compile(r"https://iss\.moex\.com/iss/history/engines/stock/markets/shares/securities/SBER\.json.*"),
        json={
            "history": [
                {"TRADEDATE": "2024-09-01", "CLOSE": 250, "VOLUME": 1000, "VALUE": 100000},
                {"TRADEDATE": "2024-09-02", "CLOSE": 252, "VOLUME": 900, "VALUE": 95000},
            ]
        },
    )
    history = providers.get_security_history("SBER", "TQBR", days=5)
    assert len(history) == 2
    assert history[0]["TRADEDATE"] == "2024-09-01"
    providers.get_security_history("SBER", "TQBR", days=5)
    assert len(responses.calls) == 1
