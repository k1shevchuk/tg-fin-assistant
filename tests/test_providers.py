import re
from datetime import datetime, timedelta, timezone

import pytest
import responses

from app import providers


@pytest.fixture(autouse=True)
def clear_provider_caches():
    providers._KEY_RATE_CACHE = None
    providers._INDEX_CACHE.clear()
    providers._SECURITY_CACHE.clear()
    providers._HISTORY_CACHE.clear()
    providers._QUOTE_CACHE.clear()
    responses.reset()
    yield
    providers._KEY_RATE_CACHE = None
    providers._INDEX_CACHE.clear()
    providers._SECURITY_CACHE.clear()
    providers._HISTORY_CACHE.clear()
    providers._QUOTE_CACHE.clear()
    responses.reset()


@responses.activate
def test_resolve_source_detects_moex_board():
    responses.add(
        responses.GET,
        re.compile(r"https://iss\.moex\.com/iss/securities/SBER\.json.*"),
        json={
            "securities": {
                "columns": ["SECID"],
                "data": [["SBER"]],
            },
            "boards": {
                "columns": ["boardid", "is_traded", "market", "engine"],
                "data": [["TQBR", 1, "shares", "stock"]],
            },
        },
    )

    route = providers.resolve_source("SBER")

    assert route.name == "MOEX"
    assert route.board == "TQBR"
    assert route.market == "shares"


def test_resolve_source_aggregator_for_delisted():
    route = providers.resolve_source("FXIT")
    assert route.name == "AGGREGATOR"
    assert route.reason == "delisted_from_moex"
    assert route.symbol.endswith("FXIT.MOEX")


@responses.activate
def test_get_quote_moex_returns_price_and_metadata():
    responses.add(
        responses.GET,
        re.compile(r"https://iss\.moex\.com/iss/securities/SBER\.json.*"),
        json={
            "securities": {
                "columns": ["SECID", "FACEUNIT", "LOTSIZE"],
                "data": [["SBER", "SUR", 10]],
            },
            "boards": {
                "columns": ["boardid", "is_traded", "market", "engine"],
                "data": [["TQBR", 1, "shares", "stock"]],
            },
        },
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
    assert quote.currency == "SUR"
    assert quote.lot == 10
    assert quote.volume == pytest.approx(123456)
    assert quote.value == pytest.approx(4567890)
    assert quote.change == pytest.approx(1.25)
    assert quote.ts_utc is not None


@responses.activate
def test_get_quote_falls_back_to_daily_close():
    responses.add(
        responses.GET,
        re.compile(r"https://iss\.moex\.com/iss/securities/SBER\.json.*"),
        json={
            "securities": {
                "columns": ["SECID", "FACEUNIT", "LOTSIZE"],
                "data": [["SBER", "SUR", 10]],
            },
            "boards": {
                "columns": ["boardid", "is_traded", "market", "engine"],
                "data": [["TQBR", 1, "shares", "stock"]],
            },
        },
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


def test_get_quote_aggregator_without_keys():
    quote = providers.get_quote("FXIT")
    assert quote.source == "TWELVEDATA"
    assert quote.price is None
    assert quote.reason == "missing_api_key"
    assert quote.context == "delisted_from_moex"


@responses.activate
def test_get_key_rate_fetches_ruonia():
    responses.add(
        responses.GET,
        "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/ruonia.json",
        json={
            "ruonia": {
                "columns": ["TRADEDATE", "RUONIA"],
                "data": [["2024-10-01", "13.50"]],
            }
        },
    )
    rate = providers.get_key_rate()
    assert pytest.approx(rate, rel=1e-6) == 0.135
    providers.get_key_rate()
    assert len(responses.calls) == 1


@responses.activate
def test_get_key_rate_falls_back_to_cbr():
    responses.add(
        responses.GET,
        "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/ruonia.json",
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
