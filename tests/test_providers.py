import re

import pytest
import responses

from app import providers


@pytest.fixture(autouse=True)
def clear_provider_caches():
    providers._KEY_RATE_CACHE = None
    providers._INDEX_CACHE.clear()
    providers._SNAPSHOT_CACHE.clear()
    providers._HISTORY_CACHE.clear()
    providers._QUOTE_CACHE.clear()
    responses.reset()
    yield
    providers._KEY_RATE_CACHE = None
    providers._INDEX_CACHE.clear()
    providers._SNAPSHOT_CACHE.clear()
    providers._HISTORY_CACHE.clear()
    providers._QUOTE_CACHE.clear()
    responses.reset()


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


@responses.activate
def test_get_security_quote_enriches_data():
    responses.add(
        responses.GET,
        "https://iss.moex.com/iss/engines/stock/markets/shares/securities/SBER.json",
        json={
            "securities": [{"BOARDID": "TQBR", "FACEUNIT": "RUB", "LOTSIZE": 10}],
            "marketdata": [
                {
                    "BOARDID": "TQBR",
                    "LAST": 250.5,
                    "VOLTODAY": 123456,
                    "VALTODAY": 4567890,
                    "LASTCHANGEPRCNT": 1.25,
                    "SYSTIME": "2024-10-01 12:00:00",
                }
            ],
        },
    )
    quote = providers.get_security_quote("SBER", "TQBR")
    assert quote.price == pytest.approx(250.5)
    assert quote.volume == pytest.approx(123456)
    assert quote.value == pytest.approx(4567890)
    assert quote.change == pytest.approx(1.25)
    assert quote.currency == "RUB"
