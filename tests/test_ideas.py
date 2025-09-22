from datetime import datetime, timedelta, timezone

import pytest

from app import ideas
from app.ideas import Idea
from app.sources import IdeaSource


class DummyAsset:
    def __init__(self, ticker: str, board: str, tag: str):
        self.ticker = ticker
        self.board = board
        self.tag = tag
        self.type = tag


class DummyQuote:
    def __init__(self, price: float = 250.0):
        self.price = price
        self.currency = "RUB"
        self.board = "TQBR"
        self.ticker = "SBER"
        self.lot = 10
        self.as_of = datetime.now(timezone.utc)
        self.change = 1.2


@pytest.fixture
def mock_providers(monkeypatch):
    history_rows = [
        {
            "TRADEDATE": (datetime(2024, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d"),
            "CLOSE": 200 + i * 0.5,
            "VOLUME": 100000 + i,
            "VALUE": 5000000 + i * 1000,
        }
        for i in range(260)
    ]

    monkeypatch.setattr(ideas, "portfolio_assets", lambda risk: [DummyAsset("SBER", "TQBR", "dividends")])
    monkeypatch.setattr(ideas, "_extra_candidates", lambda: [])
    monkeypatch.setattr(ideas, "get_security_quote", lambda ticker, board: DummyQuote())
    monkeypatch.setattr(ideas, "get_security_snapshot", lambda ticker: {"PE": "8.5", "DIVYIELD": "12.1"})
    monkeypatch.setattr(ideas, "get_security_history", lambda ticker, board, days=260: history_rows)
    monkeypatch.setattr(ideas, "get_key_rate", lambda: 0.12)
    monkeypatch.setattr(
        ideas,
        "get_market_commentary",
        lambda: {"url": "https://moex.com/analytics", "source": "MOEX"},
    )
    monkeypatch.setattr(
        ideas,
        "get_edgar_sources",
        lambda ticker: [IdeaSource(url="https://sec.gov/doc", name="SEC 10-Q", date=datetime.now(timezone.utc))],
    )
    monkeypatch.setattr(
        ideas,
        "get_latest_value",
        lambda series, label: (1.23, [IdeaSource(url="https://fred.stlouisfed.org/series/DGS10", name=label, date=datetime.now(timezone.utc))]),
    )
    monkeypatch.setattr(
        ideas,
        "get_coin_market",
        lambda coin_id, vs: (
            {
                "current_price": 62000,
                "price_change_percentage_7d_in_currency": 5.5,
                "price_change_percentage_24h_in_currency": 1.1,
                "price_change_percentage_30d_in_currency": 12.3,
                "total_volume": 123456789,
                "market_cap": 900000000,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            },
            [IdeaSource(url="https://coingecko.com/bitcoin", name="CoinGecko", date=datetime.now(timezone.utc))],
        ),
    )
    return history_rows


def test_generate_ideas_returns_security_and_crypto(mock_providers):
    generated = ideas.generate_ideas("balanced")
    assert any(item.ticker == "SBER" for item in generated)
    assert any(item.ticker == "BTC" for item in generated)
    security = next(item for item in generated if item.ticker == "SBER")
    assert "dma20" in security.metrics
    assert security.horizon_days > 0


def test_rank_and_filter_scores_and_limits(monkeypatch):
    monkeypatch.setattr(ideas.settings, "IDEAS_TOPN", 2, raising=False)
    monkeypatch.setattr(ideas.settings, "IDEAS_MIN_SOURCES", 2, raising=False)
    monkeypatch.setattr(ideas.settings, "IDEAS_SCORE_THRESHOLD", 0.5, raising=False)
    monkeypatch.setattr(ideas, "get_key_rate", lambda: 0.12)
    now = datetime.now(timezone.utc)
    base_sources = [
        IdeaSource(url="https://moex.com/sber", name="MOEX котировки", date=now),
        IdeaSource(url="https://moex.com/analytics", name="MOEX аналитика", date=now),
    ]
    idea_high = Idea(
        ticker="SBER",
        board="TQBR",
        asset_type="dividends",
        thesis="test",
        horizon_days=120,
        entry_range=(200.0, 210.0),
        stop_hint=180.0,
        metrics={
            "price": 205.0,
            "currency": "RUB",
            "dma20": 200.0,
            "dma50": 198.0,
            "dma200": 190.0,
            "rsi14": 55.0,
            "avg_value": 1e8,
            "pe": 10.0,
            "dividend_yield": 12.0,
        },
        risks=[],
        confidence="low",
        sources=list(base_sources),
    )
    idea_low = Idea(
        ticker="XYZ",
        board="TQBR",
        asset_type="growth",
        thesis="test",
        horizon_days=180,
        entry_range=(100.0, 110.0),
        stop_hint=90.0,
        metrics={
            "price": 50.0,
            "currency": "RUB",
            "dma20": 55.0,
            "dma50": 60.0,
            "dma200": 80.0,
            "rsi14": 80.0,
            "avg_value": 10000.0,
            "pe": 50.0,
            "dividend_yield": 0.0,
        },
        risks=[],
        confidence="low",
        sources=[base_sources[0]],
    )
    ranked = ideas.rank_and_filter([idea_low, idea_high])
    assert ranked[0].ticker == "SBER"
    assert ranked[0].confidence in {"mid", "high"}
    assert ranked[-1].ticker in {"SBER", "XYZ"}
    low_risk = next(item for item in ranked if item.ticker == "XYZ")
    assert "данных недостаточно" in " ".join(low_risk.risks)


def test_generate_ideas_survives_key_rate_failure(monkeypatch):
    history_rows = [
        {
            "TRADEDATE": (datetime(2024, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d"),
            "CLOSE": 200 + i * 0.5,
            "VOLUME": 100000 + i,
            "VALUE": 5000000 + i * 1000,
        }
        for i in range(120)
    ]

    monkeypatch.setattr(ideas, "portfolio_assets", lambda risk: [DummyAsset("SBER", "TQBR", "dividends")])
    monkeypatch.setattr(ideas, "_extra_candidates", lambda: [])
    monkeypatch.setattr(ideas, "get_security_quote", lambda ticker, board: DummyQuote())

    def boom_snapshot(ticker: str):
        raise ideas.RequestException("no snapshot")

    monkeypatch.setattr(ideas, "get_security_snapshot", boom_snapshot)
    monkeypatch.setattr(ideas, "get_security_history", lambda ticker, board, days=260: history_rows)

    def boom_key_rate():
        raise ideas.RequestException("no rate")

    monkeypatch.setattr(ideas, "get_key_rate", boom_key_rate)
    monkeypatch.setattr(ideas, "get_market_commentary", lambda: None)
    monkeypatch.setattr(ideas, "get_edgar_sources", lambda ticker: [])
    monkeypatch.setattr(ideas, "get_latest_value", lambda series, label: (None, []))
    monkeypatch.setattr(ideas, "get_coin_market", lambda coin_id, vs: ({}, []))

    generated = ideas.generate_ideas("balanced")
    assert generated  # no exception and at least one idea
    assert generated[0].ticker == "SBER"
