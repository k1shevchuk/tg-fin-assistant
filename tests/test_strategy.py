from datetime import datetime
from pathlib import Path

import pytest

from app import strategy
from app.brokers import tinkoff_filter
from app.config import settings
from app.providers import Quote


@pytest.fixture(autouse=True)
def reset_filter_cache():
    tinkoff_filter._reset_cache_for_tests()
    yield
    tinkoff_filter._reset_cache_for_tests()


def test_propose_allocation_marks_unavailable(monkeypatch, tmp_path: Path):
    path = tmp_path / "universe.yml"
    path.write_text(
        """
        stocks: [SBER]
        etfs: [FXIT]
        bonds: [SU26238RMFS9]
        """,
        encoding="utf-8",
    )

    monkeypatch.setattr(settings, "TINKOFF_UNIVERSE_PATH", str(path))
    monkeypatch.setattr(settings, "TINKOFF_FILTER_ENABLED", True)

    monkeypatch.setattr(strategy, "get_key_rate", lambda: 0.16)
    monkeypatch.setattr(strategy, "get_market_commentary", lambda: {})

    requested: list[str] = []

    def fake_quote(ticker: str) -> Quote:
        requested.append(ticker.upper())
        return Quote(
            ticker=ticker.upper(),
            price=245.5,
            currency="RUB",
            ts_utc=datetime.utcnow().isoformat() + "Z",
            source="MOEX",
            board="TQBR",
            lot=10,
        )

    monkeypatch.setattr(strategy, "get_quote", fake_quote)

    advice = strategy.propose_allocation(10_000, "balanced")

    rosneft = next(line for line in advice.plan if line.ticker == "ROSN")
    fxus = next(line for line in advice.plan if line.ticker == "FXUS")

    assert rosneft.note == "Недоступно в Т-Банке"
    assert rosneft.quote is not None
    assert rosneft.quote.reason == "not_available_in_tbank"
    assert rosneft.quote.price is None

    assert fxus.note == "Недоступно в Т-Банке"
    assert fxus.quote is not None
    assert fxus.quote.reason == "not_available_in_tbank"

    assert "ROSN" not in requested
    assert "FXUS" not in requested
    assert "SBER" in requested
