from pathlib import Path

import pytest

from app.brokers import tinkoff_filter
from app.config import settings


@pytest.fixture(autouse=True)
def reset_cache():
    tinkoff_filter._reset_cache_for_tests()
    yield
    tinkoff_filter._reset_cache_for_tests()


def test_load_universe_parses_sections(tmp_path: Path):
    path = tmp_path / "universe.yml"
    path.write_text(
        """
        stocks:
          - sber
          - gazp;TQBR
        bonds: [SU26238RMFS9]
        etfs:
          - FXIT
        """,
        encoding="utf-8",
    )

    universe = tinkoff_filter.load_universe(path)

    assert universe["STOCKS"] == {"SBER", "GAZP"}
    assert universe["BONDS"] == {"SU26238RMFS9"}
    assert universe["ETFS"] == {"FXIT"}


def test_is_tradable_respects_yaml(monkeypatch, tmp_path: Path):
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

    assert tinkoff_filter.is_tradable("SBER", "stock") is True
    assert tinkoff_filter.is_tradable("FXIT", "etf") is True
    assert tinkoff_filter.is_tradable("ROSN", "stock") is False
    assert tinkoff_filter.is_tradable("FXRB", "etf") is False
    assert tinkoff_filter.is_tradable("SU26238RMFS9", "bond") is True


def test_missing_file_allows_all(monkeypatch, tmp_path: Path):
    path = tmp_path / "missing.yml"
    monkeypatch.setattr(settings, "TINKOFF_UNIVERSE_PATH", str(path))
    monkeypatch.setattr(settings, "TINKOFF_FILTER_ENABLED", True)

    assert tinkoff_filter.is_tradable("SBER", "stock") is True
    assert tinkoff_filter.is_tradable("UNKNOWN", "stock") is True
