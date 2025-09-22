from datetime import datetime

from app.formatting import format_idea_plan_details
from app.ideas import Idea
from app.sources import IdeaSource


def test_format_idea_plan_details_includes_key_blocks():
    idea = Idea(
        ticker="SBER",
        board="TQBR",
        asset_type="dividends",
        thesis="Стабильная дивидендная история на фоне сильной нефти",
        horizon_days=120,
        entry_range=(200.0, 210.0),
        stop_hint=180.0,
        metrics={
            "currency": "RUB",
            "price": 205.0,
            "lot": 10,
            "score": 0.72,
        },
        risks=["волатильность цен на нефть"],
        confidence="mid",
        sources=[
            IdeaSource(
                url="https://moex.com/sber",
                name="MOEX котировки",
                date=datetime(2024, 1, 10),
            ),
            IdeaSource(
                url="https://moex.com/analytics",
                name="MOEX аналитика",
                date=datetime(2024, 1, 11),
            ),
        ],
    )

    formatted = format_idea_plan_details(idea)

    assert "Горизонт: 120 дн." in formatted
    assert "Источники: [1] MOEX котировки (2024-01-10)" in formatted
    assert "Уверенность: средняя (скор 0.72)" in formatted
    assert "Риски: волатильность цен на нефть" in formatted
