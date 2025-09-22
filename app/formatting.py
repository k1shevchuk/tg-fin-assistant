from __future__ import annotations

from __future__ import annotations

from datetime import datetime
from textwrap import shorten
from typing import Iterable


def fmt_amount(value: float, precision: int = 0) -> str:
    """Format monetary amounts using a space as thousands separator."""
    if precision > 0:
        formatted = f"{value:,.{precision}f}"
    else:
        formatted = f"{value:,.0f}"
    return formatted.replace(",", " ")


def fmt_signed(value: float, precision: int = 0) -> str:
    if value == 0:
        return "0"
    sign = "+" if value > 0 else "-"
    return f"{sign}{fmt_amount(abs(value), precision=precision)}"


def format_idea(idea: "Idea") -> str:
    from .ideas import Idea

    header = f"[{idea.asset_type.upper()}] {idea.ticker} @ {idea.board}"
    thesis = f"Тезис: {idea.thesis}"
    catalyst = f"Катализаторы/горизонт: {idea.horizon_days} дней"

    entry_low, entry_high = idea.entry_range
    currency = idea.metrics.get("currency") or "RUB"
    entry = (
        f"Вход/риск: {fmt_amount(entry_low, 2)}–{fmt_amount(entry_high, 2)} {currency}; "
        f"стоп: {fmt_amount(idea.stop_hint, 2)} {currency}"
    )

    metrics_line = _render_metrics(idea.metrics)
    risks_line = "Риски: " + (", ".join(idea.risks) if idea.risks else "-" )
    sources_line = "Источники: " + _render_sources(idea.sources)
    confidence = f"Уверенность: { _confidence_ru(idea.confidence) }"

    return "\n".join(
        [
            header,
            thesis,
            catalyst,
            entry,
            f"Метрики: {metrics_line}",
            risks_line,
            sources_line,
            confidence,
        ]
    )


def format_idea_digest(idea: "Idea") -> str:
    sources = _render_sources(idea.sources, limit=2)
    return (
        f"{idea.ticker} ({idea.board}) — {idea.thesis}. "
        f"Горизонт {idea.horizon_days} дн. Источники: {sources}"
    )


def format_idea_plan_details(idea: "Idea") -> str:
    from .ideas import Idea

    entry_low, entry_high = idea.entry_range
    currency = idea.metrics.get("currency") or "RUB"
    thesis = shorten(idea.thesis, width=180, placeholder="…") if idea.thesis else ""
    entry = (
        f"  Горизонт: {idea.horizon_days} дн.; вход {fmt_amount(entry_low, 2)}–{fmt_amount(entry_high, 2)} {currency}; "
        f"стоп {fmt_amount(idea.stop_hint, 2)} {currency}"
    )

    lines = []
    if thesis:
        lines.append(f"  Тезис: {thesis}")
    lines.append(entry)

    metrics_line = _render_metrics(idea.metrics)
    if metrics_line and metrics_line != "-":
        lines.append(f"  Метрики: {metrics_line}")

    risks = ", ".join(idea.risks) if idea.risks else "-"
    lines.append(f"  Риски: {risks}")

    sources = _render_sources(idea.sources, limit=2)
    lines.append(f"  Источники: {sources}")

    confidence = _confidence_ru(idea.confidence)
    score = idea.metrics.get("score")
    if isinstance(score, (int, float)):
        lines.append(f"  Уверенность: {confidence} (скор {float(score):.2f})")
    else:
        lines.append(f"  Уверенность: {confidence}")

    return "\n".join(lines)


def _render_metrics(metrics: dict[str, float | str | None]) -> str:
    items: list[str] = []
    price = metrics.get("price")
    currency = metrics.get("currency") or "RUB"
    if isinstance(price, (int, float)):
        items.append(f"Цена {fmt_amount(float(price), 2)} {currency}")
    for key, label in (
        ("dma20", "20DMA"),
        ("dma50", "50DMA"),
        ("dma200", "200DMA"),
    ):
        value = metrics.get(key)
        if isinstance(value, (int, float)):
            items.append(f"{label} {fmt_amount(float(value), 2)}")
    rsi = metrics.get("rsi14")
    if isinstance(rsi, (int, float)):
        items.append(f"RSI14 {float(rsi):.1f}")
    high52 = metrics.get("high52")
    low52 = metrics.get("low52")
    if isinstance(high52, (int, float)) and isinstance(low52, (int, float)):
        items.append(f"52W {fmt_amount(float(low52), 2)}–{fmt_amount(float(high52), 2)}")
    pe = metrics.get("pe")
    if isinstance(pe, (int, float)):
        items.append(f"P/E {float(pe):.1f}")
    div_yield = metrics.get("dividend_yield")
    if isinstance(div_yield, (int, float)):
        items.append(f"Див. доходность {float(div_yield):.1f}%")
    macro = metrics.get("macro_indicator")
    if isinstance(macro, (int, float)):
        items.append(f"Макро {float(macro):.2f}")
    score = metrics.get("score")
    if isinstance(score, (int, float)):
        items.append(f"Скор {float(score):.2f}")
    return "; ".join(items) if items else "-"


def _render_sources(sources: Iterable["IdeaSource"], limit: int | None = None) -> str:
    from .sources import IdeaSource

    items: list[str] = []
    for idx, src in enumerate(sources, start=1):
        if limit and idx > limit:
            break
        date = src.date.strftime("%Y-%m-%d") if isinstance(src.date, datetime) else str(src.date)
        items.append(f"[{idx}] {src.name} ({date}) — {src.url}")
    return "; ".join(items) if items else "нет данных"


def _confidence_ru(value: str) -> str:
    mapping = {"low": "низкая", "mid": "средняя", "high": "высокая"}
    return mapping.get(value, value)
