from __future__ import annotations

from datetime import datetime
from textwrap import shorten
from typing import Iterable, Optional


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

    currency = idea.metrics.get("currency") or "RUB"
    price = idea.metrics.get("price")
    if isinstance(price, (int, float)):
        price_text = f"{fmt_amount(float(price), 2)} {currency}"
    else:
        price_text = "нет данных"

    entry_low, entry_high = idea.entry_range
    entry_line = (
        f"Диапазон покупки: {fmt_amount(entry_low, 2)}–{fmt_amount(entry_high, 2)} {currency}"
    )

    probability = _format_probability(idea.metrics.get("score"))
    confidence = _confidence_ru(idea.confidence)
    summary = shorten(idea.thesis, width=160, placeholder="…") if idea.thesis else ""
    risk = idea.risks[0] if idea.risks else ""
    source = _render_primary_source(idea.sources)

    lines = [
        f"{idea.ticker} ({idea.board}) — {price_text}",
        f"Горизонт: {idea.horizon_days} дн.",
        entry_line,
        f"Рост: {probability}; уверенность: {confidence}",
    ]

    if summary:
        lines.append(f"Комментарий: {summary}")
    if risk:
        lines.append(f"Риск: {risk}")
    if source:
        lines.append(f"Источник: {source}")

    return "\n".join(lines)


def format_idea_digest(idea: "Idea") -> str:
    sources = _render_sources(idea.sources, limit=2)
    return (
        f"{idea.ticker} ({idea.board}) — {idea.thesis}. "
        f"Горизонт {idea.horizon_days} дн. Источники: {sources}"
    )


def format_idea_plan_details(idea: "Idea") -> str:
    from .ideas import Idea

    currency = idea.metrics.get("currency") or "RUB"
    price = idea.metrics.get("price")

    entry_low, entry_high = idea.entry_range
    entry_line = (
        f"  Диапазон покупки: {fmt_amount(entry_low, 2)}–{fmt_amount(entry_high, 2)} {currency}"
    )

    probability = _format_probability(idea.metrics.get("score"))
    confidence = _confidence_ru(idea.confidence)

    lines = [
        entry_line,
        f"  Рост: {probability}",
        f"  Уверенность: {confidence}",
    ]

    if idea.risks:
        lines.append(f"  Риск: {idea.risks[0]}")

    source = _render_primary_source(idea.sources)
    if source:
        lines.append(f"  Источник: {source}")

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


def _format_probability(score: object) -> str:
    if isinstance(score, (int, float)):
        value = max(0.0, min(1.0, float(score)))
        return f"{value * 100:.0f}% (скор {value:.2f})"
    return "нет данных"


def _render_primary_source(sources: Iterable["IdeaSource"]) -> str:
    from .sources import IdeaSource

    first = next(iter(sources), None)
    if not isinstance(first, IdeaSource):
        return ""
    date = (
        first.date.strftime("%Y-%m-%d")
        if isinstance(first.date, datetime)
        else str(first.date)
    )
    return f"{first.name} ({date}) — {first.url}"


_QUOTE_REASON_MESSAGES = {
    "no_active_trading_on_moex": "торги на MOEX не активны, показана альтернативная котировка",
    "delisted_from_moex": "инструмент делистингован на MOEX",
    "moex_delisting_announced": "MOEX объявил делистинг, используем внешний источник",
    "eod_close_fallback": "нет сделок сейчас, показано закрытие дня",
    "stale_price": "использована последняя доступная цена с MOEX",
    "no_trades_no_history": "нет свежих сделок и истории по инструменту",
    "missing_api_key": "нет API ключа для агрегатора",
    "upstream_unavailable": "внешний источник временно недоступен",
    "unknown_ticker": "тикер не найден у поставщика",
    "moex_unavailable": "MOEX временно недоступен, показана альтернативная котировка",
    "not_available_in_tbank": "инструмент недоступен в Т-Банке",
}


def describe_quote_reason(reason: Optional[str], context: Optional[str] = None) -> Optional[str]:
    """Return a human-friendly explanation for quote availability."""

    messages: list[str] = []
    for code in (reason, context):
        if not code:
            continue
        text = _QUOTE_REASON_MESSAGES.get(code)
        if text and text not in messages:
            messages.append(text)
    if not messages:
        return None
    return "; ".join(messages)
