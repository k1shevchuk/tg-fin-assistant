from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Optional

from ._loguru import logger
from ._requests import RequestException
from .config import settings
from .providers import (
    MarketDataError,
    get_key_rate,
    get_market_commentary,
    get_quote,
    get_security_history,
    get_security_snapshot,
)
from .providers_coingecko import get_coin_market
from .providers_edgar import get_sources as get_edgar_sources
from .providers_fred import get_latest_value
from .sources import IdeaSource, filter_fresh_sources
from .strategy import portfolio_assets

_KEY_RATE_WARNING_EMITTED = False


@dataclass(slots=True)
class Idea:
    ticker: str
    board: str
    asset_type: str
    thesis: str
    horizon_days: int
    entry_range: tuple[float, float]
    stop_hint: float
    metrics: dict[str, float | str | None]
    risks: list[str]
    confidence: Literal["low", "mid", "high"]
    sources: list[IdeaSource]
    score: float = 0.0


@dataclass(slots=True)
class HistoryPoint:
    date: datetime
    close: Optional[float]
    volume: Optional[float]
    value: Optional[float]


def _safe_key_rate() -> Optional[float]:
    """Return key rate if available, swallowing transient HTTP errors."""

    global _KEY_RATE_WARNING_EMITTED

    try:
        value = get_key_rate()
    except Exception as exc:  # pragma: no cover - network issues in production
        if not _KEY_RATE_WARNING_EMITTED:
            logger.warning("Key rate unavailable: %s", exc)
            _KEY_RATE_WARNING_EMITTED = True
        return None

    _KEY_RATE_WARNING_EMITTED = False
    return value


def generate_ideas(risk: str) -> list[Idea]:
    assets = portfolio_assets(risk)
    seen: set[tuple[str, str]] = set()
    ideas: list[Idea] = []

    for asset in assets:
        if not asset.ticker:
            continue
        key = (asset.ticker, asset.board or "TQBR")
        if key in seen:
            continue
        seen.add(key)
        idea = _build_security_idea(asset.ticker, asset.board or "TQBR", asset.tag or asset.type)
        if idea:
            ideas.append(idea)

    # add optional ETF/FX picks regardless of risk profile
    for ticker, board, tag in _extra_candidates():
        key = (ticker, board)
        if key in seen:
            continue
        seen.add(key)
        idea = _build_security_idea(ticker, board, tag)
        if idea:
            ideas.append(idea)

    # add crypto idea
    btc_idea = _build_crypto_idea("bitcoin", "BTC")
    if btc_idea:
        ideas.append(btc_idea)

    return ideas


def rank_and_filter(ideas: list[Idea]) -> list[Idea]:
    scored: list[Idea] = []
    for idea in ideas:
        fresh_sources = filter_fresh_sources(idea.sources, settings.IDEAS_MAX_AGE_DAYS)
        idea.sources = fresh_sources
        fund = _score_fundamentals(idea)
        tech = _score_tech(idea)
        news = _score_news(idea)
        liquidity = _score_liquidity(idea)
        score = 0.35 * fund + 0.25 * tech + 0.25 * news + 0.15 * liquidity
        idea.score = round(score, 4)
        idea.metrics["score"] = idea.score
        source_count = len(fresh_sources)
        if source_count < settings.IDEAS_MIN_SOURCES:
            idea.confidence = "low"
            if "данных недостаточно" not in idea.risks:
                idea.risks.append("данных недостаточно: мало свежих источников")
        else:
            if idea.score >= 0.75:
                idea.confidence = "high"
            elif idea.score >= 0.55:
                idea.confidence = "mid"
            else:
                idea.confidence = "low"
        scored.append(idea)

    scored.sort(key=lambda item: item.score, reverse=True)
    threshold = settings.IDEAS_SCORE_THRESHOLD
    filtered = [idea for idea in scored if idea.score >= threshold]
    if len(filtered) < 3:
        filtered = scored[:3]
    topn = min(settings.IDEAS_TOPN, len(filtered))
    return filtered[:topn]


def _build_security_idea(ticker: str, board: str, tag: str) -> Optional[Idea]:
    try:
        quote = get_quote(ticker)
    except MarketDataError as exc:
        logger.warning("Quote unavailable for %s %s: %s", ticker, board, exc)
        return None
    except Exception as exc:  # pragma: no cover
        logger.error("Unexpected error loading quote for %s %s: %s", ticker, board, exc)
        return None

    if quote.price is None:
        logger.warning(
            "Skipping idea for %s %s: missing price (reason=%s)",
            ticker,
            board,
            quote.reason,
        )
        return None

    board_for_history = quote.board or board

    try:
        snapshot = get_security_snapshot(ticker)
    except (RequestException, MarketDataError) as exc:
        logger.warning("Snapshot unavailable for %s: %s", ticker, exc)
        snapshot = {}
    except Exception as exc:  # pragma: no cover
        logger.error("Unexpected snapshot error for %s: %s", ticker, exc)
        snapshot = {}
    history_rows = get_security_history(ticker, board_for_history, days=260)
    if not history_rows:
        logger.warning("History empty for %s", ticker)
        return None

    history = _normalize_history(history_rows)
    if not history:
        logger.warning("History normalize failed for %s", ticker)
        return None

    metrics = _compute_metrics(history, quote, snapshot)
    key_rate_value = _safe_key_rate()
    key_rate_percent = key_rate_value * 100 if key_rate_value is not None else None
    if key_rate_percent is not None:
        metrics["key_rate"] = key_rate_percent
    thesis = _compose_thesis(ticker, metrics, key_rate_percent)
    horizon = _horizon_for(tag)
    entry_low = round(quote.price * 0.97, 2)
    entry_high = round(quote.price * 1.03, 2)
    stop = round(quote.price * 0.9, 2)

    sources, macro_value = _collect_sources_for_security(
        ticker, board_for_history, quote, snapshot, tag
    )
    if macro_value is not None:
        metrics["macro_indicator"] = macro_value
    risks = _detect_risks(metrics, tag)

    return Idea(
        ticker=ticker.upper(),
        board=board_for_history.upper(),
        asset_type=tag,
        thesis=thesis,
        horizon_days=horizon,
        entry_range=(entry_low, entry_high),
        stop_hint=stop,
        metrics=metrics,
        risks=risks,
        confidence="low",
        sources=sources,
    )


def _build_crypto_idea(coin_id: str, symbol: str) -> Optional[Idea]:
    market, sources = get_coin_market(coin_id, "usd")
    if not market:
        return None

    price = float(market.get("current_price", 0.0))
    change_7d = market.get("price_change_percentage_7d_in_currency")
    volume = market.get("total_volume")
    market_cap = market.get("market_cap")
    horizon = 60
    entry_low = round(price * 0.95, 2)
    entry_high = round(price * 1.05, 2)
    stop = round(price * 0.85, 2)

    metrics: dict[str, float | str | None] = {
        "price": price,
        "currency": "USD",
        "market_cap": market_cap,
        "volume": volume,
        "change_24h": market.get("price_change_percentage_24h_in_currency"),
        "change_7d": change_7d,
        "change_30d": market.get("price_change_percentage_30d_in_currency"),
    }

    thesis = (
        f"BTC/USD {price:.0f} USD, 7д изм. {change_7d:.1f}%"
        if change_7d is not None
        else f"BTC/USD {price:.0f} USD"
    )
    risks = ["высокая волатильность криптовалют"]

    return Idea(
        ticker=symbol.upper(),
        board="CRYPTO",
        asset_type="crypto",
        thesis=thesis,
        horizon_days=horizon,
        entry_range=(entry_low, entry_high),
        stop_hint=stop,
        metrics=metrics,
        risks=risks,
        confidence="low",
        sources=sources,
    )


def _extra_candidates() -> list[tuple[str, str, str]]:
    return [
        ("FXRL", "TQTF", "etf"),
        ("FXDE", "TQTF", "etf"),
        ("RGBI", "SNDX", "bonds"),
    ]


def _normalize_history(rows: list[dict[str, object]]) -> list[HistoryPoint]:
    points: list[HistoryPoint] = []
    for row in rows:
        date_value = row.get("TRADEDATE") or row.get("DATE")
        if not date_value:
            continue
        try:
            trade_dt = datetime.fromisoformat(str(date_value))
        except ValueError:
            try:
                trade_dt = datetime.strptime(str(date_value), "%Y-%m-%d")
            except ValueError:
                continue
        close = _coerce_float(
            row.get("CLOSE")
            or row.get("LEGALCLOSEPRICE")
            or row.get("MARKETPRICE3")
            or row.get("MARKETPRICE")
        )
        volume = _coerce_float(row.get("VOLUME"))
        value = _coerce_float(row.get("VALUE"))
        points.append(HistoryPoint(date=trade_dt, close=close, volume=volume, value=value))
    points.sort(key=lambda item: item.date)
    return points[-260:]


def _compute_metrics(history: list[HistoryPoint], quote, snapshot: dict) -> dict[str, float | str | None]:
    closes = [point.close for point in history if point.close is not None]
    metrics: dict[str, float | str | None] = {
        "price": quote.price,
        "currency": quote.currency,
    }
    if closes:
        metrics["dma20"] = _moving_average(closes, 20)
        metrics["dma50"] = _moving_average(closes, 50)
        metrics["dma200"] = _moving_average(closes, 200)
        metrics["rsi14"] = _compute_rsi(closes, 14)
        window = closes[-260:] if len(closes) >= 260 else closes
        metrics["high52"] = max(window)
        metrics["low52"] = min(window)
    volumes = [point.volume for point in history if point.volume is not None]
    if volumes:
        tail = volumes[-20:] if len(volumes) >= 20 else volumes
        metrics["avg_volume"] = sum(tail) / len(tail)
    values = [point.value for point in history if point.value is not None]
    if values:
        tail = values[-20:] if len(values) >= 20 else values
        metrics["avg_value"] = sum(tail) / len(tail)

    fundamentals = {
        "PE": "pe",
        "DIVYIELD": "dividend_yield",
        "ISSUECAPITALIZATION": "market_cap",
    }
    for field, key in fundamentals.items():
        raw = snapshot.get(field)
        if raw not in (None, ""):
            try:
                metrics[key] = float(str(raw).replace(",", "."))
            except ValueError:
                metrics[key] = None
        else:
            metrics[key] = None
    metrics["lot"] = quote.lot
    if quote.price is not None and quote.lot:
        metrics["lot_value"] = quote.price * quote.lot
    else:
        metrics["lot_value"] = None
    metrics["change_percent"] = quote.change
    return metrics


def _moving_average(values: list[float], window: int) -> Optional[float]:
    if len(values) < window:
        return None
    segment = values[-window:]
    return sum(segment) / window


def _compute_rsi(values: list[float], window: int) -> Optional[float]:
    if len(values) <= window:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for idx in range(1, len(values)):
        change = values[idx] - values[idx - 1]
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))
    if len(gains) < window or len(losses) < window:
        return None
    avg_gain = sum(gains[-window:]) / window
    avg_loss = sum(losses[-window:]) / window
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _compose_thesis(
    ticker: str,
    metrics: dict[str, float | str | None],
    key_rate_percent: Optional[float],
) -> str:
    price = float(metrics.get("price") or 0.0)
    dma50 = metrics.get("dma50")
    parts = [f"{ticker.upper()} {price:.2f} {metrics.get('currency', 'RUB')}"]
    if isinstance(dma50, (int, float)):
        parts.append(f">50DMA {float(dma50):.2f}")
    change = metrics.get("change_percent")
    if isinstance(change, (int, float)):
        parts.append(f"день {float(change):.2f}%")
    if isinstance(key_rate_percent, (int, float)):
        parts.append(f"ключевая {float(key_rate_percent):.2f}%")
    return ", ".join(parts)


def _horizon_for(tag: str) -> int:
    horizons = {
        "cash": 30,
        "bonds": 120,
        "core_equity": 180,
        "dividends": 180,
        "growth": 210,
        "gold": 150,
        "alternatives": 120,
        "etf": 150,
        "bonds_index": 120,
    }
    return horizons.get(tag, 120)


def _collect_sources_for_security(
    ticker: str,
    board: str,
    quote,
    snapshot: dict,
    tag: str,
) -> tuple[list[IdeaSource], Optional[float]]:
    sources: list[IdeaSource] = []
    sources.append(
        IdeaSource(
            url=f"https://www.moex.com/ru/issue/{ticker}.aspx?board={board}",
            name="MOEX ISS котировки",
            date=quote.as_of,
        )
    )
    updated_str = snapshot.get("UPDATEDATE") or snapshot.get("LISTLEVELCHANGEDATE")
    try:
        updated_date = datetime.fromisoformat(str(updated_str)) if updated_str else quote.as_of
    except Exception:
        updated_date = quote.as_of
    else:
        if isinstance(updated_date, datetime) and updated_date.tzinfo is None:
            updated_date = updated_date.replace(tzinfo=timezone.utc)
    sources.append(
        IdeaSource(
            url=f"https://iss.moex.com/iss/securities/{ticker}.json",
            name="MOEX профиль эмитента",
            date=updated_date,
        )
    )
    commentary = get_market_commentary()
    if commentary and commentary.get("url"):
        sources.append(
            IdeaSource(
                url=commentary["url"],
                name=f"{commentary.get('source', 'MOEX')} аналитика",
                date=datetime.now(timezone.utc),
            )
        )
    sec_sources = get_edgar_sources(ticker)
    sources.extend(sec_sources)

    macro_value: Optional[float] = None
    fred_series: Optional[tuple[str, str]] = None
    if tag in {"bonds", "bonds_index"}:
        fred_series = ("RUSCPIALLMINMEI", "OECD Russia CPI")
    elif tag in {"growth", "core_equity"}:
        fred_series = ("DGS10", "US 10Y Treasury")
    elif tag in {"dividends"}:
        fred_series = ("FEDFUNDS", "US Fed Funds Rate")

    if fred_series:
        macro_value, fred_sources = get_latest_value(*fred_series)
        sources.extend(fred_sources)

    return sources, macro_value


def _detect_risks(metrics: dict[str, float | str | None], tag: str) -> list[str]:
    risks: list[str] = []
    rsi = metrics.get("rsi14")
    if isinstance(rsi, float):
        if rsi > 70:
            risks.append("перекупленность по RSI")
        elif rsi < 30:
            risks.append("перепроданность по RSI")
    change = metrics.get("change_percent")
    if isinstance(change, float) and abs(change) > 3:
        risks.append("дневная волатильность выше 3%")
    if tag in {"alternatives", "crypto"}:
        risks.append("повышенный риск категории актива")
    return risks


def _score_fundamentals(idea: Idea) -> float:
    metrics = idea.metrics
    pe = metrics.get("pe")
    div_yield = metrics.get("dividend_yield")
    score = 0.4
    if isinstance(pe, float) and pe > 0:
        if 5 <= pe <= 15:
            score += 0.3
        elif pe < 5 or pe > 25:
            score -= 0.1
    key_rate_percent: Optional[float]
    stored_rate = metrics.get("key_rate")
    if isinstance(stored_rate, (int, float)):
        key_rate_percent = float(stored_rate)
    else:
        rate_value = _safe_key_rate()
        key_rate_percent = rate_value * 100 if rate_value is not None else None
        if key_rate_percent is not None:
            metrics["key_rate"] = key_rate_percent
    if isinstance(div_yield, float) and div_yield:
        if key_rate_percent is not None and div_yield >= key_rate_percent:
            score += 0.2
        elif div_yield >= 5:
            score += 0.1
    return max(min(score, 1.0), 0.0)


def _score_tech(idea: Idea) -> float:
    metrics = idea.metrics
    price = metrics.get("price")
    dma20 = metrics.get("dma20")
    dma50 = metrics.get("dma50")
    dma200 = metrics.get("dma200")
    rsi = metrics.get("rsi14")
    score = 0.5
    if all(isinstance(val, float) for val in (price, dma20, dma50) if val is not None):
        if price and dma20 and price >= dma20 and dma20 >= (dma50 or dma20):
            score += 0.2
        elif price and dma20 and price < dma20:
            score -= 0.1
    if isinstance(dma200, float) and isinstance(price, float):
        if price >= dma200:
            score += 0.1
    if isinstance(rsi, float):
        if 40 <= rsi <= 60:
            score += 0.1
        elif rsi > 70 or rsi < 30:
            score -= 0.1
    return max(min(score, 1.0), 0.0)


def _score_news(idea: Idea) -> float:
    sources = idea.sources
    count = sum(1 for src in sources if "аналит" in src.name.lower() or "sec" in src.name.lower())
    total = len(sources)
    if total == 0:
        return 0.0
    return max(min((count / total) + 0.3, 1.0), 0.0)


def _score_liquidity(idea: Idea) -> float:
    metrics = idea.metrics
    avg_value = metrics.get("avg_value")
    if isinstance(avg_value, float) and avg_value:
        if avg_value >= 1e8:
            return 1.0
        if avg_value >= 5e7:
            return 0.8
        if avg_value >= 1e7:
            return 0.6
        return 0.3
    avg_volume = metrics.get("avg_volume")
    if isinstance(avg_volume, float) and avg_volume:
        if avg_volume >= 1_000_000:
            return 0.8
        if avg_volume >= 200_000:
            return 0.6
        return 0.3
    return 0.1


def _coerce_float(value: object) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None
