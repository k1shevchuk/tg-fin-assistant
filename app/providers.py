from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

import requests


class MarketDataError(RuntimeError):
    """Raised when market data for a security is unavailable."""


@dataclass
class Quote:
    ticker: str
    board: str
    price: float
    currency: str
    lot: int
    as_of: datetime
    source: str = "MOEX ISS"


_QUOTE_CACHE: dict[tuple[str, str], tuple[datetime, Quote]] = {}
_CACHE_TTL = timedelta(minutes=5)


def get_key_rate() -> float:
    """Return the current key rate (stubbed with a fixed value)."""
    return 0.17


def get_inflation_est() -> float:
    return 0.08


def get_security_quote(ticker: str, board: str = "TQBR") -> Quote:
    """Fetch the latest quote for a MOEX-listed security."""

    key = (ticker.upper(), board.upper())
    now = datetime.utcnow()
    cached = _QUOTE_CACHE.get(key)
    if cached and now - cached[0] < _CACHE_TTL:
        return cached[1]

    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}.json"
    try:
        tables = _fetch_moex_tables(url, {"iss.meta": "off"})
    except requests.RequestException as exc:
        raise MarketDataError(f"не удалось получить котировку {ticker}") from exc

    securities = tables.get("securities") or []
    marketdata = tables.get("marketdata") or []

    sec_row = _find_row(securities, "BOARDID", board) or (securities[0] if securities else None)
    md_row = _find_row(marketdata, "BOARDID", board) or (marketdata[0] if marketdata else None)

    if sec_row is None or md_row is None:
        raise MarketDataError(f"нет данных для {ticker} ({board})")

    price = _extract_price(md_row)
    lot = _extract_lot(sec_row, md_row)
    as_of = _extract_timestamp(md_row)
    currency = (
        sec_row.get("FACEUNIT")
        or sec_row.get("CURRENCYID")
        or md_row.get("CURRENCYID")
        or "RUB"
    )

    quote = Quote(
        ticker=ticker.upper(),
        board=board.upper(),
        price=price,
        currency=currency,
        lot=lot,
        as_of=as_of,
    )
    _QUOTE_CACHE[key] = (now, quote)
    return quote


def get_market_commentary() -> Optional[dict[str, str]]:
    """Return the latest analytics headline from the MOEX ISS feed."""

    url = "https://iss.moex.com/iss/statistics/engines/stock/markets/index/analytics.json"
    try:
        tables = _fetch_moex_tables(url, {"iss.meta": "off"})
    except requests.RequestException:
        return None

    analytics = tables.get("analytics") or []
    if not analytics:
        return None

    row = analytics[0]
    normalized = {str(k).lower(): v for k, v in row.items()}

    title = _clean_str(normalized.get("title") or normalized.get("name"))
    if not title:
        return None

    summary = _clean_str(
        normalized.get("annotation")
        or normalized.get("brief")
        or normalized.get("text")
    )
    url_value = _clean_str(
        normalized.get("url")
        or normalized.get("href")
        or normalized.get("link")
    )
    source = _clean_str(normalized.get("source")) or "MOEX"

    result = {"title": title, "source": source}
    if summary:
        result["summary"] = summary
    if url_value:
        result["url"] = url_value
    return result


# --- Helpers -----------------------------------------------------------------

def _fetch_moex_tables(url: str, params: Optional[dict[str, Any]] = None) -> dict[str, list[dict[str, Any]]]:
    response = requests.get(url, params=params, timeout=5)
    response.raise_for_status()
    payload = response.json()
    tables: dict[str, list[dict[str, Any]]] = {}

    if isinstance(payload, list):
        for block in payload:
            if isinstance(block, dict):
                for name, value in block.items():
                    tables[name] = _normalize_table(value)
    elif isinstance(payload, dict):
        for name, value in payload.items():
            tables[name] = _normalize_table(value)
    return tables


def _normalize_table(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        if value and isinstance(value[0], dict):
            return value  # already normalized
        return value
    if isinstance(value, dict):
        columns = value.get("columns")
        data = value.get("data")
        if columns and data:
            return [dict(zip(columns, row)) for row in data]
    return []


def _find_row(rows: list[dict[str, Any]], key: str, value: str) -> Optional[dict[str, Any]]:
    target = value.upper()
    for row in rows:
        if str(row.get(key, "")).upper() == target:
            return row
    return None


def _extract_price(row: dict[str, Any]) -> float:
    for field in ("LAST", "LASTTOPREVPRICE", "LCLOSEPRICE", "MARKETPRICE3", "MARKETPRICE", "CLOSE"):
        raw = row.get(field)
        if isinstance(raw, (int, float)) and raw:
            return float(raw)
    raise MarketDataError("цена не найдена")


def _extract_lot(sec_row: dict[str, Any], md_row: dict[str, Any]) -> int:
    for source in (md_row, sec_row):
        raw = source.get("LOTSIZE")
        if raw is None:
            continue
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return 1


def _extract_timestamp(row: dict[str, Any]) -> datetime:
    for field in ("SYSTIME", "TIME", "UPDATETIME", "DATETIME"):
        raw = row.get(field)
        if not raw:
            continue
        if isinstance(raw, (int, float)):
            try:
                return datetime.fromtimestamp(float(raw))
            except ValueError:
                continue
        if isinstance(raw, str):
            cleaned = raw.strip().replace(" ", "T")
            for pattern in (cleaned, cleaned + "Z"):
                try:
                    return datetime.fromisoformat(pattern)
                except ValueError:
                    continue
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(raw.strip(), fmt)
                except ValueError:
                    continue
    return datetime.utcnow()


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
