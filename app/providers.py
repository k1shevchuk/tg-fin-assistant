from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

from . import _requests as requests
from ._loguru import logger
from ._tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


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
    value: Optional[float] = None
    volume: Optional[float] = None
    change: Optional[float] = None


_QUOTE_CACHE: dict[tuple[str, str], tuple[datetime, Quote]] = {}
_QUOTE_CACHE_TTL = timedelta(minutes=10)
_KEY_RATE_CACHE: tuple[datetime, float] | None = None
_KEY_RATE_TTL = timedelta(hours=1)
_INDEX_CACHE: dict[str, tuple[datetime, float]] = {}
_INDEX_CACHE_TTL = timedelta(minutes=10)
_SNAPSHOT_CACHE: dict[str, tuple[datetime, dict[str, Any]]] = {}
_SNAPSHOT_TTL = timedelta(minutes=10)
_HISTORY_CACHE: dict[tuple[str, str, int], tuple[datetime, list[dict[str, Any]]]] = {}
_HISTORY_TTL = timedelta(minutes=10)


def get_key_rate() -> float:
    """Return the current key rate using MOEX RUONIA statistics."""

    global _KEY_RATE_CACHE

    now = datetime.utcnow()
    if _KEY_RATE_CACHE and now - _KEY_RATE_CACHE[0] < _KEY_RATE_TTL:
        return _KEY_RATE_CACHE[1]

    url = "https://iss.moex.com/iss/statistics/engines/stock/markets/bonds/ruonia.json"
    try:
        tables = _fetch_moex_tables(url, {"iss.meta": "off", "limit": 1})
    except requests.RequestException as exc:
        logger.warning("Failed to fetch key rate from RUONIA: {exc}", exc=exc)
        if _KEY_RATE_CACHE:
            return _KEY_RATE_CACHE[1]
        raise

    rows = tables.get("ruonia") or tables.get("data") or []
    if not rows:
        raise MarketDataError("нет данных RUONIA для расчёта ключевой ставки")

    row = rows[0]
    value = None
    for field in ("RUONIA", "RUONIAINDEX", "VALUE"):
        raw = row.get(field)
        if isinstance(raw, (int, float)):
            value = float(raw)
            break
        if isinstance(raw, str) and raw:
            try:
                value = float(raw.replace(",", "."))
                break
            except ValueError:
                continue

    if value is None:
        raise MarketDataError("RUONIA вернулась без численного значения")

    rate = value / 100 if value > 1.5 else value
    _KEY_RATE_CACHE = (now, rate)
    return rate


def get_inflation_est() -> float:
    return 0.08


def get_security_quote(ticker: str, board: str = "TQBR") -> Quote:
    """Fetch the latest quote for a MOEX-listed security."""

    key = (ticker.upper(), board.upper())
    now = datetime.utcnow()
    cached = _QUOTE_CACHE.get(key)
    if cached and now - cached[0] < _QUOTE_CACHE_TTL:
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
        value=_safe_float(md_row.get("VALTODAY")),
        volume=_safe_float(md_row.get("VOLTODAY")),
        change=_safe_float(md_row.get("LASTCHANGEPRCNT")),
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


def get_index_value(name: str) -> float:
    """Return the latest value for a MOEX index."""

    key = name.upper()
    now = datetime.utcnow()
    cached = _INDEX_CACHE.get(key)
    if cached and now - cached[0] < _INDEX_CACHE_TTL:
        return cached[1]

    url = (
        "https://iss.moex.com/iss/statistics/engines/stock/markets/index/"
        f"securities/{key}.json"
    )
    try:
        tables = _fetch_moex_tables(url, {"iss.meta": "off"})
    except requests.RequestException as exc:
        logger.warning("Failed to load index value {key}: {exc}", key=key, exc=exc)
        if cached:
            return cached[1]
        raise

    securities = tables.get("securities") or tables.get("index") or []
    if not securities:
        raise MarketDataError(f"индекс {key} не найден")

    row = securities[0]
    value = None
    for field in ("CURRENTVALUE", "LASTVALUE", "VALUE"):
        value = _safe_float(row.get(field))
        if value is not None:
            break

    if value is None:
        raise MarketDataError(f"не удалось получить значение индекса {key}")

    _INDEX_CACHE[key] = (now, value)
    return value


def get_security_snapshot(ticker: str) -> dict[str, Any]:
    """Return static instrument metadata from ISS /securities endpoint."""

    now = datetime.utcnow()
    cached = _SNAPSHOT_CACHE.get(ticker.upper())
    if cached and now - cached[0] < _SNAPSHOT_TTL:
        return cached[1]

    url = f"https://iss.moex.com/iss/securities/{ticker}.json"
    tables = _fetch_moex_tables(url, {"iss.meta": "off"})
    data = tables.get("securities") or []
    result = data[0] if data else {}
    _SNAPSHOT_CACHE[ticker.upper()] = (now, result)
    return result


def get_security_history(ticker: str, board: str, days: int = 260) -> list[dict[str, Any]]:
    """Fetch historical trading data for the given security."""

    key = (ticker.upper(), board.upper(), days)
    now = datetime.utcnow()
    cached = _HISTORY_CACHE.get(key)
    if cached and now - cached[0] < _HISTORY_TTL:
        return cached[1]

    markets = _market_candidates(board)
    cutoff = (datetime.utcnow() - timedelta(days=days * 2)).date()
    collected: list[dict[str, Any]] = []

    for market in markets:
        start = 0
        while True:
            params = {
                "iss.meta": "off",
                "from": cutoff.isoformat(),
                "start": start,
            }
            url = (
                "https://iss.moex.com/iss/history/engines/stock/markets/"
                f"{market}/securities/{ticker}.json"
            )
            try:
                tables = _fetch_moex_tables(url, params)
            except requests.RequestException as exc:
                logger.warning(
                    "History fetch failed for {ticker} on {market}: {exc}",
                    ticker=ticker,
                    market=market,
                    exc=exc,
                )
                break

            rows = tables.get("history") or []
            if not rows:
                break

            collected.extend(rows)
            if len(rows) < 100 or len(collected) >= days:
                break
            start += len(rows)

        if collected:
            break

    _HISTORY_CACHE[key] = (now, collected)
    return collected


# --- Helpers -----------------------------------------------------------------

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, max=2),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def _http_get(url: str, params: Optional[dict[str, Any]] = None, headers: Optional[dict[str, str]] = None) -> requests.Response:
    return requests.get(url, params=params, headers=headers, timeout=5)


def _fetch_moex_tables(url: str, params: Optional[dict[str, Any]] = None) -> dict[str, list[dict[str, Any]]]:
    response = _http_get(url, params=params)
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


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if isinstance(value, str):
        try:
            return float(value.replace(",", "."))
        except ValueError:
            return None
    return None


def _market_candidates(board: str) -> list[str]:
    mapping = {
        "TQBR": ["shares"],
        "TQTF": ["shares", "etf"],
        "TQTD": ["shares"],
        "TQOB": ["bonds"],
        "TQCB": ["bonds"],
        "TQOD": ["bonds"],
        "SMAL": ["shares"],
        "FQBR": ["shares"],
        "TOM": ["currencies"],
        "SNDX": ["index"],
    }
    candidates = mapping.get(board.upper())
    if candidates:
        return candidates
    return ["shares", "bonds", "etf", "index", "foreignshares", "currencies"]
