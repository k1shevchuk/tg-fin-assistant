from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal, Optional

from . import _requests as requests
from ._loguru import logger
from ._tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from .config import settings


class MarketDataError(RuntimeError):
    """Raised when market data for a security is unavailable."""


class AggregatorAuthError(MarketDataError):
    """Raised when an aggregator rejects a request due to missing credentials."""


@dataclass(slots=True)
class SourceRoute:
    name: Literal["MOEX", "BINANCE", "AGGREGATOR", "UNKNOWN"]
    symbol: str
    board: Optional[str] = None
    market: Optional[str] = None
    engine: Optional[str] = None
    reason: Optional[str] = None
    currency: Optional[str] = None


@dataclass(slots=True)
class Quote:
    ticker: str
    price: Optional[float]
    currency: str
    ts_utc: Optional[str]
    source: Literal["MOEX", "BINANCE", "TWELVEDATA", "FINNHUB", "CBR", "UNKNOWN"]
    board: Optional[str] = None
    market: Optional[str] = None
    reason: Optional[str] = None
    lot: Optional[int] = None
    change: Optional[float] = None
    volume: Optional[float] = None
    value: Optional[float] = None
    context: Optional[str] = None

    @property
    def as_of(self) -> Optional[datetime]:
        if not self.ts_utc:
            return None
        try:
            return datetime.fromisoformat(self.ts_utc.replace("Z", "+00:00"))
        except ValueError:
            return None

_MOEX_BASE = "https://iss.moex.com/iss"
_BINANCE_BASE = "https://api.binance.com/api/v3"
_TWELVEDATA_BASE = "https://api.twelvedata.com"
_FINNHUB_BASE = "https://finnhub.io/api/v1"
_CBR_URL = "https://www.cbr-xml-daily.ru/daily_json.js"

_CRYPTO_PAIR_RE = re.compile(r"^[A-Z]{3,10}(USDT|BTC|BUSD)$")
_ALWAYS_AGGREGATOR: dict[str, dict[str, str]] = {
    "FXIT": {"symbol": "FXIT.MOEX", "currency": "SUR", "reason": "delisted_from_moex"},
    "FXWO": {"symbol": "FXWO.MOEX", "currency": "SUR", "reason": "delisted_from_moex"},
    "FXGD": {"symbol": "FXGD.MOEX", "currency": "SUR", "reason": "delisted_from_moex"},
    "FXRB": {"symbol": "FXRB.MOEX", "currency": "SUR", "reason": "delisted_from_moex"},
    "YNDX": {"symbol": "YNDX.US", "currency": "USD", "reason": "moex_delisting_announced"},
}

_CACHE_TTL = timedelta(seconds=settings.CACHE_TTL_SEC)
_QUOTE_CACHE: dict[str, tuple[datetime, Quote]] = {}
_SECURITY_CACHE: dict[str, tuple[datetime, dict[str, list[dict[str, Any]]]]] = {}
_HISTORY_CACHE: dict[tuple[str, str, int], tuple[datetime, list[dict[str, Any]]]] = {}
_KEY_RATE_CACHE: tuple[datetime, float] | None = None
_KEY_RATE_TTL = timedelta(hours=1)
_INDEX_CACHE: dict[str, tuple[datetime, float]] = {}
_INDEX_CACHE_TTL = timedelta(minutes=10)


class _NotFoundError(Exception):
    """Raised when ISS returns 404 for a security."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat().replace("+00:00", "Z")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, max=2),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def _http_get(
    url: str,
    params: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
):
    final_headers = {"Accept": "application/json", "User-Agent": settings.SEC_USER_AGENT}
    if headers:
        final_headers.update(headers)
    start = time.perf_counter()
    response = requests.get(url, params=params, headers=final_headers, timeout=settings.HTTP_TIMEOUT_SEC)
    duration = (time.perf_counter() - start) * 1000
    status = getattr(response, "status_code", "n/a")
    logger.debug(
        "HTTP GET {url} status={status} duration_ms={duration:.1f}",
        url=url,
        status=status,
        duration=duration,
    )
    return response


def resolve_source(ticker: str) -> SourceRoute:
    """Determine which provider should serve the given ticker."""

    symbol = ticker.upper().strip()
    if not symbol:
        return SourceRoute(name="UNKNOWN", symbol="", reason="unknown_ticker", currency="SUR")

    preset = _ALWAYS_AGGREGATOR.get(symbol)
    if preset:
        return SourceRoute(
            name="AGGREGATOR",
            symbol=preset.get("symbol", f"{symbol}.MOEX"),
            reason=preset.get("reason"),
            currency=preset.get("currency"),
        )

    if _CRYPTO_PAIR_RE.match(symbol):
        return SourceRoute(
            name="BINANCE",
            symbol=symbol,
            currency="USDT" if symbol.endswith("USDT") else "USD",
        )

    try:
        tables = _get_security_tables(symbol)
    except _NotFoundError:
        fallback = _aggregator_route(symbol, "unknown_ticker")
        if fallback:
            return fallback
        return SourceRoute(name="UNKNOWN", symbol=symbol, reason="unknown_ticker", currency="SUR")
    except requests.RequestException as exc:
        logger.warning("Failed to resolve %s via MOEX: %s", symbol, exc)
        fallback = _aggregator_route(symbol, "moex_unavailable")
        if fallback:
            return fallback
        raise MarketDataError("не удалось определить источник котировки") from exc

    boards = tables.get("boards") or []
    board_row = _pick_traded_board(boards)
    if board_row:
        board_code = str(board_row.get("boardid") or board_row.get("board") or "").upper() or None
        market = str(board_row.get("market") or "shares").lower()
        engine = str(board_row.get("engine") or "stock").lower()
        return SourceRoute(
            name="MOEX",
            symbol=symbol,
            board=board_code,
            market=market,
            engine=engine,
            currency="SUR",
        )

    if boards:
        fallback = _aggregator_route(symbol, "no_active_trading_on_moex")
        if fallback:
            return fallback
        return SourceRoute(
            name="MOEX",
            symbol=symbol,
            reason="no_active_trading_on_moex",
            currency="SUR",
        )

    fallback = _aggregator_route(symbol, "unknown_ticker")
    if fallback:
        return fallback
    return SourceRoute(name="UNKNOWN", symbol=symbol, reason="unknown_ticker", currency="SUR")


def get_quote(ticker: str) -> Quote:
    """Return the latest quote for the given ticker from the appropriate source."""

    normalized = ticker.upper().strip()
    route = resolve_source(normalized)
    cache_key = f"{route.name}:{route.symbol}"
    cached = _QUOTE_CACHE.get(cache_key)
    now = _now()
    if cached and now - cached[0] < _CACHE_TTL:
        return cached[1]

    if route.name == "MOEX":
        quote = _get_moex_quote(normalized, route)
    elif route.name == "BINANCE":
        quote = _get_binance_quote(normalized, route)
    elif route.name == "AGGREGATOR":
        quote = _get_aggregator_quote(normalized, route)
    else:
        quote = Quote(
            ticker=normalized,
            price=None,
            currency=route.currency or "SUR",
            ts_utc=None,
            source="UNKNOWN",
            reason=route.reason or "unknown_ticker",
        )

    if quote.price is not None:
        _QUOTE_CACHE[cache_key] = (now, quote)
    return quote

def get_daily_close_moex(secid: str, board: str, market: str, day: date) -> Optional[float]:
    """Return end-of-day close for the specified security."""

    params = {
        "iss.meta": "off",
        "from": day.isoformat(),
        "till": day.isoformat(),
    }
    url = (
        f"{_MOEX_BASE}/history/engines/stock/markets/{market}/boards/{board}/"
        f"securities/{secid}.json"
    )
    try:
        response = _http_get(url, params=params)
    except requests.RequestException as exc:
        logger.warning("Daily close fetch failed for %s %s: %s", secid, board, exc)
        return None

    if getattr(response, "status_code", 200) == 404:
        return None

    response.raise_for_status()
    tables = _parse_iss_tables(response.json())
    rows = tables.get("history") or []
    if not rows:
        return None

    last = rows[-1]
    for field in ("CLOSE", "LEGALCLOSEPR", "LCLOSEPRICE"):
        value = _safe_float(last.get(field))
        if value is not None:
            return value
    return None


def get_key_rate() -> float:
    """Return the current key rate, preferring MOEX RUONIA with CBR fallback."""

    global _KEY_RATE_CACHE

    now = _now()
    if _KEY_RATE_CACHE and now - _KEY_RATE_CACHE[0] < _KEY_RATE_TTL:
        return _KEY_RATE_CACHE[1]

    rate = _fetch_ruonia_key_rate()
    if rate is None:
        rate = _fetch_cbr_key_rate()

    if rate is None:
        fallback = getattr(settings, "KEY_RATE_FALLBACK", None)
        if fallback is not None:
            logger.warning(
                "Key rate unavailable from remote sources; using fallback %.2f%%",
                fallback * 100,
            )
            rate = fallback
        elif _KEY_RATE_CACHE:
            return _KEY_RATE_CACHE[1]
        else:
            raise MarketDataError("не удалось получить ключевую ставку")

    _KEY_RATE_CACHE = (now, rate)
    return rate


def get_market_commentary() -> Optional[dict[str, str]]:
    url = f"{_MOEX_BASE}/statistics/engines/stock/markets/index/analytics.json"
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
    key = name.upper()
    now = _now()
    cached = _INDEX_CACHE.get(key)
    if cached and now - cached[0] < _INDEX_CACHE_TTL:
        return cached[1]

    url = f"{_MOEX_BASE}/statistics/engines/stock/markets/index/securities/{key}.json"
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
    tables = _get_security_tables(ticker.upper())
    data = tables.get("securities") or []
    return data[0] if data else {}


def get_security_history(ticker: str, board: str, days: int = 260) -> list[dict[str, Any]]:
    board = board.upper()
    key = (ticker.upper(), board, days)
    now = _now()
    cached = _HISTORY_CACHE.get(key)
    if cached and now - cached[0] < timedelta(minutes=10):
        return cached[1]

    routes = _market_candidates(board)
    cutoff = (_now() - timedelta(days=days * 2)).date()
    collected: list[dict[str, Any]] = []

    for engine, market in routes:
        start = 0
        while True:
            params = {
                "iss.meta": "off",
                "from": cutoff.isoformat(),
                "start": start,
            }
            url = (
                f"{_MOEX_BASE}/history/engines/{engine}/markets/{market}/"
                f"securities/{ticker}.json"
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

def _get_moex_quote(ticker: str, route: SourceRoute) -> Quote:
    if not route.board or not route.market:
        return Quote(
            ticker=ticker,
            price=None,
            currency=route.currency or "SUR",
            ts_utc=None,
            source="MOEX",
            board=route.board,
            market=route.market,
            reason=route.reason or "no_active_trading_on_moex",
        )

    tables = _get_security_tables(ticker)
    securities = tables.get("securities") or []
    sec_row = securities[0] if securities else {}

    params = {"iss.meta": "off", "iss.only": "marketdata"}
    engine = route.engine or "stock"
    url = (
        f"{_MOEX_BASE}/engines/{engine}/markets/{route.market}/"
        f"boards/{route.board}/securities/{ticker}.json"
    )
    response = _http_get(url, params=params)
    response.raise_for_status()
    md_tables = _parse_iss_tables(response.json())
    marketdata = md_tables.get("marketdata") or []
    md_row = _find_row_by_board(marketdata, route.board) or (marketdata[0] if marketdata else {})

    price = _extract_price(md_row)
    lot = _extract_lot(sec_row, md_row)
    timestamp = _extract_timestamp(md_row)
    change = _safe_float(md_row.get("LASTCHANGEPRCNT"))
    volume = _safe_float(md_row.get("VOLTODAY"))
    value = _safe_float(md_row.get("VALTODAY"))
    currency = _extract_currency(sec_row, md_row)
    reason = route.reason

    if price is None:
        close = get_daily_close_moex(ticker, route.board, route.market, _now().date())
        if close is None:
            return Quote(
                ticker=ticker,
                price=None,
                currency=currency,
                ts_utc=None,
                source="MOEX",
                board=route.board,
                market=route.market,
                reason=reason or "no_trades_no_history",
                lot=lot,
                change=change,
                volume=volume,
                value=value,
            )
        price = close
        reason = "eod_close_fallback"

    ts_iso = timestamp.isoformat().replace("+00:00", "Z") if timestamp else _now_iso()
    return Quote(
        ticker=ticker,
        price=price,
        currency=currency,
        ts_utc=ts_iso,
        source="MOEX",
        board=route.board,
        market=route.market,
        reason=reason,
        lot=lot,
        change=change,
        volume=volume,
        value=value,
    )


def _get_binance_quote(ticker: str, route: SourceRoute) -> Quote:
    params = {"symbol": route.symbol}
    response = _http_get(f"{_BINANCE_BASE}/ticker/price", params=params)
    response.raise_for_status()
    payload = response.json()
    price = _safe_float(payload.get("price"))
    if price is None:
        raise MarketDataError(f"цена не найдена для {route.symbol} на Binance")
    currency = route.currency or ("USDT" if route.symbol.endswith("USDT") else "USD")
    return Quote(
        ticker=ticker,
        price=price,
        currency=currency,
        ts_utc=_now_iso(),
        source="BINANCE",
        reason=route.reason,
    )


def _get_aggregator_quote(ticker: str, route: SourceRoute) -> Quote:
    try:
        quote = _fetch_twelvedata_quote(ticker, route)
    except AggregatorAuthError:
        return Quote(
            ticker=ticker,
            price=None,
            currency=route.currency or "USD",
            ts_utc=None,
            source="TWELVEDATA",
            reason="missing_api_key",
            context=route.reason,
        )
    except MarketDataError:
        quote = None
    if quote:
        if route.reason and not quote.reason:
            quote.reason = route.reason
        elif route.reason and quote.reason != route.reason:
            quote.context = route.reason
        return quote

    quote = _fetch_finnhub_quote(ticker, route)
    if quote:
        if route.reason and not quote.reason:
            quote.reason = route.reason
        elif route.reason and quote.reason != route.reason:
            quote.context = route.reason
        return quote

    if not settings.TWELVEDATA_API_KEY and not settings.FINNHUB_API_KEY:
        return Quote(
            ticker=ticker,
            price=None,
            currency=route.currency or "USD",
            ts_utc=None,
            source="TWELVEDATA",
            reason="missing_api_key",
            context=route.reason,
        )

    return Quote(
        ticker=ticker,
        price=None,
        currency=route.currency or "USD",
        ts_utc=None,
        source="TWELVEDATA",
        reason="upstream_unavailable",
        context=route.reason,
    )


def _fetch_twelvedata_quote(ticker: str, route: SourceRoute) -> Optional[Quote]:
    api_key = settings.TWELVEDATA_API_KEY
    if not api_key:
        raise AggregatorAuthError("missing_api_key")

    params = {"symbol": route.symbol, "apikey": api_key}
    response = _http_get(f"{_TWELVEDATA_BASE}/quote", params=params)
    status_code = getattr(response, "status_code", None)
    if status_code in (401, 403):
        logger.warning(
            "Twelve Data auth failure for %s: HTTP %s",
            route.symbol,
            status_code,
        )
        raise AggregatorAuthError("missing_api_key")

    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict) and payload.get("status") == "error":
        message = str(payload.get("message") or "")
        logger.warning("Twelve Data error for %s: %s", route.symbol, message)
        code_raw = payload.get("code")
        try:
            code = int(code_raw)
        except (TypeError, ValueError):
            code = None
        if code in (401, 403):
            raise AggregatorAuthError("missing_api_key")
        lowered = message.lower()
        if "api key" in lowered or "apikey" in lowered or "unauthorized" in lowered:
            raise AggregatorAuthError("missing_api_key")
        return None

    price = _safe_float(payload.get("price") or payload.get("close"))
    if price is None:
        return None

    ts_iso = _to_iso(payload.get("datetime") or payload.get("timestamp"))
    currency = (payload.get("currency") or route.currency or "USD").upper()
    return Quote(
        ticker=ticker,
        price=price,
        currency=currency,
        ts_utc=ts_iso,
        source="TWELVEDATA",
        reason=route.reason,
    )


def _fetch_finnhub_quote(ticker: str, route: SourceRoute) -> Optional[Quote]:
    api_key = settings.FINNHUB_API_KEY
    if not api_key:
        return None

    params = {"symbol": route.symbol, "token": api_key}
    response = _http_get(f"{_FINNHUB_BASE}/quote", params=params)
    response.raise_for_status()
    payload = response.json()
    price = _safe_float(payload.get("c"))
    if price is None:
        return None

    ts_iso = _to_iso(payload.get("t"))
    currency = route.currency or "USD"
    return Quote(
        ticker=ticker,
        price=price,
        currency=currency,
        ts_utc=ts_iso,
        source="FINNHUB",
        reason=route.reason,
    )

def _fetch_ruonia_key_rate() -> Optional[float]:
    url = f"{_MOEX_BASE}/statistics/engines/stock/markets/bonds/ruonia.json"
    try:
        tables = _fetch_moex_tables(url, {"iss.meta": "off", "limit": 1})
    except requests.RequestException as exc:
        logger.warning("Failed to fetch key rate from RUONIA: {exc}", exc=exc)
        return None

    rows = tables.get("ruonia") or tables.get("data") or []
    if not rows:
        logger.warning("RUONIA payload did not contain data rows")
        return None

    row = rows[0]
    value = None
    for field in ("RUONIA", "RUONIAINDEX", "VALUE"):
        value = _safe_float(row.get(field))
        if value is not None:
            break

    if value is None:
        logger.warning("RUONIA payload missing numeric value")
        return None

    return value / 100 if value > 1.5 else value


def _fetch_cbr_key_rate() -> Optional[float]:
    try:
        response = _http_get(_CBR_URL)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.warning("Failed to fetch key rate from CBR: {exc}", exc=exc)
        return None

    raw = payload.get("KeyRate") or payload.get("key_rate")
    if raw in (None, ""):
        return None
    value = _safe_float(raw)
    if value is None:
        return None
    return value / 100 if value > 1.5 else value


def _get_security_tables(ticker: str) -> dict[str, list[dict[str, Any]]]:
    key = ticker.upper()
    now = _now()
    cached = _SECURITY_CACHE.get(key)
    if cached and now - cached[0] < _CACHE_TTL:
        return cached[1]

    params = {"iss.meta": "off"}
    url = f"{_MOEX_BASE}/securities/{key}.json"
    response = _http_get(url, params=params)
    if getattr(response, "status_code", 200) == 404:
        raise _NotFoundError()
    response.raise_for_status()
    tables = _parse_iss_tables(response.json())
    _SECURITY_CACHE[key] = (now, tables)
    return tables


def _fetch_moex_tables(
    url: str, params: Optional[dict[str, Any]] = None
) -> dict[str, list[dict[str, Any]]]:
    response = _http_get(url, params=params)
    response.raise_for_status()
    return _parse_iss_tables(response.json())


def _parse_iss_tables(payload: Any) -> dict[str, list[dict[str, Any]]]:
    tables: dict[str, list[dict[str, Any]]] = {}
    if isinstance(payload, dict):
        for name, value in payload.items():
            tables[name] = _normalize_table(value)
    elif isinstance(payload, list):
        for block in payload:
            if isinstance(block, dict):
                for name, value in block.items():
                    tables[name] = _normalize_table(value)
    return tables


def _normalize_table(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        if value and isinstance(value[0], dict):
            return value
        return value
    if isinstance(value, dict):
        columns = value.get("columns")
        data = value.get("data")
        if columns and data:
            return [dict(zip(columns, row)) for row in data]
    return []


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


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _extract_currency(sec_row: dict[str, Any], md_row: dict[str, Any]) -> str:
    for source in (sec_row, md_row):
        for key in ("FACEUNIT", "CURRENCYID", "SETTLECURRENCY"):
            raw = source.get(key)
            if raw:
                code = str(raw).upper()
                if code in {"RUB", "SUR"}:
                    return "SUR"
                return code
    return "SUR"


def _extract_price(row: dict[str, Any]) -> Optional[float]:
    for field in ("LAST", "LCURRENTPRICE", "MARKETPRICE3", "MARKETPRICE", "LASTTOPREVPRICE", "CLOSE"):
        value = _safe_float(row.get(field))
        if value is not None and value > 0:
            return value
    return None


def _extract_lot(sec_row: dict[str, Any], md_row: dict[str, Any]) -> Optional[int]:
    for source in (md_row, sec_row):
        raw = source.get("LOTSIZE")
        if raw is None:
            continue
        try:
            lot = int(float(raw))
        except (TypeError, ValueError):
            continue
        if lot > 0:
            return lot
    return None


def _extract_timestamp(row: dict[str, Any]) -> Optional[datetime]:
    for field in ("SYSTIME", "TIME", "UPDATETIME", "DATETIME"):
        raw = row.get(field)
        if not raw:
            continue
        if isinstance(raw, (int, float)):
            try:
                return datetime.fromtimestamp(float(raw), tz=timezone.utc)
            except ValueError:
                continue
        if isinstance(raw, str):
            cleaned = raw.strip()
            if not cleaned:
                continue
            iso_candidate = cleaned.replace(" ", "T").replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(iso_candidate)
            except ValueError:
                parsed = None
            if parsed is not None:
                return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    parsed = datetime.strptime(cleaned, fmt)
                except ValueError:
                    continue
                return parsed.replace(tzinfo=timezone.utc)
    return None


def _find_row_by_board(rows: list[dict[str, Any]], board: str) -> Optional[dict[str, Any]]:
    target = board.upper()
    for row in rows:
        for key in ("BOARDID", "BOARD"):
            value = row.get(key)
            if value and str(value).upper() == target:
                return row
    return None


def _pick_traded_board(rows: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    for row in rows:
        value = row.get("is_traded")
        if value is None:
            value = row.get("IS_TRADED")
        if _is_true(value):
            return row
    return None


def _is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _aggregator_route(ticker: str, fallback_reason: str) -> SourceRoute | None:
    preset = _ALWAYS_AGGREGATOR.get(ticker)
    if preset:
        return SourceRoute(
            name="AGGREGATOR",
            symbol=preset.get("symbol", f"{ticker}.MOEX"),
            reason=preset.get("reason") or fallback_reason,
            currency=preset.get("currency"),
        )
    return SourceRoute(
        name="AGGREGATOR",
        symbol=f"{ticker}.MOEX",
        reason=fallback_reason,
        currency="SUR",
    )


def _market_candidates(board: str) -> list[tuple[str, str]]:
    mapping: dict[str, list[tuple[str, str]]] = {
        "TQBR": [("stock", "shares")],
        "TQTD": [("stock", "shares")],
        "SMAL": [("stock", "shares")],
        "FQBR": [("stock", "shares")],
        "TQTF": [("stock", "etf"), ("stock", "shares")],
        "TQOB": [("stock", "bonds")],
        "TQCB": [("stock", "bonds")],
        "TQOD": [("stock", "bonds")],
        "SNDX": [("stock", "index")],
        "TOM": [("currency", "selt"), ("currency", "spot")],
    }
    candidates = mapping.get(board.upper())
    if candidates:
        return candidates
    return [
        ("stock", "shares"),
        ("stock", "etf"),
        ("stock", "bonds"),
        ("stock", "index"),
        ("currency", "selt"),
    ]


def _to_iso(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            return None
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        candidates = [cleaned, cleaned.replace(" ", "T")]
        for candidate in candidates:
            try:
                parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
            except ValueError:
                parsed = None
            if parsed is not None:
                return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                parsed = datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
            return parsed.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return None
