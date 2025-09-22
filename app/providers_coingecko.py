from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Optional

from . import _requests as requests
from ._loguru import logger
from ._tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from .sources import IdeaSource

_BASE_URL = "https://api.coingecko.com/api/v3/coins/markets"
_CACHE_TTL = timedelta(minutes=10)
_CACHE: dict[tuple[str, str], tuple[datetime, dict[str, Any], list[IdeaSource]]] = {}


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, max=2),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def _cg_get(coin_id: str, vs_currency: str) -> requests.Response:
    params = {
        "vs_currency": vs_currency,
        "ids": coin_id,
        "price_change_percentage": "24h,7d,30d",
    }
    headers = {"User-Agent": "tg-fin-assistant/1.0"}
    return requests.get(_BASE_URL, params=params, headers=headers, timeout=5)


def get_coin_market(coin_id: str, vs_currency: str = "usd") -> tuple[dict[str, Any], list[IdeaSource]]:
    key = (coin_id, vs_currency)
    now = datetime.utcnow()
    cached = _CACHE.get(key)
    if cached and now - cached[0] < _CACHE_TTL:
        return cached[1], cached[2]

    try:
        response = _cg_get(coin_id, vs_currency)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("CoinGecko request failed for %s: %s", coin_id, exc)
        data: dict[str, Any] = {}
        sources: list[IdeaSource] = []
        _CACHE[key] = (now, data, sources)
        return data, sources

    payload = response.json()
    market = payload[0] if payload else {}
    sources: list[IdeaSource] = []
    if market:
        updated_str: Optional[str] = market.get("last_updated")
        try:
            updated = datetime.fromisoformat(updated_str.replace("Z", "+00:00")) if updated_str else now
        except Exception:
            updated = now
        sources.append(
            IdeaSource(
                url=f"https://www.coingecko.com/en/coins/{coin_id}",
                name=f"CoinGecko {coin_id.title()}",
                date=updated,
            )
        )
    _CACHE[key] = (now, market, sources)
    return market, sources


def get_sources(coin_id: str, vs_currency: str = "usd") -> list[IdeaSource]:
    _, sources = get_coin_market(coin_id, vs_currency)
    return sources
