from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from . import _requests as requests
from ._loguru import logger
from ._tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from .config import settings
from .sources import IdeaSource

_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
_CACHE_TTL = timedelta(hours=6)
_CACHE: dict[str, tuple[datetime, Optional[float], list[IdeaSource]]] = {}


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, max=2),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def _fred_get(series_id: str) -> requests.Response:
    params = {
        "series_id": series_id,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 1,
    }
    if settings.FRED_API_KEY:
        params["api_key"] = settings.FRED_API_KEY
    headers = {"User-Agent": settings.SEC_USER_AGENT}
    return requests.get(_BASE_URL, params=params, headers=headers, timeout=5)


def get_latest_value(series_id: str, label: str) -> tuple[Optional[float], list[IdeaSource]]:
    """Return latest observation value and metadata for the given FRED series."""

    now = datetime.utcnow()
    cached = _CACHE.get(series_id)
    if cached and now - cached[0] < _CACHE_TTL:
        return cached[1], cached[2]

    if not settings.FRED_API_KEY:
        logger.warning("FRED API key missing; returning empty data for %s", series_id)
        sources: list[IdeaSource] = []
        _CACHE[series_id] = (now, None, sources)
        return None, sources

    try:
        response = _fred_get(series_id)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Failed to fetch FRED series %s: %s", series_id, exc)
        sources = []
        _CACHE[series_id] = (now, None, sources)
        return None, sources

    data = response.json()
    observations = data.get("observations", [])
    value: Optional[float] = None
    sources: list[IdeaSource] = []
    if observations:
        obs = observations[0]
        value = _to_float(obs.get("value"))
        date_str = obs.get("date")
        try:
            obs_date = datetime.fromisoformat(date_str)
        except Exception:
            obs_date = now
        sources.append(
            IdeaSource(
                url=f"https://fred.stlouisfed.org/series/{series_id}",
                name=label,
                date=obs_date,
            )
        )
    _CACHE[series_id] = (now, value, sources)
    return value, sources


def get_sources(series_id: str, label: str) -> list[IdeaSource]:
    """Helper to expose the cached sources for tests."""

    _, sources = get_latest_value(series_id, label)
    return sources


def _to_float(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None
