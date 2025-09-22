from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from . import _requests as requests
from ._loguru import logger
from ._tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from .config import settings
from .sources import IdeaSource

_TICKER_CACHE_TTL = timedelta(days=1)
_SUBMISSION_TTL = timedelta(hours=6)
_TICKER_CACHE: tuple[datetime, dict[str, str]] | None = None
_SUBMISSION_CACHE: dict[str, tuple[datetime, dict]] = {}
_EARNINGS_FORMS = {"10-Q", "10-K"}


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, max=2),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def _get(url: str) -> requests.Response:
    headers = {"User-Agent": settings.SEC_USER_AGENT}
    return requests.get(url, headers=headers, timeout=5)


def _load_ticker_map() -> dict[str, str]:
    global _TICKER_CACHE
    now = datetime.utcnow()
    if _TICKER_CACHE and now - _TICKER_CACHE[0] < _TICKER_CACHE_TTL:
        return _TICKER_CACHE[1]

    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        response = _get(url)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        logger.warning("Failed to load SEC ticker map: %s", exc)
        return _TICKER_CACHE[1] if _TICKER_CACHE else {}

    mapping: dict[str, str] = {}
    if isinstance(payload, list):
        iterable = payload
    else:
        iterable = payload.values()
    for entry in iterable:
        ticker = str(entry.get("ticker") or "").upper()
        cik = str(entry.get("cik_str") or "").zfill(10)
        if ticker and cik:
            mapping[ticker] = cik
    _TICKER_CACHE = (now, mapping)
    return mapping


def _load_submissions(cik: str) -> Optional[dict]:
    now = datetime.utcnow()
    cached = _SUBMISSION_CACHE.get(cik)
    if cached and now - cached[0] < _SUBMISSION_TTL:
        return cached[1]

    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    try:
        response = _get(url)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Failed to load SEC submissions for %s: %s", cik, exc)
        return cached[1] if cached else None

    data = response.json()
    _SUBMISSION_CACHE[cik] = (now, data)
    return data


def get_next_report_source(ticker: str) -> Optional[IdeaSource]:
    mapping = _load_ticker_map()
    cik = mapping.get(ticker.upper())
    if not cik:
        return None

    data = _load_submissions(cik)
    if not data:
        return None

    filings = data.get("filings", {}).get("recent", {})
    forms = filings.get("form", [])
    report_dates = filings.get("reportDate", [])
    filing_dates = filings.get("filingDate", [])
    accession_numbers = filings.get("accessionNumber", [])
    documents = filings.get("primaryDocument", [])

    for idx, form in enumerate(forms):
        if form not in _EARNINGS_FORMS:
            continue
        date_str = report_dates[idx] if idx < len(report_dates) else filing_dates[idx]
        try:
            report_date = datetime.fromisoformat(date_str)
        except Exception:
            report_date = datetime.utcnow()
        accession = accession_numbers[idx].replace("-", "") if idx < len(accession_numbers) else ""
        document = documents[idx] if idx < len(documents) else ""
        url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{document}"
            if accession and document
            else f"https://www.sec.gov/cgi-bin/browse-edgar?CIK={cik}"
        )
        return IdeaSource(url=url, name=f"SEC {form}", date=report_date)
    return None


def get_sources(ticker: str) -> list[IdeaSource]:
    source = get_next_report_source(ticker)
    return [source] if source else []
