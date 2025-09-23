from __future__ import annotations

from pathlib import Path
from threading import Lock
from typing import Dict, Literal, Set

import yaml

from .._loguru import logger
from ..config import settings

UniverseDict = Dict[str, Set[str]]

_LOCK = Lock()
_UNIVERSE_CACHE: tuple[Path, float, UniverseDict, bool] | None = None
_ALLOWED_KEYS = {"STOCKS", "BONDS", "ETFS"}


def _normalize_symbol(value: str | None) -> str | None:
    if not value:
        return None
    symbol = value.strip().upper()
    if not symbol:
        return None
    if ";" in symbol:
        symbol = symbol.split(";", 1)[0].strip().upper()
    return symbol or None


def load_universe(path: str | Path) -> UniverseDict:
    """Load Tinkoff tradable universe from YAML."""

    normalized: UniverseDict = {key: set() for key in _ALLOWED_KEYS}
    p = Path(path)
    if not p.exists():
        return normalized

    with p.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    if not isinstance(data, dict):
        return normalized

    for raw_key, raw_values in data.items():
        key = str(raw_key).strip().upper()
        if key not in _ALLOWED_KEYS:
            continue
        bucket = normalized[key]
        if isinstance(raw_values, (list, tuple, set)):
            values = raw_values
        else:
            values = [raw_values]
        for item in values:
            symbol = _normalize_symbol(str(item)) if item is not None else None
            if symbol:
                bucket.add(symbol)
    return normalized


def _load_configured_universe() -> tuple[UniverseDict, bool]:
    global _UNIVERSE_CACHE
    path = Path(settings.TINKOFF_UNIVERSE_PATH)
    try:
        stat = path.stat()
    except FileNotFoundError:
        with _LOCK:
            should_log = (
                _UNIVERSE_CACHE is None
                or _UNIVERSE_CACHE[0] != path
                or _UNIVERSE_CACHE[3] is not False
            )
            if should_log:
                logger.info(
                    "Tinkoff universe file %s not found; allowing all instruments",
                    path,
                )
            _UNIVERSE_CACHE = (path, 0.0, {key: set() for key in _ALLOWED_KEYS}, False)
            data = _UNIVERSE_CACHE[2]
        return data, False

    mtime = stat.st_mtime
    with _LOCK:
        if (
            _UNIVERSE_CACHE is None
            or _UNIVERSE_CACHE[0] != path
            or _UNIVERSE_CACHE[1] != mtime
        ):
            universe = load_universe(path)
            _UNIVERSE_CACHE = (path, mtime, universe, True)
        data = _UNIVERSE_CACHE[2]
        strict = _UNIVERSE_CACHE[3]
    return data, strict


def _bucket_name(sec_type: Literal["stock", "bond", "etf"] | str) -> str:
    mapping = {
        "stock": "STOCKS",
        "bond": "BONDS",
        "etf": "ETFS",
    }
    return mapping.get(sec_type.lower(), "STOCKS")


def is_tradable(ticker: str, sec_type: Literal["stock", "bond", "etf"] | str) -> bool:
    symbol = _normalize_symbol(ticker)
    if not symbol:
        return False

    sec_type_normalized = sec_type.lower() if isinstance(sec_type, str) else "stock"
    if sec_type_normalized not in {"stock", "bond", "etf"}:
        return True

    universe, strict = _load_configured_universe()
    if not strict:
        return True

    bucket = universe.get(_bucket_name(sec_type_normalized), set())
    if not bucket:
        return False
    return symbol in bucket


def _reset_cache_for_tests() -> None:
    global _UNIVERSE_CACHE
    with _LOCK:
        _UNIVERSE_CACHE = None
