"""Minimal subset of the responses API for offline tests."""
from __future__ import annotations

import json
import re
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Callable, Pattern
from urllib.parse import urlencode

from app import _requests as requests

GET = "GET"

_registry: list[tuple[str, Any, dict[str, Any], int]] = []
calls: list[SimpleNamespace] = []


def add(method: str, url: str | Pattern[str], json: dict[str, Any], status: int = 200) -> None:
    _registry.append((method, url, json, status))


def _match(url: str, target: str | Pattern[str]) -> bool:
    if isinstance(target, re.Pattern):
        return bool(target.match(url))
    return url == target or url.startswith(f"{target}?")


def _build_url(url: str, params: dict[str, Any] | None) -> str:
    if not params:
        return url
    return f"{url}?{urlencode(params, doseq=True)}"


class _DummyResponse:
    def __init__(self, payload: dict[str, Any], status: int = 200):
        self._payload = payload
        self.status_code = status

    def json(self) -> dict[str, Any]:
        return json.loads(json.dumps(self._payload))

    def raise_for_status(self) -> None:
        if not (200 <= self.status_code < 400):
            raise requests.HTTPError(f"status={self.status_code}")


@contextmanager
def _patched() -> Callable[..., Any]:
    original_get = requests.get

    def fake_get(url: str, params: dict[str, Any] | None = None, **kwargs: Any):
        full_url = _build_url(url, params)
        for method, target, payload, status in list(_registry):
            if method == GET and _match(full_url, target):
                calls.append(SimpleNamespace(request=SimpleNamespace(url=full_url), response=SimpleNamespace(status=status)))
                return _DummyResponse(payload, status=status)
        raise AssertionError(f"Unexpected GET {full_url}")

    try:
        requests.get = fake_get
        yield fake_get
    finally:
        requests.get = original_get
        _registry.clear()


def activate(func: Callable) -> Callable:
    def wrapper(*args: Any, **kwargs: Any):
        with _patched():
            return func(*args, **kwargs)

    return wrapper


def reset() -> None:
    _registry.clear()
    calls.clear()
