from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

try:  # pragma: no cover - prefer real requests when available
    import requests as _real
except ImportError:  # pragma: no cover - fallback implementation
    from urllib.error import HTTPError as _UrlHTTPError, URLError
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    class RequestException(Exception):
        """Base exception for HTTP errors."""

    class HTTPError(RequestException):
        """Raised when HTTP returns non-success status."""

    @dataclass
    class Response:
        status_code: int
        _body: bytes

        def json(self) -> Dict[str, Any]:
            return json.loads(self._body.decode("utf-8"))

        def raise_for_status(self) -> None:
            if not (200 <= self.status_code < 400):
                raise HTTPError(f"status {self.status_code}")

    def get(
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> Response:
        full_url = url
        if params:
            full_url = f"{url}?{urlencode(params, doseq=True)}"
        request = Request(full_url, headers=headers or {})
        try:
            with urlopen(request, timeout=timeout) as resp:
                body = resp.read()
                return Response(status_code=resp.status, _body=body)
        except _UrlHTTPError as exc:
            raise HTTPError(str(exc)) from exc
        except URLError as exc:
            raise RequestException(str(exc)) from exc
else:  # pragma: no cover - direct proxy to real requests
    RequestException = _real.RequestException
    HTTPError = _real.HTTPError

    def get(
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ):
        return _real.get(url, params=params, headers=headers, timeout=timeout)

    Response = _real.Response
