from __future__ import annotations

try:  # pragma: no cover - prefer real loguru when available
    from loguru import logger  # type: ignore
except ImportError:  # pragma: no cover - fallback to stdlib logging
    import logging
    from typing import Any

    logging.basicConfig(level=logging.INFO)

    class _Adapter:
        def __init__(self) -> None:
            self._logger = logging.getLogger("tg-fin-assistant")

        def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
            exc = kwargs.pop("exc", None)
            if exc:
                kwargs["exc_info"] = exc
            self._logger.warning(message, *args, **kwargs)

        def error(self, message: str, *args: Any, **kwargs: Any) -> None:
            exc = kwargs.pop("exc", None)
            if exc:
                kwargs["exc_info"] = exc
            self._logger.error(message, *args, **kwargs)

        def info(self, message: str, *args: Any, **kwargs: Any) -> None:
            exc = kwargs.pop("exc", None)
            if exc:
                kwargs["exc_info"] = exc
            self._logger.info(message, *args, **kwargs)

        def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
            exc = kwargs.pop("exc", None)
            if exc:
                kwargs["exc_info"] = exc
            self._logger.debug(message, *args, **kwargs)

    logger = _Adapter()

__all__ = ["logger"]
