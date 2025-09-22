from __future__ import annotations

try:  # pragma: no cover
    from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
except ImportError:  # pragma: no cover
    def retry(*args, **kwargs):
        def decorator(func):
            return func

        return decorator

    def retry_if_exception_type(*args, **kwargs):
        return None

    def stop_after_attempt(*args, **kwargs):
        return None

    def wait_exponential(*args, **kwargs):
        return None

__all__ = [
    "retry",
    "retry_if_exception_type",
    "stop_after_attempt",
    "wait_exponential",
]
