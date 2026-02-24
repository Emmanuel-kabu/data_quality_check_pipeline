"""
Retry Handler Module
====================
Provides retry-with-exponential-backoff for transient failures
during pipeline execution (e.g., network issues, cloud storage timeouts).
"""

import functools
import logging
import time
from typing import Any, Callable, TypeVar

from src.config import RETRY_CONFIG

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class RetryError(Exception):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, func_name: str, attempts: int, last_exception: Exception) -> None:
        self.func_name = func_name
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(
            f"{func_name} failed after {attempts} attempts. "
            f"Last error: {last_exception}"
        )


def retry_with_backoff(
    max_retries: int | None = None,
    base_delay: float | None = None,
    max_delay: float | None = None,
    exponential_base: float | None = None,
    retryable_exceptions: tuple | None = None,
) -> Callable:
    """
    Decorator that retries a function with exponential backoff.

    Usage:
        @retry_with_backoff()
        def upload_to_s3(data):
            ...

        @retry_with_backoff(max_retries=5, base_delay=1)
        def fetch_data():
            ...
    """
    _max_retries = max_retries or RETRY_CONFIG["max_retries"]
    _base_delay = base_delay or RETRY_CONFIG["base_delay_seconds"]
    _max_delay = max_delay or RETRY_CONFIG["max_delay_seconds"]
    _exp_base = exponential_base or RETRY_CONFIG["exponential_base"]

    # Build tuple of retryable exception classes
    if retryable_exceptions is None:
        _retryable = (ConnectionError, TimeoutError, OSError)
    else:
        _retryable = retryable_exceptions

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Exception | None = None

            for attempt in range(1, _max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except _retryable as exc:
                    last_exc = exc
                    if attempt == _max_retries:
                        logger.error(
                            "RETRY EXHAUSTED: %s failed after %d attempts. "
                            "Last error: %s",
                            func.__name__, attempt, exc,
                        )
                        raise RetryError(func.__name__, attempt, exc) from exc

                    delay = min(_base_delay * (_exp_base ** (attempt - 1)), _max_delay)
                    logger.warning(
                        "RETRY %d/%d: %s failed with %s. "
                        "Retrying in %.1fs...",
                        attempt, _max_retries, func.__name__,
                        type(exc).__name__, delay,
                    )
                    time.sleep(delay)

            # Should not reach here, but just in case
            raise RetryError(func.__name__, _max_retries, last_exc)

        return wrapper  # type: ignore
    return decorator


class RetryableOperation:
    """Context manager for retryable operations with tracking."""

    def __init__(self, operation_name: str, max_retries: int | None = None) -> None:
        self.operation_name = operation_name
        self.max_retries = max_retries or RETRY_CONFIG["max_retries"]
        self.attempts: list[dict] = []

    def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute a function with retry logic."""
        base_delay = RETRY_CONFIG["base_delay_seconds"]
        max_delay = RETRY_CONFIG["max_delay_seconds"]
        exp_base = RETRY_CONFIG["exponential_base"]

        for attempt in range(1, self.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                self.attempts.append({
                    "attempt": attempt,
                    "status": "success",
                    "error": None,
                })
                return result
            except Exception as exc:
                self.attempts.append({
                    "attempt": attempt,
                    "status": "failed",
                    "error": str(exc),
                })

                if attempt == self.max_retries:
                    logger.error(
                        "Operation '%s' failed after %d attempts",
                        self.operation_name, attempt,
                    )
                    raise

                delay = min(base_delay * (exp_base ** (attempt - 1)), max_delay)
                logger.warning(
                    "Operation '%s' attempt %d/%d failed: %s. Retrying in %.1fs...",
                    self.operation_name, attempt, self.max_retries, exc, delay,
                )
                time.sleep(delay)
