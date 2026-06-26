"""Shared resilience helpers: retry with backoff and simple rate limiting.

Every external API call in sources/ should go through these so retry and
rate-limit policy lives in exactly one place.
"""
from __future__ import annotations

import time
from functools import wraps
from typing import Callable

from tenacity import retry, stop_after_attempt, wait_exponential_jitter


def with_retry(max_attempts: int = 5) -> Callable:
    """Retry on any Exception with exponential backoff + jitter (2..30s)."""
    def decorator(fn: Callable) -> Callable:
        wrapped = retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential_jitter(initial=2, max=30),
            reraise=True,
        )(fn)
        return wrapped
    return decorator


def rate_limited(min_interval_s: float) -> Callable:
    """Ensure at least `min_interval_s` between successive calls of the wrapped fn."""
    def decorator(fn: Callable) -> Callable:
        last = {"t": 0.0}

        @wraps(fn)
        def inner(*args, **kwargs):
            elapsed = time.monotonic() - last["t"]
            if elapsed < min_interval_s:
                time.sleep(min_interval_s - elapsed)
            try:
                return fn(*args, **kwargs)
            finally:
                last["t"] = time.monotonic()
        return inner
    return decorator
