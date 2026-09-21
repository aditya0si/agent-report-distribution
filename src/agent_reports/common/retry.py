"""Exponential backoff with jitter, plus a retry driver that understands the error taxonomy.

The AWS SDKs already retry, but they retry *blindly* against a single client. This module is the
pipeline's own policy layer: it knows which failures are safe to repeat, it caps total wall-clock
time, it adds full jitter so a fleet of Lambda invocations does not synchronise after a throttling
event, and it reports every attempt through a callback so the dispatcher can emit metrics.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeVar

from . import errors
from .logging_utils import get_logger, log_event

__all__ = ["RetryPolicy", "RetryStats", "call_with_retry", "compute_delay"]

T = TypeVar("T")
_LOG = get_logger(__name__)

JITTER_MODES = ("full", "equal", "none")


@dataclass(frozen=True)
class RetryPolicy:
    """Backoff parameters.

    ``full`` jitter (``random * min(cap, base * 2**attempt)``) is the default: it is the AWS
    recommended strategy and it is the one tested here.
    """

    max_attempts: int = 5
    base_delay: float = 0.05
    max_delay: float = 2.0
    jitter: str = "full"
    retry_on: tuple[type[BaseException], ...] = ()

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be >= 0")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if self.jitter not in JITTER_MODES:
            raise ValueError(f"jitter must be one of {JITTER_MODES} (got {self.jitter!r})")

    def ceiling(self, attempt: int) -> float:
        """Un-jittered delay ceiling for a 0-based attempt number."""
        return float(min(self.max_delay, self.base_delay * (2.0**attempt)))


@dataclass
class RetryStats:
    """What actually happened - returned to callers that need to report metrics."""

    attempts: int = 0
    total_sleep: float = 0.0
    retried_codes: list[str] = field(default_factory=list)

    @property
    def retries(self) -> int:
        return max(0, self.attempts - 1)


def compute_delay(
    attempt: int, policy: RetryPolicy, rand: Callable[[], float] = random.random
) -> float:
    """Delay in seconds before attempt ``attempt + 1`` (0-based ``attempt``)."""
    ceiling = policy.ceiling(attempt)
    if ceiling <= 0:
        return 0.0
    if policy.jitter == "none":
        return ceiling
    if policy.jitter == "equal":
        return ceiling / 2 + rand() * (ceiling / 2)
    return rand() * ceiling  # full jitter


def call_with_retry(
    fn: Callable[[], T],
    *,
    policy: RetryPolicy | None = None,
    operation: str = "call",
    sleep: Callable[[float], None] = time.sleep,
    rand: Callable[[], float] = random.random,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
    stats: RetryStats | None = None,
) -> T:
    """Call ``fn`` until it succeeds, the failure is permanent, or the budget is spent.

    Raises the *classified* :class:`agent_reports.common.errors.AgentReportsError`, so callers can
    branch on ``exc.retryable`` without re-parsing botocore payloads.
    """
    active_policy = policy or RetryPolicy()
    counters = stats if stats is not None else RetryStats()
    last: errors.AgentReportsError | None = None

    for attempt in range(active_policy.max_attempts):
        counters.attempts += 1
        try:
            return fn()
        except BaseException as exc:
            classified = errors.classify(exc)
            if not _should_retry(exc, classified, active_policy):
                raise classified from exc
            last = classified
            if attempt == active_policy.max_attempts - 1:
                break
            delay = compute_delay(attempt, active_policy, rand)
            counters.total_sleep += delay
            counters.retried_codes.append(classified.code)
            if on_retry is not None:
                on_retry(attempt + 1, classified, delay)
            log_event(
                _LOG,
                "retry_scheduled",
                operation=operation,
                attempt=attempt + 1,
                max_attempts=active_policy.max_attempts,
                delay_seconds=round(delay, 4),
                error_code=classified.code,
                error_message=classified.message,
            )
            sleep(delay)

    assert last is not None  # loop always sets `last` before breaking
    raise errors.DependencyError(
        f"{operation} failed after {active_policy.max_attempts} attempts",
        context={
            "last_error_code": last.code,
            "last_error_message": last.message,
            "attempts": counters.attempts,
            "total_sleep_seconds": round(counters.total_sleep, 4),
        },
    ) from last


def _should_retry(
    exc: BaseException, classified: errors.AgentReportsError, policy: RetryPolicy
) -> bool:
    if policy.retry_on and isinstance(exc, policy.retry_on):
        return True
    return classified.retryable
