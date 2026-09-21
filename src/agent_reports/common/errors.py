"""Error taxonomy: every failure raised by this package is classified retryable or permanent.

The dispatcher (SQS-triggered) turns retryable failures into ``batchItemFailures`` so SQS redelivers
the message and finally moves it to the DLQ; permanent failures are logged as a distinct metric and
the message is acknowledged so a poison payload cannot loop forever.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

import botocore.exceptions

__all__ = [
    "AgentReportsError",
    "AuthzDeniedError",
    "ConfigError",
    "DependencyError",
    "InvalidMessageError",
    "MissingReportError",
    "PermanentError",
    "ReportNotFoundError",
    "RetryableError",
    "ThrottlingError",
    "TransientError",
    "classify",
    "is_retryable",
]

# botocore error codes that mean "call again later".
RETRYABLE_CODES: frozenset[str] = frozenset(
    {
        "Throttling",
        "ThrottlingException",
        "ThrottledException",
        "TooManyRequestsException",
        "ProvisionedThroughputExceededException",
        "RequestThrottled",
        "RequestThrottledException",
        "RequestTimeout",
        "RequestTimeoutException",
        "PriorRequestNotComplete",
        "SlowDown",
        "ServiceUnavailable",
        "ServiceUnavailableException",
        "InternalError",
        "InternalFailure",
        "InternalServerError",
        "ServerSideEncryptionConfigurationNotFoundError",
        "TransactionInProgressException",
        "EC2ThrottledException",
    }
)

# Explicitly permanent: retrying these cannot help.
PERMANENT_CODES: frozenset[str] = frozenset(
    {
        "AccessDenied",
        "AccessDeniedException",
        "AccountSendingPausedException",
        "AuthorizationError",
        "InvalidParameterValue",
        "InvalidParameterValueException",
        "MailFromDomainNotVerifiedException",
        "MessageRejected",
        "NoSuchBucket",
        "NoSuchKey",
        "ResourceNotFoundException",
        "SignatureDoesNotMatch",
        "UnauthorizedOperation",
        "ValidationError",
        "ValidationException",
    }
)


class AgentReportsError(Exception):
    """Base class for every error raised by this package."""

    retryable: ClassVar[bool] = False
    code: ClassVar[str] = "AgentReportsError"

    def __init__(self, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.context: dict[str, Any] = dict(context or {})

    def as_log_fields(self) -> dict[str, Any]:
        """Flat dict suitable for structured logging."""
        fields: dict[str, Any] = {
            "error_code": self.code,
            "error_message": self.message,
            "retryable": self.retryable,
        }
        fields.update(self.context)
        return fields

    def __str__(self) -> str:  # pragma: no cover - trivial
        if not self.context:
            return self.message
        rendered = ", ".join(f"{k}={v!r}" for k, v in sorted(self.context.items()))
        return f"{self.message} ({rendered})"


class PermanentError(AgentReportsError):
    """Retrying will not help; the message should be dropped after being recorded."""

    retryable: ClassVar[bool] = False
    code: ClassVar[str] = "PermanentError"


class TransientError(AgentReportsError):
    """Retrying has a real chance of succeeding."""

    retryable: ClassVar[bool] = True
    code: ClassVar[str] = "TransientError"


class RetryableError(TransientError):
    """Alias with a name that matches the ``is_retryable`` API."""

    code: ClassVar[str] = "RetryableError"


class ThrottlingError(TransientError):
    """Upstream service asked us to back off (SES/SQS/S3)."""

    code: ClassVar[str] = "ThrottlingError"


class DependencyError(TransientError):
    """An AWS dependency is unhealthy or incompletely provisioned right now."""

    code: ClassVar[str] = "DependencyError"


class ReportNotFoundError(DependencyError):
    """The report object is not (yet) in S3 - upstream aggregation may still be running."""

    code: ClassVar[str] = "ReportNotFoundError"


class MissingReportError(ReportNotFoundError):
    """Explicit alias used by the dispatcher when a report is absent."""

    code: ClassVar[str] = "MissingReportError"


class InvalidMessageError(PermanentError):
    """The SQS payload / request body cannot be parsed or fails validation."""

    code: ClassVar[str] = "InvalidMessageError"


class AuthzDeniedError(PermanentError):
    """The caller is not allowed to read the requested agent's report."""

    code: ClassVar[str] = "AuthzDeniedError"


class ConfigError(PermanentError):
    """Environment configuration is missing or invalid."""

    code: ClassVar[str] = "ConfigError"


def classify(exc: BaseException) -> AgentReportsError:
    """Wrap an arbitrary exception in this package's taxonomy.

    Mapping rules (deliberately explicit, no blanket "retry everything"):
      * already-taxonomised -> returned unchanged
      * botocore ``ClientError`` -> by HTTP status (5xx retryable, 429 retryable, else permanent),
        then by service error code against ``RETRYABLE_CODES`` / ``PERMANENT_CODES``
      * botocore connection/timeout errors -> :class:`DependencyError` (retryable)
      * everything else -> :class:`PermanentError` (unknown failures are not retried blindly)
    """
    if isinstance(exc, AgentReportsError):
        return exc

    if isinstance(exc, botocore.exceptions.ClientError):
        response = exc.response if isinstance(exc.response, dict) else {}
        error = response.get("Error", {}) if isinstance(response, dict) else {}
        aws_code = str(error.get("Code", "Unknown"))
        http_status = _status_code(response)
        context: dict[str, Any] = {
            "aws_error_code": aws_code,
            "http_status": http_status,
            "operation": response.get("ResponseMetadata", {}).get("RequestId", None)
            if isinstance(response.get("ResponseMetadata"), dict)
            else None,
        }
        if aws_code in PERMANENT_CODES:
            return PermanentError(f"AWS rejected the call: {aws_code}", context=context)
        if aws_code in RETRYABLE_CODES:
            return DependencyError(
                f"AWS call failed with retryable code {aws_code}", context=context
            )
        if http_status is not None and http_status >= 500:
            return DependencyError(f"AWS call failed with HTTP {http_status}", context=context)
        if http_status == 429:
            return ThrottlingError("AWS throttled the call (HTTP 429)", context=context)
        return PermanentError(f"AWS call failed: {aws_code}", context=context)

    if isinstance(
        exc,
        (
            botocore.exceptions.ConnectionError,
            botocore.exceptions.EndpointConnectionError,
            botocore.exceptions.ConnectTimeoutError,
            botocore.exceptions.ReadTimeoutError,
            botocore.exceptions.HTTPClientError,
        ),
    ):
        return DependencyError(f"transport failure: {type(exc).__name__}: {exc}")

    if isinstance(exc, (TimeoutError, ConnectionError)):
        return DependencyError(f"transport failure: {type(exc).__name__}: {exc}")

    return PermanentError(f"{type(exc).__name__}: {exc}")


def is_retryable(exc: BaseException) -> bool:
    """True when *exc* is (or maps to) a retryable failure."""
    return classify(exc).retryable


def _status_code(response: Mapping[str, Any]) -> int | None:
    meta = response.get("ResponseMetadata")
    if isinstance(meta, dict):
        raw = meta.get("HTTPStatusCode")
        if isinstance(raw, int):
            return raw
        if isinstance(raw, str) and raw.isdigit():
            return int(raw)
    return None
