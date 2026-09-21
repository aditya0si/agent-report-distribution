"""Structured JSON logging for Lambda and EMR, with an Embedded Metric Format (EMF) side channel.

CloudWatch Logs Insights can query JSON fields directly, which is the whole point: every log line is
one JSON object with a stable ``event`` key plus whatever fields the call site passes.

EMF needs the log *event message* to be the metric document (with ``_aws`` at the root), not JSON
wrapped in a ``message`` string. So the package logger carries two handlers:

``json``  - every normal record, rendered by :class:`JsonFormatter`
``emf``   - only records flagged ``extra={"emf": True}``, written verbatim

There are no side effects at import time; :func:`configure_logging` is called from handler/CLI
entry points.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import UTC, datetime
from typing import Any, TextIO

__all__ = [
    "JsonFormatter",
    "configure_logging",
    "get_logger",
    "log_emf",
    "log_event",
]

ROOT_LOGGER_NAME = "agent_reports"

#: LogRecord attributes that must never leak into the JSON payload as "extra" fields.
_RESERVED = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "message",
        "asctime",
        "emf",
    }
)


class JsonFormatter(logging.Formatter):
    """Render a :class:`logging.LogRecord` as a single-line JSON document."""

    def __init__(self, *, service: str = "agent-reports") -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        timestamp = (
            datetime.fromtimestamp(record.created, tz=UTC).isoformat().replace("+00:00", "Z")
        )
        payload: dict[str, Any] = {
            "timestamp": timestamp,
            "level": record.levelname,
            "logger": record.name,
            "service": self.service,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key.startswith("_"):
                continue
            if key in _RESERVED:
                continue
            payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


class _EmfGate(logging.Filter):
    """Pass only records whose ``emf`` flag matches ``want``."""

    def __init__(self, want: bool) -> None:
        super().__init__()
        self.want = want

    def filter(self, record: logging.LogRecord) -> bool:
        return bool(getattr(record, "emf", False)) is self.want


class _EmfHandler(logging.StreamHandler):  # type: ignore[type-arg]
    """Writes the raw message so CloudWatch parses it as an EMF document."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except Exception:  # pragma: no cover - logging must never raise
            self.handleError(record)


def configure_logging(
    level: str | int | None = None,
    *,
    stream: TextIO | None = None,
    force: bool = False,
) -> logging.Logger:
    """Attach the JSON + EMF handlers to the package logger (idempotent unless ``force``)."""
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    resolved = level if level is not None else os.environ.get("AGENT_REPORTS_LOG_LEVEL", "INFO")
    logger.setLevel(resolved)

    if force:
        for handler in list(logger.handlers):
            if getattr(handler, "_agent_reports_handler", False):
                logger.removeHandler(handler)

    if not any(getattr(h, "_agent_reports_handler", False) for h in logger.handlers):
        target = stream if stream is not None else sys.stdout

        json_handler = logging.StreamHandler(target)
        json_handler.setFormatter(JsonFormatter())
        json_handler.addFilter(_EmfGate(want=False))
        json_handler._agent_reports_handler = True  # type: ignore[attr-defined]
        logger.addHandler(json_handler)

        emf_handler = _EmfHandler(target)
        emf_handler.setFormatter(logging.Formatter("%(message)s"))
        emf_handler.addFilter(_EmfGate(want=True))
        emf_handler._agent_reports_handler = True  # type: ignore[attr-defined]
        logger.addHandler(emf_handler)

    logger.propagate = False
    return logger


def get_logger(name: str) -> logging.Logger:
    """Child logger under the package root (never configures handlers itself)."""
    if name == ROOT_LOGGER_NAME or name.startswith(ROOT_LOGGER_NAME + "."):
        return logging.getLogger(name)
    return logging.getLogger(f"{ROOT_LOGGER_NAME}.{name}")


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    level: int = logging.INFO,
    exc_info: bool = False,
    **fields: Any,
) -> dict[str, Any]:
    """Log one structured event and return the dict that was logged."""
    payload: dict[str, Any] = {"event": event}
    payload.update(fields)
    logger.log(level, event, extra=payload, exc_info=exc_info or None)
    return payload


def log_emf(logger: logging.Logger, document: dict[str, Any]) -> str:
    """Write an EMF document verbatim to the log stream; returns the serialised line."""
    line = json.dumps(document, default=str, separators=(",", ":"))
    logger.info(line, extra={"emf": True})
    return line
