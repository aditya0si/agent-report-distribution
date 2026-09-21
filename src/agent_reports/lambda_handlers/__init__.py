"""Lambda handlers: orchestrator (fan-out), dispatcher (SQS -> SES), presign (API Gateway),
chunker (free-tier aggregation that replaces the EMR step for modest volumes).

``lambda`` is a Python keyword, so the package is named ``lambda_handlers``; the deployed function
names in ``infra/terraform`` are still ``...-orchestrator``, ``...-dispatcher``, ``...-presign`` and
``...-chunker``.
"""

from __future__ import annotations

__all__: list[str] = []
