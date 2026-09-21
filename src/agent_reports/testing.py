"""Local-environment helpers built on moto, for the e2e script and the integration tests.

Kept out of the production import graph: :mod:`agent_reports.pipeline` never imports this module, and
``moto`` is imported lazily inside the functions that need its internals. That way the same
``run_local_pipeline`` runs against moto, LocalStack, or real AWS.
"""

from __future__ import annotations

from typing import Any

import boto3
from botocore.exceptions import ClientError

from .common.aws import reset_client_cache
from .common.keys import report_key
from .common.settings import Settings
from .common.storage import Zones, open_zones
from .pipeline import extract_download_url

__all__ = [
    "ensure_bucket",
    "fetch_url",
    "provision_local_resources",
    "sent_messages",
    "verify_presigned_delivery",
]


def ensure_bucket(s3: Any, bucket: str) -> None:
    """Create the bucket unless it already exists."""
    try:
        s3.head_bucket(Bucket=bucket)
        return
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in ("404", "NoSuchBucket", "NotFound"):
            raise
    s3.create_bucket(Bucket=bucket)


def provision_local_resources(settings: Settings, *, verify_recipients: int = 200) -> dict[str, str]:
    """Create the buckets, queues (with redrive), SES identities and configuration set.

    In real AWS this is ``terraform apply``; here it is the same shape so the offline path exercises
    the same queue attributes (visibility timeout, redrive policy) as production.
    """
    reset_client_cache()
    s3 = boto3.client("s3", region_name=settings.region, endpoint_url=settings.endpoint_url)
    sqs = boto3.client("sqs", region_name=settings.region, endpoint_url=settings.endpoint_url)
    ses = boto3.client("ses", region_name=settings.region, endpoint_url=settings.endpoint_url)

    for bucket in (settings.raw_bucket, settings.processed_bucket, settings.reports_bucket):
        ensure_bucket(s3, bucket)

    dlq_name = settings.dlq_url.rstrip("/").rsplit("/", 1)[-1] or "agent-reports-fanout-dlq"
    main_name = (
        settings.agent_queue_url.rstrip("/").rsplit("/", 1)[-1] or "agent-reports-fanout"
    )
    dlq_url = sqs.create_queue(
        QueueName=dlq_name,
        Attributes={"MessageRetentionPeriod": "1209600"},
    )["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    import json  # noqa: PLC0415 - only needed for the redrive policy document

    main_url = sqs.create_queue(
        QueueName=main_name,
        Attributes={
            "VisibilityTimeout": "60",
            "RedrivePolicy": json.dumps(
                {"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"}
            ),
        },
    )["QueueUrl"]

    ses.verify_email_identity(EmailAddress=settings.ses_sender)
    try:
        ses.create_configuration_set(ConfigurationSet={"Name": settings.ses_configuration_set})
    except ClientError as exc:
        if "AlreadyExists" not in str(exc.response.get("Error", {}).get("Code", "")):
            raise
    # SES sandbox semantics: both ends must be verified, so pre-verify the synthetic agent domain.
    for index in range(1, verify_recipients + 1):
        ses.verify_email_identity(EmailAddress=f"agt-{index:06d}@example.com")

    return {"main_queue_url": main_url, "dlq_url": dlq_url, "dlq_arn": dlq_arn}


def sent_messages(region: str) -> list[Any]:
    """Every message moto's SES backend has accepted (the offline equivalent of the SES store)."""
    from moto.ses.models import ses_backends  # noqa: PLC0415 - moto is test-only

    account = list(ses_backends.values())[0]
    backend = account[region]
    return list(backend.sent_messages)


def verify_presigned_delivery(
    settings: Settings,
    *,
    region: str,
    recipient: str,
    agent_id: str,
    report_date: str,
    zones: Zones | None = None,
) -> dict[str, Any]:
    """End-to-end delivery check: read the sent email, fetch its embedded link, compare the bytes."""
    messages = [
        message for message in sent_messages(region) if recipient in str(message.destinations)
    ]
    if not messages:
        raise AssertionError(f"no SES message captured for {recipient}")
    message = messages[-1]
    url = extract_download_url(str(message.body))
    status, payload = fetch_url(url)
    if status != 200:
        raise AssertionError(f"pre-signed URL returned HTTP {status}")

    active_zones = zones or open_zones(settings)
    key = report_key(report_date, agent_id)
    expected = active_zones.reports.get_bytes(key)
    if payload != expected:
        raise AssertionError(
            f"pre-signed URL body ({len(payload)} bytes) does not match the report object "
            f"({len(expected)} bytes)"
        )
    return {
        "recipient": recipient,
        "agent_id": agent_id,
        "report_key": key,
        "url_scheme": url.split(":", 1)[0],
        "url_query_keys": sorted(
            part.split("=", 1)[0] for part in url.split("?", 1)[-1].split("&") if "=" in part
        ),
        "http_status": status,
        "bytes": len(payload),
        "matches_report_object": True,
    }


def fetch_url(url: str, *, timeout: int = 15) -> tuple[int, bytes]:
    """GET a pre-signed URL (moto intercepts it inside ``mock_aws``)."""
    import requests  # noqa: PLC0415 - requests only exists for the local/offline path

    response = requests.get(url, timeout=timeout)
    return response.status_code, response.content
