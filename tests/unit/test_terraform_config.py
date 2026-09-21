"""The Terraform stack is a deliverable, so it is tested like one.

``terraform validate`` proves the HCL is well formed; these tests prove the *content* is what the
pipeline needs - lifecycle transitions on every zone, a real redrive policy, log retention, the
metric filters and alarms that back the runbook playbooks, and no wildcard IAM actions.

The observability tests go further than "the alarm exists": every metric identity an alarm or the
dashboard reads is checked against the identities the handlers actually publish (captured from a real
moto run in :func:`published_metric_identities`), because a metric is identified by namespace + name +
the *full* dimension set - an alarm on a dimension set nobody publishes is an alarm that can never
fire.

Parsing is done with ``python-hcl2`` so the tests run everywhere; when the Terraform CLI is present
the same files are additionally checked with ``terraform fmt -check``/``validate`` (see the Makefile
``tf-validate`` target and VERIFY.md).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

# hcl2 is imported loudly, not with pytest.importorskip: python-hcl2 is a declared dev dependency
# (requirements-dev.txt) and CI installs it, so a missing module is a broken environment - skipping
# the whole Terraform suite would hide exactly the drift these tests exist to catch.
import hcl2
import pytest

TF_DIR = Path(__file__).resolve().parents[2] / "infra" / "terraform"

_INT_RE = re.compile(r"^-?\d+$")
_INTERPOLATION_RE = re.compile(r"^\$\{(.*)\}$", re.DOTALL)


def normalise(value: Any) -> Any:
    """Collapse hcl2's token-preserving output into plain Python values.

    This build of ``python-hcl2`` keeps the surrounding quotes on quoted keys and string values
    (``'"aws_s3_bucket"'``) and leaves ``${...}`` wrappers in place; tests want ``aws_s3_bucket`` and
    the bare expression, so both are unwrapped here.
    """
    if isinstance(value, dict):
        return {
            normalise_key(key): normalise(item)
            for key, item in value.items()
            if key != "__is_block__"
        }
    if isinstance(value, list):
        return [normalise(item) for item in value]
    if isinstance(value, str):
        text = value
        if len(text) >= 2 and text.startswith('"') and text.endswith('"'):
            text = text[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        match = _INTERPOLATION_RE.match(text)
        if match:
            text = match.group(1).strip()
        if text == "true":
            return True
        if text == "false":
            return False
        if _INT_RE.match(text):
            return int(text)
        return text
    return value


def normalise_key(key: Any) -> str:
    text = str(key)
    if len(text) >= 2 and text.startswith('"') and text.endswith('"'):
        return text[1:-1]
    return text


def emr_module_path() -> Path:
    """The EMR module lives at ``infra/emr`` and is referenced as ``../emr`` from the TF root."""
    return TF_DIR.parent / "emr" / "main.tf"


def load(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], normalise(hcl2.loads(path.read_text(encoding="utf-8"))))


def flatten(entries: Any) -> dict[str, Any]:
    """hcl2 returns blocks as a list of single-key dicts; collapse them into one mapping."""
    if entries is None:
        return {}
    if isinstance(entries, dict):
        return entries
    merged: dict[str, Any] = {}
    for entry in entries:
        merged.update(entry)
    return merged


def block(document: dict[str, Any], section: str) -> dict[str, Any]:
    return flatten(document.get(section))


def group(document: dict[str, Any], section: str) -> dict[str, Any]:
    """``resource``/``data``: {type: {name: body}}."""
    grouped: dict[str, dict[str, Any]] = {}
    for entry in document.get(section) or []:
        for kind, bodies in entry.items():
            grouped.setdefault(kind, {}).update(bodies)
    return grouped


@pytest.fixture(scope="module")
def stack() -> dict[str, Any]:
    """Every root-module ``*.tf`` merged into one document."""
    merged: dict[str, Any] = {
        "resource": [],
        "variable": [],
        "output": [],
        "locals": [],
        "data": [],
    }
    for path in sorted(TF_DIR.glob("*.tf")):
        document = load(path)
        for section in merged:
            entries = document.get(section)
            if entries:
                merged[section].extend(entries if isinstance(entries, list) else [entries])
    return merged


@pytest.fixture(scope="module")
def emr_module() -> dict[str, Any]:
    return load(emr_module_path())


@pytest.fixture(scope="module")
def published_metric_identities() -> set[str]:
    """Every metric identity this package publishes, captured from a real moto run.

    Running the pipeline is the point: the identities come from the handlers' own EMF documents, so
    the Terraform checks below cannot drift away from what the code emits.
    """
    from dataclasses import replace

    from moto import mock_aws

    from agent_reports.common.aws import reset_client_cache
    from agent_reports.common.settings import Settings
    from agent_reports.pipeline import PipelineOptions, run_local_pipeline
    from agent_reports.testing import provision_local_resources

    settings = Settings(
        region="us-east-1",
        raw_bucket="agent-reports-raw",
        processed_bucket="agent-reports-processed",
        reports_bucket="agent-reports-out",
        agent_queue_url="https://sqs.us-east-1.amazonaws.com/000000000000/agent-reports-fanout",
        dlq_url="https://sqs.us-east-1.amazonaws.com/000000000000/agent-reports-fanout-dlq",
        ses_sender="reports@example.com",
        ses_configuration_set="agent-reports",
        presign_ttl_seconds=900,
        report_date="2026-09-20",
    ).validate()
    with mock_aws():
        created = provision_local_resources(settings, verify_recipients=4)
        resolved = replace(
            settings,
            agent_queue_url=created["main_queue_url"],
            dlq_url=created["dlq_url"],
        )
        reset_client_cache()
        result = run_local_pipeline(
            resolved,
            PipelineOptions(report_date="2026-09-20", agents=3, shards=1),
        )
    assert result.emails_sent == 3, result.as_dict()
    return set(result.metric_identities)


def resources(stack: dict[str, Any], kind: str) -> dict[str, Any]:
    return cast(dict[str, Any], group(stack, "resource").get(kind, {}))


def data_sources(stack: dict[str, Any], kind: str) -> dict[str, Any]:
    return cast(dict[str, Any], group(stack, "data").get(kind, {}))


def variables(stack: dict[str, Any]) -> dict[str, Any]:
    return block(stack, "variable")


def find(stack: dict[str, Any], kind: str, name_contains: str) -> dict[str, Any]:
    matches = {name: body for name, body in resources(stack, kind).items() if name_contains in name}
    assert matches, f"no {kind} matching {name_contains!r}"
    return next(iter(matches.values()))


# --------------------------------------------------------------------------- metric identities
def merged_block(value: Any) -> dict[str, Any]:
    """hcl2 renders a block as a list of single-key dicts; merge it (and pass dicts through)."""
    if isinstance(value, list):
        merged: dict[str, Any] = {}
        for entry in value:
            if isinstance(entry, dict):
                merged.update(entry)
        return merged
    return value if isinstance(value, dict) else {}


def dimension_map(value: Any) -> dict[str, str]:
    """A ``dimensions``/``dimensions = {...}`` block as plain ``{str: str}``."""
    return {normalise_key(key): str(normalise(item)) for key, item in merged_block(value).items()}


def metric_identity(namespace: str, name: str, dimensions: Mapping[str, str]) -> str:
    """``Namespace/Name{Dim=Value,...}`` - how CloudWatch identifies a metric."""
    label = ",".join(f"{key}={dimensions[key]}" for key in sorted(dimensions))
    return f"{namespace}/{name}{{{label}}}"


def alarm_metric_references(alarm: dict[str, Any]) -> list[str]:
    """Every metric identity an alarm reads, single-metric or metric-math."""
    references: list[str] = []
    if alarm.get("metric_name"):
        references.append(
            metric_identity(
                str(alarm.get("namespace", "")),
                str(alarm["metric_name"]),
                dimension_map(alarm.get("dimensions")),
            )
        )
    for query in alarm.get("metric_query") or []:
        metric = merged_block(query).get("metric")
        if metric is None:
            continue
        body = merged_block(metric)
        references.append(
            metric_identity(
                str(normalise(body.get("namespace", ""))),
                str(normalise(body.get("metric_name", ""))),
                dimension_map(body.get("dimensions")),
            )
        )
    return references


#: ``["Namespace", "MetricName", "Dimension", value]`` - the shape every dashboard metric uses.
_DASHBOARD_REFERENCE_RE = re.compile(
    r'\[\s*"(?P<namespace>[A-Za-z0-9/_.-]+)"\s*,\s*"(?P<metric>[A-Za-z0-9/_.-]+)"\s*,'
    r'\s*"(?P<dimension>[A-Za-z0-9/_.-]+)"\s*,\s*(?P<value>"[^"]*"|[^\]\s]+)\s*\]'
)


def dashboard_metric_references() -> list[str]:
    """Every metric identity the dashboard widget definitions read, parsed from the raw HCL.

    The dashboard body is one ``jsonencode({...})`` expression, which ``python-hcl2`` hands back as an
    opaque string, so the references are read from the file text instead of the parsed document.
    """
    text = (TF_DIR / "cloudwatch.tf").read_text(encoding="utf-8")
    references: list[str] = []
    for match in _DASHBOARD_REFERENCE_RE.finditer(text):
        value = match.group("value").strip().strip('"')
        references.append(
            metric_identity(
                match.group("namespace"),
                match.group("metric"),
                {match.group("dimension"): value},
            )
        )
    return references


def filter_metric_identities(stack: dict[str, Any]) -> set[str]:
    """Identities published by the CloudWatch log metric filters (namespace + name + dimensions)."""
    identities: set[str] = set()
    for body in resources(stack, "aws_cloudwatch_log_metric_filter").values():
        transform = flatten(body["metric_transformation"])
        identities.add(
            metric_identity(
                str(transform["namespace"]),
                str(transform["name"]),
                dimension_map(transform.get("dimensions")),
            )
        )
    return identities


class TestBuckets:
    def test_three_zones_exist(self, stack: dict[str, Any]) -> None:
        assert set(resources(stack, "aws_s3_bucket")) == {"raw", "processed", "reports"}

    @pytest.mark.parametrize("zone", ["raw", "processed", "reports"])
    def test_every_zone_is_private_versioned_and_encrypted(
        self, stack: dict[str, Any], zone: str
    ) -> None:
        assert find(stack, "aws_s3_bucket_public_access_block", zone)["bucket"]
        versioning = find(stack, "aws_s3_bucket_versioning", zone)
        assert versioning["versioning_configuration"][0]["status"] == "Enabled"
        encryption = find(stack, "aws_s3_bucket_server_side_encryption_configuration", zone)
        rule = encryption["rule"][0]
        assert rule["apply_server_side_encryption_by_default"][0]["sse_algorithm"] == "AES256"

    def test_raw_zone_tiers_through_ia_and_glacier_ir(self, stack: dict[str, Any]) -> None:
        lifecycle = find(stack, "aws_s3_bucket_lifecycle_configuration", "raw")
        rule = lifecycle["rule"][0]
        transitions = {entry["storage_class"]: entry["days"] for entry in rule["transition"]}
        assert transitions == {"STANDARD_IA": 30, "GLACIER_IR": 90}
        assert rule["expiration"][0]["days"] == 400
        assert rule["noncurrent_version_expiration"][0]["noncurrent_days"] == 60
        # Tiering is only worth it above the 128 KB infrequent-access minimum; the raw part files
        # (~500 KB each) are, and this is the only zone where a transition is declared.
        for zone in ("processed", "reports"):
            for other in find(stack, "aws_s3_bucket_lifecycle_configuration", zone)["rule"]:
                assert "transition" not in other, (zone, other["id"])

    def test_reports_zone_expires(self, stack: dict[str, Any]) -> None:
        rule = find(stack, "aws_s3_bucket_lifecycle_configuration", "reports")["rule"][0]
        assert rule["expiration"][0]["days"] == 120
        assert rule["noncurrent_version_expiration"][0]["noncurrent_days"] == 7
        # A ~1.4 KB report billed at the 128 KB infrequent-access minimum costs ~50x more per month
        # than it does in Standard, so the zone must not tier. See docs/COST.md.
        assert "transition" not in rule

    def test_processed_zone_keeps_state_for_a_year(self, stack: dict[str, Any]) -> None:
        lifecycle = find(stack, "aws_s3_bucket_lifecycle_configuration", "processed")
        rules = {rule["id"]: rule for rule in lifecycle["rule"]}
        assert rules["state-retention"]["expiration"][0]["days"] == 365
        assert rules["quarantine-short-retention"]["expiration"][0]["days"] == 90
        # Dispatch markers are ~400 B: same 128 KB minimum, same reason not to tier.
        assert "transition" not in rules["state-retention"]


class TestQueues:
    def test_redrive_policy_and_visibility_timeout(self, stack: dict[str, Any]) -> None:
        queue = resources(stack, "aws_sqs_queue")["fanout"]
        assert "maxReceiveCount" in queue["redrive_policy"]
        assert "deadLetterTargetArn" in queue["redrive_policy"]
        # Visibility timeout must exceed the dispatcher timeout, or SQS redelivers mid-send.
        assert queue["visibility_timeout_seconds"] > 120
        assert queue["receive_wait_time_seconds"] == 20

    def test_dlq_retains_for_the_maximum(self, stack: dict[str, Any]) -> None:
        assert (
            resources(stack, "aws_sqs_queue")["fanout_dlq"]["message_retention_seconds"] == 1209600
        )

    def test_redrive_allow_policy_is_scoped_to_the_source_queue(
        self, stack: dict[str, Any]
    ) -> None:
        policy = find(stack, "aws_sqs_queue_redrive_allow_policy", "fanout")["redrive_allow_policy"]
        assert "byQueue" in policy
        assert "aws_sqs_queue.fanout.arn" in policy

    def test_queue_policy_denies_insecure_transport(self, stack: dict[str, Any]) -> None:
        statement = data_sources(stack, "aws_iam_policy_document")["fanout_queue"]["statement"][0]
        assert statement["effect"] == "Deny"
        assert statement["condition"][0]["variable"] == "aws:SecureTransport"


class TestLambdas:
    def test_four_functions_with_the_expected_handlers(self, stack: dict[str, Any]) -> None:
        functions = resources(stack, "aws_lambda_function")
        assert set(functions) == {"chunker", "orchestrator", "dispatcher", "presign"}
        for name, body in functions.items():
            assert body["handler"].startswith("agent_reports.lambda_handlers."), name
            assert body["handler"].endswith(".handler"), name
            assert body["runtime"] == "local.lambda_runtime"
            assert body["timeout"] > 0
            assert body["memory_size"] >= 256
        assert block(stack, "locals")["lambda_runtime"] == "python3.11"

    def test_dispatcher_event_source_mapping_reports_partial_batch_failures(
        self, stack: dict[str, Any]
    ) -> None:
        mapping = find(stack, "aws_lambda_event_source_mapping", "dispatcher")
        assert mapping["function_response_types"] == ["ReportBatchItemFailures"]
        assert mapping["batch_size"] == "var.sqs_batch_size"
        assert variables(stack)["sqs_batch_size"]["default"] <= 10

    def test_every_function_has_a_log_group_with_retention(self, stack: dict[str, Any]) -> None:
        groups = resources(stack, "aws_cloudwatch_log_group")
        for name in ("chunker", "orchestrator", "dispatcher", "presign"):
            assert groups[name]["retention_in_days"] == "var.log_retention_days", name
        assert variables(stack)["log_retention_days"]["default"] > 0

    def test_environment_variables_cover_the_settings(self, stack: dict[str, Any]) -> None:
        environment = block(stack, "locals")["environment_variables"]
        for key in (
            "AGENT_REPORTS_RAW_BUCKET",
            "AGENT_REPORTS_PROCESSED_BUCKET",
            "AGENT_REPORTS_REPORTS_BUCKET",
            "AGENT_REPORTS_AGENT_QUEUE_URL",
            "AGENT_REPORTS_DLQ_URL",
            "AGENT_REPORTS_SES_SENDER",
            "AGENT_REPORTS_PRESIGN_TTL_SECONDS",
        ):
            assert key in environment, key

    def test_presign_is_reachable_over_http_api(self, stack: dict[str, Any]) -> None:
        assert find(stack, "aws_apigatewayv2_route", "get_report")["route_key"] == "GET /reports"
        assert (
            find(stack, "aws_lambda_permission", "presign_api")["principal"]
            == "apigateway.amazonaws.com"
        )

    def test_the_presign_route_carries_a_jwt_authorizer_when_one_is_configured(
        self, stack: dict[str, Any]
    ) -> None:
        """The route must not be a bare, unauthenticated integration.

        With ``presign_jwt_issuer`` set the route is JWT-authorised by the gateway; with the default
        empty issuer it is explicitly ``NONE`` *and* the function's header fallback is off, so an
        anonymous request is a 401 rather than another agent's report (see the handler tests).
        """
        route = find(stack, "aws_apigatewayv2_route", "get_report")
        assert route["authorization_type"] == 'local.presign_jwt_enabled ? "JWT" : "NONE"'
        assert "aws_apigatewayv2_authorizer.presign_jwt" in route["authorizer_id"]

        authorizer = find(stack, "aws_apigatewayv2_authorizer", "presign_jwt")
        assert authorizer["authorizer_type"] == "JWT"
        assert authorizer["count"] == "local.presign_jwt_enabled ? 1 : 0"
        configuration = merged_block(authorizer["jwt_configuration"])
        assert str(normalise(configuration["issuer"])) == "var.presign_jwt_issuer"
        assert str(normalise(configuration["audience"])) == "var.presign_jwt_audience"
        assert str(normalise(block(stack, "locals")["presign_jwt_enabled"])) == (
            'var.presign_jwt_issuer != ""'
        )

    def test_the_deployed_environment_never_enables_the_header_identity_fallback(
        self, stack: dict[str, Any]
    ) -> None:
        """``X-Caller-Agent-Id`` is spoofable, so the stack must not turn the fallback on."""
        environment = block(stack, "locals")["environment_variables"]
        assert "AGENT_REPORTS_ALLOW_CALLER_HEADER_FALLBACK" not in environment
        assert variables(stack)["presign_jwt_issuer"]["default"] == ""


class TestIam:
    def test_no_wildcard_actions_anywhere(self, stack: dict[str, Any]) -> None:
        offenders: list[str] = []
        for name, document in data_sources(stack, "aws_iam_policy_document").items():
            for statement in document.get("statement", []):
                actions = statement.get("actions", [])
                if isinstance(actions, str):
                    actions = [actions]
                if "*" in actions:
                    offenders.append(name)
        assert offenders == []

    def test_each_lambda_has_its_own_role_and_policy(self, stack: dict[str, Any]) -> None:
        assert {"chunker", "orchestrator", "dispatcher", "presign"} <= set(
            resources(stack, "aws_iam_role")
        )
        assert len(resources(stack, "aws_iam_role_policy_attachment")) == 4

    def test_chunker_can_only_write_reports(self, stack: dict[str, Any]) -> None:
        statements = data_sources(stack, "aws_iam_policy_document")["chunker"]["statement"]
        writes = [s for s in statements if "s3:PutObject" in s.get("actions", [])]
        assert len(writes) == 1
        assert "aws_s3_bucket.reports.arn" in writes[0]["resources"][0]

    def test_dispatcher_ses_permission_is_scoped_to_the_configuration_set(
        self, stack: dict[str, Any]
    ) -> None:
        statements = data_sources(stack, "aws_iam_policy_document")["dispatcher"]["statement"]
        send = [s for s in statements if "ses:SendEmail" in s.get("actions", [])]
        assert len(send) == 1
        assert "aws_ses_configuration_set.reports.arn" in send[0]["resources"][0]
        assert send[0]["condition"][0]["variable"] == "ses:FromAddress"

    def test_metric_namespace_is_pinned(self, stack: dict[str, Any]) -> None:
        for name in ("chunker", "orchestrator", "dispatcher", "presign"):
            statements = data_sources(stack, "aws_iam_policy_document")[name]["statement"]
            publish = [s for s in statements if "cloudwatch:PutMetricData" in s.get("actions", [])]
            assert publish, name
            assert publish[0]["condition"][0]["values"] == ["AgentReports"]


class TestObservability:
    def test_alarms_cover_dlq_errors_and_lag(self, stack: dict[str, Any]) -> None:
        alarms = resources(stack, "aws_cloudwatch_metric_alarm")
        assert {"dlq_not_empty", "dispatcher_errors", "report_lag"} <= set(alarms)
        for name, alarm in alarms.items():
            assert alarm["alarm_actions"] == ["aws_sns_topic.alarms.arn"], name
            assert alarm["treat_missing_data"] == "notBreaching"

    def test_metric_filters_match_the_events_the_handlers_log(self, stack: dict[str, Any]) -> None:
        patterns = " ".join(
            body["pattern"]
            for body in resources(stack, "aws_cloudwatch_log_metric_filter").values()
        )
        for event in (
            "dispatch_failed",
            "dispatcher_batch_completed",
            "message_quarantined",
            "fanout_completed",
        ):
            assert event in patterns, event

    def test_dashboard_exists(self, stack: dict[str, Any]) -> None:
        assert "pipeline" in resources(stack, "aws_cloudwatch_dashboard")

    # ------------------------------------------------------------------ identity checks
    def test_every_agent_reports_alarm_metric_is_a_published_identity(
        self, stack: dict[str, Any], published_metric_identities: set[str]
    ) -> None:
        """An alarm on a namespace/name/dimension set nobody publishes can never fire.

        This is the check the previous suite was missing: it asserted the alarms existed, not that
        their dimensions were the ones the handlers emit. A metric may be published by the handlers
        (EMF) *or* by a log metric filter; both are legitimate, a third shape is not.
        """
        published = published_metric_identities | filter_metric_identities(stack)
        checked = 0
        for name, alarm in resources(stack, "aws_cloudwatch_metric_alarm").items():
            for reference in alarm_metric_references(alarm):
                if not reference.startswith("AgentReports/"):
                    assert reference.startswith("AWS/"), f"{name}: {reference}"
                    continue  # AWS service metrics are published by AWS, not by this package
                assert reference in published, (
                    f"{name} reads {reference}, which nothing publishes. Published: {sorted(published)}"
                )
                checked += 1
        assert checked >= 3  # dispatcher_errors, report_lag, and the emails-not-sent expression

    def test_the_no_emails_sent_alarm_is_metric_math_not_an_inverted_threshold(
        self, stack: dict[str, Any]
    ) -> None:
        """``MessagesEnqueued > 0`` is true on every successful day - it must not be the alarm."""
        alarm = resources(stack, "aws_cloudwatch_metric_alarm")["emails_not_sent"]
        assert "metric_name" not in alarm, (
            "a single-metric alarm cannot express 'ran but sent nothing'"
        )
        expression = " ".join(
            str(flatten(query).get("expression", "")) for query in alarm["metric_query"]
        )
        assert "AND(" in expression
        assert "delivered == 0" in expression
        assert "enqueued > 0" in expression

    def test_the_dispatcher_failure_filter_publishes_the_dimensions_the_alarm_reads(
        self, stack: dict[str, Any]
    ) -> None:
        """M8: the filter had no ``dimensions`` block, so the alarm's datapoint never existed."""
        transform = flatten(
            resources(stack, "aws_cloudwatch_log_metric_filter")["dispatcher_failures"][
                "metric_transformation"
            ]
        )
        assert transform["name"] == "DispatchFailures"
        assert normalise(transform["dimensions"]) == {"Service": "dispatcher"}
        alarm = resources(stack, "aws_cloudwatch_metric_alarm")["dispatcher_errors"]
        assert alarm["metric_name"] == "DispatchFailures"
        assert normalise(alarm["dimensions"]) == {"Service": "dispatcher"}

    def test_no_log_metric_filter_republishes_an_emf_identity(
        self, stack: dict[str, Any], published_metric_identities: set[str]
    ) -> None:
        """The double-count guard: a metric must have exactly one publishing mechanism.

        A CloudWatch metric is identified by namespace + name + full dimension set, so a metric
        filter that emits the same identity the handlers already publish as EMF doubles every
        ``Sum`` the alarms and dashboard read.
        """
        for name, body in resources(stack, "aws_cloudwatch_log_metric_filter").items():
            transform = flatten(body["metric_transformation"])
            reference = metric_identity(
                str(transform["namespace"]),
                str(transform["name"]),
                normalise(transform.get("dimensions") or {}),
            )
            assert reference not in published_metric_identities, (
                f"metric filter {name} republishes {reference}, which the handlers already emit as EMF"
            )

    def test_every_agent_reports_dashboard_reference_is_a_published_identity(
        self, published_metric_identities: set[str]
    ) -> None:
        """M9: 4 of the dashboard's 8 references used dimension sets the code never published."""
        references = dashboard_metric_references()
        assert len(references) >= 8
        for reference in references:
            if not reference.startswith("AgentReports/"):
                assert reference.startswith("AWS/"), reference
                continue
            assert reference in published_metric_identities, (
                f"dashboard reads {reference}, which no handler publishes. Published: "
                f"{sorted(published_metric_identities)}"
            )

    def test_no_published_identity_carries_a_per_day_dimension(
        self, published_metric_identities: set[str]
    ) -> None:
        """A ReportDate dimension costs a metric-month per day and cannot be alarmed on."""
        assert all("ReportDate" not in identity for identity in published_metric_identities)

    def test_schedules_run_aggregation_before_fanout(self, stack: dict[str, Any]) -> None:
        rules = resources(stack, "aws_cloudwatch_event_rule")
        assert rules["aggregate"]["schedule_expression"] == "var.aggregation_schedule_cron"
        assert rules["fanout"]["schedule_expression"] == "var.report_schedule_cron"
        declared = variables(stack)
        # Aggregation must finish before the fan-out starts on the same day.
        assert declared["aggregation_schedule_cron"]["default"] == "cron(30 1 * * ? *)"
        assert declared["report_schedule_cron"]["default"] == "cron(30 2 * * ? *)"

    def test_one_event_target_per_chunker_shard(self, stack: dict[str, Any]) -> None:
        target = find(stack, "aws_cloudwatch_event_target", "chunker_shard")
        assert target["count"] == "var.chunker_shards"
        assert "shard" in target["input"]


class TestSes:
    def test_identity_and_configuration_set(self, stack: dict[str, Any]) -> None:
        assert "sender" in resources(stack, "aws_ses_email_identity")
        config_set = resources(stack, "aws_ses_configuration_set")["reports"]
        assert config_set["sending_enabled"] is True
        assert config_set["delivery_options"][0]["tls_policy"] == "Require"

    def test_events_are_published_to_cloudwatch(self, stack: dict[str, Any]) -> None:
        destination = resources(stack, "aws_ses_event_destination")["reports_cloudwatch"]
        assert {"bounce", "complaint", "reject", "delivery"} <= set(destination["matching_types"])


class TestVariablesAndOutputs:
    def test_validated_variables_have_bounds(self, stack: dict[str, Any]) -> None:
        declared = variables(stack)
        assert "validation" in declared["name_prefix"]
        assert "validation" in declared["environment"]
        assert "validation" in declared["presign_ttl_seconds"]
        assert declared["enable_emr_module"]["default"] is False

    def test_outputs_describe_the_stack_surface(self, stack: dict[str, Any]) -> None:
        outputs = block(stack, "output")
        for name in (
            "raw_bucket",
            "processed_bucket",
            "reports_bucket",
            "fanout_queue_url",
            "fanout_dlq_url",
            "presign_api_endpoint",
            "emr_application_id",
        ):
            assert name in outputs, name
            assert outputs[name]["description"]


class TestEmrModule:
    def test_module_is_optional_and_off_by_default(
        self, stack: dict[str, Any], emr_module: dict[str, Any]
    ) -> None:
        assert variables(stack)["enable_emr_module"]["default"] is False
        module = block(load(TF_DIR / "emr.tf"), "module")["emr_serverless"]
        assert module["count"] == "var.enable_emr_module ? 1 : 0"
        assert module["source"] == "../emr"
        assert emr_module_path().exists()

    def test_application_is_spark_with_a_capacity_ceiling(self, emr_module: dict[str, Any]) -> None:
        application = group(emr_module, "resource")["aws_emrserverless_application"]["spark"]
        assert application["type"] == "SPARK"
        assert application["auto_stop_configuration"][0]["idle_timeout_minutes"] == 15
        assert application["maximum_capacity"][0]["cpu"] == "16 vCPU"
        assert application["initial_capacity"][0]["initial_capacity_type"] == "Driver"

    def test_job_role_can_only_touch_raw_reports_and_artifacts(
        self, emr_module: dict[str, Any]
    ) -> None:
        document = group(emr_module, "data")["aws_iam_policy_document"]["job_permissions"]
        statements = document["statement"]
        actions = {action for statement in statements for action in statement["actions"]}
        assert "s3:PutObject" in actions
        assert not any(action.startswith(("iam:", "sts:AssumeRole")) for action in actions)
        for statement in statements:
            if "s3:PutObject" in statement["actions"]:
                assert all("reports_bucket" in resource for resource in statement["resources"])

    def test_the_log_permission_grants_something(self, emr_module: dict[str, Any]) -> None:
        """A log ARN without the ``log-group:`` segment matches no resource and grants nothing."""
        document = group(emr_module, "data")["aws_iam_policy_document"]["job_permissions"]
        logs = [
            statement
            for statement in document["statement"]
            if any(str(action).startswith("logs:") for action in statement["actions"])
        ]
        assert logs, "the job role must be able to write its own logs"
        for statement in logs:
            for resource in statement["resources"]:
                assert "log-group:" in str(resource), resource
                assert str(resource).endswith(":*"), resource

    def test_module_outputs_what_a_job_run_needs(self, emr_module: dict[str, Any]) -> None:
        outputs = block(emr_module, "output")
        for name in ("application_id", "job_role_arn", "entry_point", "py_files"):
            assert name in outputs, name
