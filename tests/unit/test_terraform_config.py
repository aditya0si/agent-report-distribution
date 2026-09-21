"""The Terraform stack is a deliverable, so it is tested like one.

``terraform validate`` proves the HCL is well formed; these tests prove the *content* is what the
pipeline needs - lifecycle transitions on every zone, a real redrive policy, log retention, the
metric filters and alarms that back the runbook playbooks, and no wildcard IAM actions.

Parsing is done with ``python-hcl2`` so the tests run everywhere; when the Terraform CLI is present
the same files are additionally checked with ``terraform fmt -check``/``validate`` (see the Makefile
``tf-validate`` target and VERIFY.md).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

import pytest

hcl2 = pytest.importorskip("hcl2")

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

    def test_reports_zone_expires(self, stack: dict[str, Any]) -> None:
        rule = find(stack, "aws_s3_bucket_lifecycle_configuration", "reports")["rule"][0]
        assert rule["expiration"][0]["days"] == 120
        assert rule["noncurrent_version_expiration"][0]["noncurrent_days"] == 7

    def test_processed_zone_keeps_state_for_a_year(self, stack: dict[str, Any]) -> None:
        lifecycle = find(stack, "aws_s3_bucket_lifecycle_configuration", "processed")
        rules = {rule["id"]: rule for rule in lifecycle["rule"]}
        assert rules["state-retention"]["expiration"][0]["days"] == 365
        assert rules["quarantine-short-retention"]["expiration"][0]["days"] == 90


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

    def test_module_outputs_what_a_job_run_needs(self, emr_module: dict[str, Any]) -> None:
        outputs = block(emr_module, "output")
        for name in ("application_id", "job_role_arn", "entry_point", "py_files"):
            assert name in outputs, name
