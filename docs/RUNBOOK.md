# Runbook

Operational notes for the agent-report pipeline. Everything here is either a command that was run
during development or a procedure that maps to a specific failure mode in the code.

## 1. Run it locally (offline, no AWS account)

```bash
uv venv .venv --python 3.11
uv pip install --python .venv/bin/python -r requirements-dev.txt
uv pip install --python .venv/bin/python -e . --no-deps

python scripts/e2e_local.py --rows 5000 --shards 2      # whole pipeline under moto, prints a report
python -m pytest tests -q                               # unit + integration
```

`scripts/e2e_local.py` generates a synthetic day, aggregates it with the chunker, fans out over SQS,
delivers through SES, then opens the pre-signed link found inside a delivered email and compares the
bytes with the report object. It exits non-zero if any invariant fails.

Spark (the EMR path) needs a JVM:

```bash
# any Java 17+; Temurin 21 is what CI uses
export JAVA_HOME=/path/to/jdk-21-jre
# Windows only: Spark's local file access needs Hadoop's winutils.exe + hadoop.dll
export HADOOP_HOME=/path/to/hadoop          # must contain bin/winutils.exe and bin/hadoop.dll
export SPARK_LOCAL_IP=127.0.0.1             # needed when the machine hostname resolves to a
export PYSPARK_PYTHON=$PWD/.venv/Scripts/python.exe   # link-local IPv6 address (see below)
python -m pytest tests/integration/test_spark_job.py -q
```

Windows notes that cost real debugging time:

- without `winutils.exe`/`hadoop.dll` the job dies with
  `UnsatisfiedLinkError: NativeIO$Windows.access0` — install Hadoop's Windows binaries and point
  `HADOOP_HOME` at the parent of `bin/`;
- if the hostname resolves to a link-local IPv6 address, the Python worker cannot call back to the
  driver (`Python worker failed to connect back`); `SPARK_LOCAL_IP=127.0.0.1` and an explicit
  `PYSPARK_PYTHON` fix it. `build_spark_session()` sets the driver host/bind address from
  `SPARK_LOCAL_IP` and `java.library.path` from `HADOOP_HOME` automatically.

## 2. Deploy to real AWS

Prerequisites: an AWS account, Terraform >= 1.6, and a verified SES identity (see §3).

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars   # edit region, name_prefix, ses_sender
terraform init
terraform plan  -out=tfplan                    # review: buckets, queues, IAM, alarms, schedule
terraform apply tfplan
```

Order matters only in one place: `terraform apply` creates the buckets, queues and SES identity that
the Lambdas read from their environment at cold start, so no manual bootstrap is required. If you
prefer to create state storage first, that is the only "bootstrap" step:

```bash
# optional: remote state (create the bucket/table out of band, then)
terraform init -backend-config=backend.hcl
```

IAM bootstrap: the identity running `terraform apply` needs permission to manage S3, SQS, Lambda,
IAM, CloudWatch, EventBridge, API Gateway, SES and (when `enable_emr_module = true`) EMR Serverless.
The four function roles created by this stack are already least-privilege; nothing needs to be
attached to your own principal beyond the ability to create those resources.

After apply, the outputs give you the function names, queue URLs, API endpoint and alarm topic.
Smoke-test the fan-out without waiting for the schedule:

```bash
aws lambda invoke --function-name "$(terraform output -raw chunker_function_name)" \
  --payload '{"shard":{"index":0,"of":4}}' /dev/stdout

aws lambda invoke --function-name "$(terraform output -raw orchestrator_function_name)" \
  --payload '{"report_date":"2026-09-20"}' /dev/stdout
```

Enable the paid scale path only when a day no longer fits the chunker guardrail
(`AGENT_REPORTS_CHUNKER_MAX_POLICIES`, default 2,000,000 policies per shard):

```bash
# terraform.tfvars
enable_emr_module = true
```

Then upload the job artifact and start a job run:

```bash
aws s3 cp src/agent_reports/emr/jobs/agent_report_job.py s3://<artifacts>/artifacts/
( cd src && zip -qr /tmp/agent_reports.zip agent_reports )
aws s3 cp /tmp/agent_reports.zip s3://<artifacts>/artifacts/

aws emr-serverless start-job-run \
  --application-id "$(terraform output -raw emr_application_id)" \
  --execution-role-arn "$(terraform -chdir=infra/terraform output -raw job_role_arn)" \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://<artifacts>/artifacts/agent_report_job.py",
      "sparkSubmitParameters": "--py-files s3://<artifacts>/artifacts/agent_reports.zip",
      "arguments": ["--raw-uri","s3://<raw>","--reports-uri","s3://<out>","--report-date","2026-09-20"]
    }
  }'
```

## 3. SES: the sandbox caveat

A new SES account is in the **sandbox**: you can only send *to* verified addresses, and the sending
quota is low. Two consequences:

1. `AGENT_REPORTS_SES_SENDER` must be a verified identity (`aws_ses_email_identity.sender` creates
   the request; clicking the link in the verification email finishes it).
2. Until production access is granted, real agent addresses must be verified too. Request production
   access in the SES console ("Request production access"); approval is usually a day or two and
   raises the quota to a documented per-second/per-day limit.

The offline test path verifies synthetic `@example.com` recipients for exactly this reason.

## 4. Rollback

Terraform is the source of truth; roll back in this order:

```bash
# 1. stop new work from starting
terraform apply -var 'report_schedule_cron=cron(30 2 * * ? *)'   # or disable the rule in the console
aws events disable-rule --name agent-reports-fanout-daily
aws events disable-rule --name agent-reports-aggregate-daily

# 2. drain or park the queue (messages survive 4 days)
aws sqs purge-queue --queue-url "$(terraform output -raw fanout_queue_url)"   # destructive

# 3. code rollback: re-apply the previous commit
git revert <bad-sha> && terraform apply

# 4. data rollback: reports are versioned and expire in 120 days; the state markers are the record
#    of what was delivered, so re-running the dispatcher for a date is safe (it suppresses duplicates)
aws s3api list-object-versions --bucket <reports-bucket> --prefix 'reports/dt=2026-09-20/'
```

Re-delivering a day is safe by design: `state/dispatch/dt=<date>/agent_id=<id>.json` is written
before the send and marked `sent` after it, and a fresh marker is created with a conditional write
(`IfNoneMatch: *`), so a replay cannot email anyone twice.

## 5. Failure playbooks

### 5a. DLQ storm

Symptom: the `agent-reports-dlq-not-empty` alarm fires; `ApproximateNumberOfMessagesVisible` on the
DLQ is growing.

```bash
DLQ=$(terraform output -raw fanout_dlq_url)
aws sqs get-queue-attributes --queue-url "$DLQ" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

# look at a sample: bodies are the dispatcher payloads, unchanged
aws sqs receive_message --queue-url "$DLQ" --max-number-of-messages 10 \
  --attribute-names All --message-attribute-names All
```

Diagnose by the `error_code` in the dispatcher log line for those messages
(`filter @message like "dispatch_failed"` in Logs Insights, or query the structured JSON field):

| `error_code` | Meaning | Fix |
| --- | --- | --- |
| `MissingReportError` | the report object was never written (aggregation failed or was late) | re-run the chunker/EMR job for that date, then redrive |
| `DependencyError` / `ThrottlingError` | SES or S3 throttled us through every retry | raise the SES quota, or redrive after the throttle clears |
| `PermanentError` with SES code `MessageRejected` | recipient or sender not verified | verify the identity, then redrive |

Redrive once the cause is fixed (messages keep their bodies):

```bash
# move everything from the DLQ back to the main queue
python - <<'PY'
import boto3, os
sqs = boto3.client("sqs")
dlq, main = os.environ["DLQ_URL"], os.environ["MAIN_URL"]
while True:
    batch = sqs.receive_message(QueueUrl=dlq, MaxNumberOfMessages=10).get("Messages", [])
    if not batch:
        break
    for message in batch:
        sqs.send_message(QueueUrl=main, MessageBody=message["Body"])
        sqs.delete_message(QueueUrl=dlq, ReceiptHandle=message["ReceiptHandle"])
PY
```

Deliveries already marked `sent` are suppressed on redrive, so this cannot double-send.

### 5b. SES throttling

Symptom: `DispatchFailures` and `BatchItemFailures` climb together; the dispatcher log shows
`error_code: DependencyError` (mapped from SES `Throttling`) after four in-invocation attempts.

1. Confirm it is throttling, not rejection:
   `aws ses get-send-quota` and check the SES reputation dashboard (`Sending`/`Bounce`/`Complaint`).
2. The dispatcher already backs off inside the invocation (exponential + full jitter, 4 attempts) and
   returns the message to SQS afterwards, so the queue *is* the backpressure — do not raise
   `maximum_concurrency` to "catch up", that makes it worse.
3. If the daily volume now exceeds the quota, request a quota increase, or shard the fan-out across
   hours by adding a second EventBridge rule with a different `report_schedule_cron`.
4. If messages exhaust `maxReceiveCount` (3) while throttled, they land in the DLQ; redrive with §5a
   once the quota is raised.

### 5c. Reports are late (report lag alarm)

Symptom: the `agent-reports-report-lag` alarm fires (`ReportAgeSeconds` > 6h) while emails still go
out. The fan-out started before aggregation finished.

- Check the chunker: `shard` invocations that failed leave `agents_skipped` in their result and no
  report objects; re-invoke the failing shard.
- Check the EMR path: `aws emr-serverless list-job-runs --application-id <id>`.
- Widen the gap between `aggregation_schedule_cron` and `report_schedule_cron` (default 1 hour apart)
  if the day genuinely takes longer than that.
- The orchestrator only targets agents that have a report object, so a late aggregation delays
  emails rather than sending broken links.
