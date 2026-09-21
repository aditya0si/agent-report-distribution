locals {
  common_tags = {
    Project     = "agent-report-distribution"
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  raw_bucket       = "${var.name_prefix}-raw"
  processed_bucket = "${var.name_prefix}-processed"
  reports_bucket   = "${var.name_prefix}-out"

  # Same layout as agent_reports.common.keys: raw/dt=..., reports/dt=..., state/...
  raw_prefix        = "raw/"
  reports_prefix    = "reports/"
  state_prefix      = "state/"
  quarantine_prefix = "state/quarantine/"

  lambda_source_dir = "${path.module}/../../src"
  lambda_runtime    = "python3.11"

  environment_variables = {
    AGENT_REPORTS_REGION                = var.region
    AGENT_REPORTS_RAW_BUCKET            = local.raw_bucket
    AGENT_REPORTS_PROCESSED_BUCKET      = local.processed_bucket
    AGENT_REPORTS_REPORTS_BUCKET        = local.reports_bucket
    AGENT_REPORTS_AGENT_QUEUE_URL       = aws_sqs_queue.fanout.url
    AGENT_REPORTS_DLQ_URL               = aws_sqs_queue.fanout_dlq.url
    AGENT_REPORTS_SES_SENDER            = var.ses_sender
    AGENT_REPORTS_SES_CONFIGURATION_SET = aws_ses_configuration_set.reports.name
    AGENT_REPORTS_PRESIGN_TTL_SECONDS   = tostring(var.presign_ttl_seconds)
    AGENT_REPORTS_SQS_BATCH_SIZE        = tostring(var.sqs_batch_size)
    AGENT_REPORTS_CHUNKER_SHARDS        = tostring(var.chunker_shards)
    AGENT_REPORTS_CHUNKER_MAX_POLICIES  = tostring(var.chunker_max_policies)
    AGENT_REPORTS_LOG_LEVEL             = "INFO"
  }
}

data "archive_file" "lambda_package" {
  type        = "zip"
  source_dir  = local.lambda_source_dir
  output_path = "${path.module}/build/agent_reports.zip"
  excludes    = ["**/__pycache__/**", "**/*.pyc"]
}
