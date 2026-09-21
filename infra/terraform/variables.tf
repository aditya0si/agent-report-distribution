variable "region" {
  description = "AWS region for every resource in this stack."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix applied to resource names (also used for the S3 bucket names)."
  type        = string
  default     = "agent-reports"

  validation {
    condition     = can(regex("^[a-z0-9-]{3,30}$", var.name_prefix))
    error_message = "name_prefix must be 3-30 characters of lowercase letters, digits or hyphens."
  }
}

variable "environment" {
  description = "Deployment environment (dev/stage/prod) - used for tags and alarm thresholds."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "stage", "prod"], var.environment)
    error_message = "environment must be one of dev, stage, prod."
  }
}

variable "ses_sender" {
  description = "Verified SES sender identity the dispatcher sends from."
  type        = string
  default     = "reports@example.com"
}

variable "presign_ttl_seconds" {
  description = "Lifetime of the pre-signed report links handed to agents (S3 SigV4 max is 604800)."
  type        = number
  default     = 3600

  validation {
    condition     = var.presign_ttl_seconds >= 60 && var.presign_ttl_seconds <= 604800
    error_message = "presign_ttl_seconds must be between 60 and 604800 seconds."
  }
}

variable "sqs_batch_size" {
  description = "Messages the orchestrator packs into one SendMessageBatch call (SQS max is 10)."
  type        = number
  default     = 10

  validation {
    condition     = var.sqs_batch_size >= 1 && var.sqs_batch_size <= 10
    error_message = "sqs_batch_size must be between 1 and 10."
  }
}

variable "presign_jwt_issuer" {
  description = <<-EOT
    OIDC issuer URL for the GET /reports JWT authorizer (e.g. https://cognito-idp.us-east-1.amazonaws.com/<pool-id>).
    Empty (the default) leaves the route unauthenticated at the gateway; the presign function then
    denies every request, because its header identity fallback is off unless
    AGENT_REPORTS_ALLOW_CALLER_HEADER_FALLBACK is set explicitly. See docs/RUNBOOK.md section 6.
  EOT
  type        = string
  default     = ""
}

variable "presign_jwt_audience" {
  description = "Audience (client id) the JWT authorizer accepts. Only used when presign_jwt_issuer is set."
  type        = list(string)
  default     = []
}

variable "dlq_max_receive_count" {
  description = "Receives before a message is moved to the dead-letter queue."
  type        = number
  default     = 3
}

variable "chunker_shards" {
  description = "Parallel chunker invocations for the free-tier aggregation path."
  type        = number
  default     = 4
}

variable "chunker_max_policies" {
  description = "Per-shard policy guardrail before the day must be routed to EMR instead."
  type        = number
  default     = 2000000
}

variable "enable_emr_module" {
  description = "Create the optional EMR Serverless application + job role (the paid scale path)."
  type        = bool
  default     = false
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for every log group in the stack."
  type        = number
  default     = 30
}

variable "alarm_email" {
  description = "Optional email subscribed to the alarm topic. Empty disables the subscription."
  type        = string
  default     = ""
}

variable "report_schedule_cron" {
  description = "EventBridge cron for the daily fan-out (default 02:30 UTC every day)."
  type        = string
  default     = "cron(30 2 * * ? *)"
}

variable "aggregation_schedule_cron" {
  description = "EventBridge cron for the aggregation step, ahead of the fan-out (01:30 UTC daily)."
  type        = string
  default     = "cron(30 1 * * ? *)"
}
