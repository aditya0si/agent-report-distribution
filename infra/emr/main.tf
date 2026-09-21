# Optional module: the paid scale path.
#
# EMR Serverless is the cheapest way to run the PySpark job without managing a cluster: the
# application is created with a *pre-initialised* worker (so the first job of the day does not pay a
# 60-second cold start), capped by maximum_capacity, and torn down automatically when idle.
#
# Billing note: EMR Serverless bills per vCPU-hour and GB-hour of *worker* usage with a 1-minute
# minimum per job run, plus a per-job-run surcharge in some regions - see docs/COST.md.

variable "name_prefix" {
  type = string
}

variable "environment" {
  type = string
}

variable "raw_bucket" {
  type = string
}

variable "reports_bucket" {
  type = string
}

variable "raw_prefix" {
  type    = string
  default = "raw/"
}

variable "reports_prefix" {
  type    = string
  default = "reports/"
}

variable "job_artifact_bucket" {
  description = "Bucket holding agent_report_job.py and agent_reports.zip."
  type        = string
}

variable "job_artifact_key" {
  type    = string
  default = "artifacts/agent_report_job.py"
}

variable "package_artifact_key" {
  type    = string
  default = "artifacts/agent_reports.zip"
}

variable "release_label" {
  description = "EMR Serverless release label (Spark version follows from it)."
  type        = string
  default     = "emr-7.2.0"
}

variable "job_timeout_min" {
  type    = number
  default = 60
}

locals {
  application_name = "${var.name_prefix}-spark"
}

resource "aws_emrserverless_application" "spark" {
  name          = local.application_name
  release_label = var.release_label
  type          = "SPARK"

  # A single pre-initialised driver avoids the cold start on the daily run; keep it small.
  initial_capacity {
    initial_capacity_type = "Driver"

    initial_capacity_config {
      worker_count = 1

      worker_configuration {
        cpu    = "2 vCPU"
        memory = "8 GB"
      }
    }
  }

  # Ceiling for the day's shuffle: 4 workers x 4 vCPU. Raise deliberately, not by accident.
  maximum_capacity {
    cpu    = "16 vCPU"
    memory = "64 GB"
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  tags = {
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

data "aws_iam_policy_document" "job_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "job_permissions" {
  statement {
    sid    = "ReadRawAndArtifacts"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = [
      "arn:aws:s3:::${var.raw_bucket}/${var.raw_prefix}*",
      "arn:aws:s3:::${var.job_artifact_bucket}/*",
    ]
  }

  statement {
    sid       = "ListRaw"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.raw_bucket}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.raw_prefix}*"]
    }
  }

  statement {
    sid    = "WriteReports"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = ["arn:aws:s3:::${var.reports_bucket}/${var.reports_prefix}*"]
  }

  statement {
    sid       = "ListReports"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.reports_bucket}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.reports_prefix}*"]
    }
  }

  statement {
    sid    = "JobLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["arn:aws:logs:*:*:/aws/emr-serverless*"]
  }
}

resource "aws_iam_role" "job" {
  name               = "${var.name_prefix}-emr-serverless-job"
  assume_role_policy = data.aws_iam_policy_document.job_assume_role.json
}

resource "aws_iam_policy" "job" {
  name   = "${var.name_prefix}-emr-serverless-job"
  policy = data.aws_iam_policy_document.job_permissions.json
}

resource "aws_iam_role_policy_attachment" "job" {
  role       = aws_iam_role.job.name
  policy_arn = aws_iam_policy.job.arn
}

output "application_id" {
  description = "EMR Serverless application id (pass to start-job-run)."
  value       = aws_emrserverless_application.spark.id
}

output "job_role_arn" {
  description = "Execution role the Spark job runs as."
  value       = aws_iam_role.job.arn
}

output "entry_point" {
  description = "s3 URI of the PySpark job."
  value       = "s3://${var.job_artifact_bucket}/${var.job_artifact_key}"
}

output "py_files" {
  description = "s3 URI of the packaged agent_reports module shipped with --py-files."
  value       = "s3://${var.job_artifact_bucket}/${var.package_artifact_key}"
}
