module "emr_serverless" {
  # Lives at infra/emr (the EMR module named in the spec), referenced from the Terraform root.
  source = "../emr"
  count  = var.enable_emr_module ? 1 : 0

  name_prefix    = var.name_prefix
  environment    = var.environment
  raw_bucket     = aws_s3_bucket.raw.id
  reports_bucket = aws_s3_bucket.reports.id
  raw_prefix     = local.raw_prefix
  reports_prefix = local.reports_prefix

  # The job artifact lives next to the Lambda package so both are deployed from one build.
  job_artifact_bucket  = aws_s3_bucket.processed.id
  job_artifact_key     = "artifacts/agent_report_job.py"
  package_artifact_key = "artifacts/agent_reports.zip"

  release_label   = "emr-7.2.0"
  job_timeout_min = 60
}
