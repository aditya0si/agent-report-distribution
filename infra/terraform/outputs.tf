output "raw_bucket" {
  description = "Bucket holding the day's raw partitions."
  value       = aws_s3_bucket.raw.id
}

output "processed_bucket" {
  description = "Bucket holding run manifests, idempotency markers and quarantined messages."
  value       = aws_s3_bucket.processed.id
}

output "reports_bucket" {
  description = "Bucket holding the per-agent CSV reports agents download."
  value       = aws_s3_bucket.reports.id
}

output "fanout_queue_url" {
  description = "SQS queue the orchestrator writes to and the dispatcher consumes."
  value       = aws_sqs_queue.fanout.url
}

output "fanout_dlq_url" {
  description = "Dead-letter queue URL."
  value       = aws_sqs_queue.fanout_dlq.url
}

output "chunker_function_name" {
  description = "Free-tier aggregation function name (invoke with {\"shard\": {\"index\": i, \"of\": n}})."
  value       = aws_lambda_function.chunker.function_name
}

output "orchestrator_function_name" {
  description = "Fan-out function name."
  value       = aws_lambda_function.orchestrator.function_name
}

output "dispatcher_function_name" {
  description = "Delivery function name."
  value       = aws_lambda_function.dispatcher.function_name
}

output "presign_function_name" {
  description = "Report-link function name."
  value       = aws_lambda_function.presign.function_name
}

output "presign_api_endpoint" {
  description = "HTTP API endpoint for GET /reports?agent_id=...&date=..."
  value       = aws_apigatewayv2_stage.reports.invoke_url
}

output "alarm_topic_arn" {
  description = "SNS topic the CloudWatch alarms publish to."
  value       = aws_sns_topic.alarms.arn
}

output "lambda_environment" {
  description = "Environment variables the Lambda functions run with (mirrors .env.example)."
  value       = local.environment_variables
  sensitive   = false
}

output "emr_application_id" {
  description = "EMR Serverless application id when the optional module is enabled."
  value       = var.enable_emr_module ? module.emr_serverless[0].application_id : null
}
