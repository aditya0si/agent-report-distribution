# Four Lambda functions, each with its own least-privilege role and its own log group (created here
# so retention is managed instead of defaulting to "never expire").

resource "aws_lambda_function" "chunker" {
  function_name = "${var.name_prefix}-chunker"
  role          = aws_iam_role.chunker.arn
  handler       = "agent_reports.lambda_handlers.chunker.handler"
  runtime       = local.lambda_runtime
  timeout       = 900
  memory_size   = 3008

  filename         = data.archive_file.lambda_package.output_path
  source_code_hash = data.archive_file.lambda_package.output_base64sha256

  environment {
    variables = local.environment_variables
  }

  tracing_config {
    mode = "Active"
  }

  depends_on = [aws_cloudwatch_log_group.chunker]
}

resource "aws_lambda_function" "orchestrator" {
  function_name = "${var.name_prefix}-orchestrator"
  role          = aws_iam_role.orchestrator.arn
  handler       = "agent_reports.lambda_handlers.orchestrator.handler"
  runtime       = local.lambda_runtime
  timeout       = 300
  memory_size   = 1024

  filename         = data.archive_file.lambda_package.output_path
  source_code_hash = data.archive_file.lambda_package.output_base64sha256

  environment {
    variables = local.environment_variables
  }

  tracing_config {
    mode = "Active"
  }

  depends_on = [aws_cloudwatch_log_group.orchestrator]
}

resource "aws_lambda_function" "dispatcher" {
  function_name = "${var.name_prefix}-dispatcher"
  role          = aws_iam_role.dispatcher.arn
  handler       = "agent_reports.lambda_handlers.dispatcher.handler"
  runtime       = local.lambda_runtime
  timeout       = 120
  memory_size   = 512

  filename         = data.archive_file.lambda_package.output_path
  source_code_hash = data.archive_file.lambda_package.output_base64sha256

  environment {
    variables = local.environment_variables
  }

  tracing_config {
    mode = "Active"
  }

  depends_on = [aws_cloudwatch_log_group.dispatcher]
}

resource "aws_lambda_function" "presign" {
  function_name = "${var.name_prefix}-presign"
  role          = aws_iam_role.presign.arn
  handler       = "agent_reports.lambda_handlers.presign.handler"
  runtime       = local.lambda_runtime
  timeout       = 30
  memory_size   = 256

  filename         = data.archive_file.lambda_package.output_path
  source_code_hash = data.archive_file.lambda_package.output_base64sha256

  environment {
    variables = local.environment_variables
  }

  tracing_config {
    mode = "Active"
  }

  depends_on = [aws_cloudwatch_log_group.presign]
}

# Partial batch responses: the dispatcher returns batchItemFailures, so SQS redelivers only the
# messages that failed instead of the whole batch (and finally moves them to the DLQ).
resource "aws_lambda_event_source_mapping" "dispatcher" {
  event_source_arn                   = aws_sqs_queue.fanout.arn
  function_name                      = aws_lambda_function.dispatcher.arn
  batch_size                         = var.sqs_batch_size
  maximum_batching_window_in_seconds = 5
  function_response_types            = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = 20
  }
}

# The presign function sits behind an HTTP API. There are two gates and the README/RUNBOOK say
# exactly which one is active:
#   1. the gateway: a JWT authorizer, active as soon as `presign_jwt_issuer` is set (Cognito, Auth0,
#      ... any OIDC issuer). With the default empty issuer the route is *unauthenticated at the
#      gateway* and the function is the only gate;
#   2. the function: deny-by-default against the claims the authorizer injects. The
#      X-Caller-Agent-Id / X-Caller-Role header fallback is off unless the deployer explicitly sets
#      AGENT_REPORTS_ALLOW_CALLER_HEADER_FALLBACK - this stack never sets it, so an unauthenticated
#      request (or a spoofed header) is a 401 rather than another agent's report.
resource "aws_apigatewayv2_api" "reports" {
  name          = "${var.name_prefix}-api"
  protocol_type = "HTTP"

  tags = { Role = "api" }
}

locals {
  presign_jwt_enabled = var.presign_jwt_issuer != ""
}

resource "aws_apigatewayv2_authorizer" "presign_jwt" {
  count = local.presign_jwt_enabled ? 1 : 0

  api_id           = aws_apigatewayv2_api.reports.id
  name             = "${var.name_prefix}-presign-jwt"
  authorizer_type  = "JWT"
  identity_sources = ["$request.header.Authorization"]

  jwt_configuration {
    issuer   = var.presign_jwt_issuer
    audience = var.presign_jwt_audience
  }
}

resource "aws_apigatewayv2_stage" "reports" {
  api_id      = aws_apigatewayv2_api.reports.id
  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_access.arn

    format = jsonencode({
      requestId      = "$context.requestId"
      routeKey       = "$context.routeKey"
      status         = "$context.status"
      responseLength = "$context.responseLength"
      latency        = "$context.responseLatency"
    })
  }
}

resource "aws_apigatewayv2_integration" "presign" {
  api_id                 = aws_apigatewayv2_api.reports.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.presign.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "get_report" {
  api_id    = aws_apigatewayv2_api.reports.id
  route_key = "GET /reports"
  target    = "integrations/${aws_apigatewayv2_integration.presign.id}"

  authorization_type = local.presign_jwt_enabled ? "JWT" : "NONE"
  authorizer_id      = local.presign_jwt_enabled ? aws_apigatewayv2_authorizer.presign_jwt[0].id : null
}

resource "aws_lambda_permission" "presign_api" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.presign.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.reports.execution_arn}/*/*"
}
