# Daily schedule, in two steps: aggregate first, then fan out. Both are EventBridge rules so a
# missed run can be replayed by hand (see docs/RUNBOOK.md) without touching the schedule.

resource "aws_cloudwatch_event_rule" "aggregate" {
  name                = "${var.name_prefix}-aggregate-daily"
  description         = "Daily free-tier aggregation (chunker shards) ahead of the fan-out."
  schedule_expression = var.aggregation_schedule_cron
}

resource "aws_cloudwatch_event_rule" "fanout" {
  name                = "${var.name_prefix}-fanout-daily"
  description         = "Daily agent-report fan-out."
  schedule_expression = var.report_schedule_cron
}

# One target per shard: each invocation aggregates a disjoint slice of the roster, which is what
# keeps a Lambda inside its memory limit (see agent_reports.common.roster.plan_shards).
resource "aws_cloudwatch_event_target" "chunker_shard" {
  count = var.chunker_shards

  rule      = aws_cloudwatch_event_rule.aggregate.name
  target_id = "${var.name_prefix}-chunker-shard-${count.index}"
  arn       = aws_lambda_function.chunker.arn

  input = jsonencode({
    shard = {
      index = count.index
      of    = var.chunker_shards
    }
  })
}

resource "aws_lambda_permission" "allow_eventbridge_chunker" {
  statement_id  = "AllowEventBridgeAggregate"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.chunker.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.aggregate.arn
}

resource "aws_cloudwatch_event_target" "orchestrator" {
  rule      = aws_cloudwatch_event_rule.fanout.name
  target_id = "${var.name_prefix}-orchestrator"
  arn       = aws_lambda_function.orchestrator.arn
}

resource "aws_lambda_permission" "allow_eventbridge_orchestrator" {
  statement_id  = "AllowEventBridgeFanout"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orchestrator.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.fanout.arn
}
