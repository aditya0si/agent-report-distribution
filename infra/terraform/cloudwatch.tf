# Log groups with managed retention, metric filters that read the EMF/JSON log lines the handlers
# emit, and alarms on the three failure modes that actually page someone:
#   1. messages stuck in the DLQ                (something is poisoning the fan-out)
#   2. dispatcher errors                        (SES rejected/throttled the sends)
#   3. report lag                               (aggregation finished too late to be useful)

resource "aws_cloudwatch_log_group" "chunker" {
  name              = "/aws/lambda/${var.name_prefix}-chunker"
  retention_in_days = var.log_retention_days

  tags = { Function = "chunker" }
}

resource "aws_cloudwatch_log_group" "orchestrator" {
  name              = "/aws/lambda/${var.name_prefix}-orchestrator"
  retention_in_days = var.log_retention_days

  tags = { Function = "orchestrator" }
}

resource "aws_cloudwatch_log_group" "dispatcher" {
  name              = "/aws/lambda/${var.name_prefix}-dispatcher"
  retention_in_days = var.log_retention_days

  tags = { Function = "dispatcher" }
}

resource "aws_cloudwatch_log_group" "presign" {
  name              = "/aws/lambda/${var.name_prefix}-presign"
  retention_in_days = var.log_retention_days

  tags = { Function = "presign" }
}

resource "aws_cloudwatch_log_group" "api_access" {
  name              = "/aws/apigateway/${var.name_prefix}-api"
  retention_in_days = var.log_retention_days

  tags = { Role = "api-access" }
}

resource "aws_cloudwatch_log_metric_filter" "dispatcher_failures" {
  name           = "${var.name_prefix}-dispatcher-failures"
  log_group_name = aws_cloudwatch_log_group.dispatcher.name
  pattern        = "{ $.event = \"dispatch_failed\" }"

  metric_transformation {
    name          = "DispatchFailures"
    namespace     = "AgentReports"
    value         = "1"
    default_value = "0"
    unit          = "Count"
    # The dimension set is part of the metric's identity: the alarm below reads
    # AgentReports/DispatchFailures{Service=dispatcher}, so the filter has to publish exactly that.
    # Without this block the filter published the metric with *no* dimensions, the alarm's datapoint
    # never existed, and treat_missing_data kept the alarm green forever.
    dimensions = {
      Service = "dispatcher"
    }
  }
}

resource "aws_cloudwatch_log_metric_filter" "batch_item_failures" {
  name           = "${var.name_prefix}-batch-item-failures"
  log_group_name = aws_cloudwatch_log_group.dispatcher.name
  pattern        = "{ $.event = \"dispatcher_batch_completed\" && $.batch_item_failures > 0 }"

  metric_transformation {
    name          = "BatchItemFailuresFromLogs"
    namespace     = "AgentReports"
    value         = "$.batch_item_failures"
    default_value = "0"
    unit          = "Count"
    dimensions = {
      Service = "dispatcher"
    }
  }
}

resource "aws_cloudwatch_log_metric_filter" "quarantined_messages" {
  name           = "${var.name_prefix}-quarantined-messages"
  log_group_name = aws_cloudwatch_log_group.dispatcher.name
  pattern        = "{ $.event = \"message_quarantined\" }"

  metric_transformation {
    name          = "QuarantinedMessages"
    namespace     = "AgentReports"
    value         = "1"
    default_value = "0"
    unit          = "Count"
    dimensions = {
      Service = "dispatcher"
    }
  }
}

resource "aws_cloudwatch_log_metric_filter" "emails_sent" {
  name           = "${var.name_prefix}-emails-sent"
  log_group_name = aws_cloudwatch_log_group.dispatcher.name
  pattern        = "{ $.event = \"dispatcher_batch_completed\" }"

  metric_transformation {
    name          = "EmailsSentFromLogs"
    namespace     = "AgentReports"
    value         = "$.sent"
    default_value = "0"
    unit          = "Count"
    dimensions = {
      Service = "dispatcher"
    }
  }
}

resource "aws_cloudwatch_log_metric_filter" "fanout_completed" {
  name           = "${var.name_prefix}-fanout-completed"
  log_group_name = aws_cloudwatch_log_group.orchestrator.name
  pattern        = "{ $.event = \"fanout_completed\" }"

  metric_transformation {
    name          = "MessagesEnqueuedFromLogs"
    namespace     = "AgentReports"
    value         = "$.messages_enqueued"
    default_value = "0"
    unit          = "Count"
    dimensions = {
      Service = "orchestrator"
    }
  }
}

resource "aws_sns_topic" "alarms" {
  name = "${var.name_prefix}-alarms"

  tags = { Role = "alarms" }
}

resource "aws_sns_topic_subscription" "alarm_email" {
  count = var.alarm_email == "" ? 0 : 1

  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${var.name_prefix}-dlq-not-empty"
  alarm_description   = "Messages are piling up in the fan-out dead-letter queue."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.fanout_dlq.name
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "dispatcher_errors" {
  alarm_name          = "${var.name_prefix}-dispatcher-errors"
  alarm_description   = "The dispatcher is failing to deliver reports (SES rejections/throttling)."
  namespace           = "AgentReports"
  metric_name         = "DispatchFailures"
  statistic           = "Sum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 10
  period              = 300
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    Service = "dispatcher"
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "report_lag" {
  alarm_name          = "${var.name_prefix}-report-lag"
  alarm_description   = "Reports are older than expected when they are dispatched - aggregation is late."
  namespace           = "AgentReports"
  metric_name         = "ReportAgeSeconds"
  statistic           = "Maximum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 21600 # 6 hours
  period              = 3600
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    Service = "dispatcher"
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "emails_not_sent" {
  alarm_name        = "${var.name_prefix}-no-emails-sent"
  alarm_description = "A fan-out enqueued messages and nothing was delivered all day: SES or the reports zone is broken."

  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  evaluation_periods  = 1
  treat_missing_data  = "notBreaching"

  # "A fan-out ran but nothing was delivered" is a statement about *two* metrics, so it has to be a
  # metric-math alarm. The previous version alarmed on MessagesEnqueued > 0, which is true on every
  # successful day - it would have paged on success. It also referenced {Service=orchestrator}, a
  # dimension set the handlers did not publish, so it could never have fired at all.
  metric_query {
    id          = "enqueued"
    return_data = false

    metric {
      namespace   = "AgentReports"
      metric_name = "MessagesEnqueued"
      stat        = "Sum"
      period      = 86400
      dimensions  = { Service = "orchestrator" }
    }
  }

  metric_query {
    id          = "delivered"
    return_data = false

    metric {
      namespace   = "AgentReports"
      metric_name = "EmailsSent"
      stat        = "Sum"
      period      = 86400
      dimensions  = { Service = "dispatcher" }
    }
  }

  metric_query {
    id          = "nothing_delivered"
    expression  = "IF(AND(enqueued > 0, delivered == 0), 1, 0)"
    label       = "fan-out ran, nothing delivered"
    return_data = true
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
}

# A dashboard is cheap and makes the "is it healthy?" question answerable at a glance.
resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = "${var.name_prefix}-pipeline"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Fan-out"
          region = var.region
          metrics = [
            ["AgentReports", "AgentsDiscovered", "Service", "orchestrator"],
            ["AgentReports", "MessagesEnqueued", "Service", "orchestrator"],
          ]
          stat   = "Sum"
          period = 86400
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "Delivery"
          region = var.region
          metrics = [
            ["AgentReports", "EmailsSent", "Service", "dispatcher"],
            ["AgentReports", "EmailsFailed", "Service", "dispatcher"],
            ["AgentReports", "DuplicatesSuppressed", "Service", "dispatcher"],
          ]
          stat   = "Sum"
          period = 86400
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Dead-letter queue depth"
          region = var.region
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.fanout_dlq.name],
          ]
          stat   = "Maximum"
          period = 300
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Aggregation"
          region = var.region
          metrics = [
            ["AgentReports", "ReportsWritten", "Service", "chunker"],
            ["AgentReports", "RowsIn", "Service", "chunker"],
          ]
          stat   = "Sum"
          period = 86400
        }
      },
    ]
  })
}
