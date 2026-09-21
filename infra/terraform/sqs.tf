# Fan-out queue + dead-letter queue.
#
# Visibility timeout must be >= the dispatcher's timeout, otherwise SQS redelivers a message while
# the first invocation is still sending it (the idempotency lease would then have to absorb the
# duplicate - it does, but the visibility timeout is the cheap fix).

resource "aws_sqs_queue" "fanout_dlq" {
  name                       = "${var.name_prefix}-fanout-dlq"
  message_retention_seconds  = 1209600 # 14 days: the maximum, so a poison message survives a weekend
  visibility_timeout_seconds = 60
  sqs_managed_sse_enabled    = true

  tags = { Role = "dead-letter" }
}

resource "aws_sqs_queue" "fanout" {
  name                       = "${var.name_prefix}-fanout"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 345600 # 4 days
  receive_wait_time_seconds  = 20     # long polling: fewer empty receives
  sqs_managed_sse_enabled    = true

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.fanout_dlq.arn
    maxReceiveCount     = var.dlq_max_receive_count
  })

  tags = { Role = "fan-out" }
}

resource "aws_sqs_queue_redrive_allow_policy" "fanout_dlq" {
  queue_url = aws_sqs_queue.fanout_dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.fanout.arn]
  })
}

# The queue is only writable by the orchestrator role and only readable by the dispatcher role;
# an explicit queue policy keeps a compromised unrelated principal out.
data "aws_iam_policy_document" "fanout_queue" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions   = ["sqs:*"]
    resources = [aws_sqs_queue.fanout.arn]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_sqs_queue_policy" "fanout" {
  queue_url = aws_sqs_queue.fanout.id
  policy    = data.aws_iam_policy_document.fanout_queue.json
}
