data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition

  log_group_arns = [
    for name in [
      aws_cloudwatch_log_group.chunker.name,
      aws_cloudwatch_log_group.orchestrator.name,
      aws_cloudwatch_log_group.dispatcher.name,
      aws_cloudwatch_log_group.presign.name,
    ] : "arn:${local.partition}:logs:${var.region}:${local.account_id}:log-group:${name}:*"
  ]
}

# ---------------------------------------------------------------------------- chunker
data "aws_iam_policy_document" "chunker" {
  statement {
    sid    = "ReadRawZone"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = ["${aws_s3_bucket.raw.arn}/${local.raw_prefix}*"]
  }

  statement {
    sid       = "ListRawZone"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.raw_prefix}*"]
    }
  }

  statement {
    sid       = "WriteReports"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.reports.arn}/${local.reports_prefix}*"]
  }

  statement {
    sid       = "PublishMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["AgentReports"]
    }
  }

  statement {
    sid       = "WriteLogs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = local.log_group_arns
  }
}

resource "aws_iam_policy" "chunker" {
  name        = "${var.name_prefix}-chunker"
  description = "Free-tier aggregation: read raw, write reports."
  policy      = data.aws_iam_policy_document.chunker.json
}

resource "aws_iam_role" "chunker" {
  name = "${var.name_prefix}-chunker"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "chunker" {
  role       = aws_iam_role.chunker.name
  policy_arn = aws_iam_policy.chunker.arn
}

# ----------------------------------------------------------------------- orchestrator
data "aws_iam_policy_document" "orchestrator" {
  statement {
    sid       = "ReadRoster"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.raw.arn}/${local.raw_prefix}*"]
  }

  statement {
    sid       = "ListRawAndReports"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.raw.arn, aws_s3_bucket.reports.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.raw_prefix}*", "${local.reports_prefix}*"]
    }
  }

  statement {
    sid       = "WriteRunManifest"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.processed.arn}/${local.state_prefix}*"]
  }

  statement {
    sid       = "FanOut"
    effect    = "Allow"
    actions   = ["sqs:SendMessage", "sqs:SendMessageBatch", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.fanout.arn]
  }

  statement {
    sid       = "PublishMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["AgentReports"]
    }
  }

  statement {
    sid       = "WriteLogs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = local.log_group_arns
  }
}

resource "aws_iam_policy" "orchestrator" {
  name        = "${var.name_prefix}-orchestrator"
  description = "Fan-out: read the roster, enqueue one message per agent, write the manifest."
  policy      = data.aws_iam_policy_document.orchestrator.json
}

resource "aws_iam_role" "orchestrator" {
  name = "${var.name_prefix}-orchestrator"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "orchestrator" {
  role       = aws_iam_role.orchestrator.name
  policy_arn = aws_iam_policy.orchestrator.arn
}

# -------------------------------------------------------------------------- dispatcher
data "aws_iam_policy_document" "dispatcher" {
  statement {
    sid    = "ReadReportsForPresignAndSummary"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = ["${aws_s3_bucket.reports.arn}/${local.reports_prefix}*"]
  }

  statement {
    sid       = "IdempotencyAndQuarantine"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.processed.arn}/${local.state_prefix}*"]
  }

  statement {
    sid       = "ListStatePrefix"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.processed.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.state_prefix}*"]
    }
  }

  statement {
    sid       = "SendReports"
    effect    = "Allow"
    actions   = ["ses:SendEmail", "ses:SendRawEmail"]
    resources = [aws_ses_configuration_set.reports.arn]

    condition {
      test     = "StringEquals"
      variable = "ses:FromAddress"
      values   = [var.ses_sender]
    }
  }

  statement {
    sid       = "ConsumeFanout"
    effect    = "Allow"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.fanout.arn]
  }

  statement {
    sid       = "PublishMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["AgentReports"]
    }
  }

  statement {
    sid       = "WriteLogs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = local.log_group_arns
  }
}

resource "aws_iam_policy" "dispatcher" {
  name        = "${var.name_prefix}-dispatcher"
  description = "Deliver reports: pre-sign, send through SES, record idempotency markers."
  policy      = data.aws_iam_policy_document.dispatcher.json
}

resource "aws_iam_role" "dispatcher" {
  name = "${var.name_prefix}-dispatcher"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "dispatcher" {
  role       = aws_iam_role.dispatcher.name
  policy_arn = aws_iam_policy.dispatcher.arn
}

# ----------------------------------------------------------------------------- presign
data "aws_iam_policy_document" "presign" {
  statement {
    sid    = "ReadOwnReport"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = ["${aws_s3_bucket.reports.arn}/${local.reports_prefix}*"]
  }

  statement {
    sid       = "PublishMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["AgentReports"]
    }
  }

  statement {
    sid       = "WriteLogs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = local.log_group_arns
  }
}

resource "aws_iam_policy" "presign" {
  name        = "${var.name_prefix}-presign"
  description = "Mint short-lived report links for authorised callers."
  policy      = data.aws_iam_policy_document.presign.json
}

resource "aws_iam_role" "presign" {
  name = "${var.name_prefix}-presign"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "presign" {
  role       = aws_iam_role.presign.name
  policy_arn = aws_iam_policy.presign.arn
}
