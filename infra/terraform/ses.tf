# SES identity + configuration set.
#
# Sandbox caveat: a new account can only send to verified addresses until production access is
# granted (see docs/RUNBOOK.md). The configuration set exists so the dispatcher can tag messages and
# so reputation metrics / event destinations can be attached without touching code.

resource "aws_ses_email_identity" "sender" {
  email = var.ses_sender
}

resource "aws_ses_configuration_set" "reports" {
  name = "${var.name_prefix}-reports"

  reputation_metrics_enabled = true
  sending_enabled            = true

  delivery_options {
    tls_policy = "Require"
  }
}

resource "aws_ses_event_destination" "reports_cloudwatch" {
  name                   = "${var.name_prefix}-events"
  configuration_set_name = aws_ses_configuration_set.reports.name
  enabled                = true
  matching_types         = ["bounce", "complaint", "reject", "delivery"]

  cloudwatch_destination {
    default_value  = "unknown"
    dimension_name = "ses:configuration-set"
    value_source   = "messageTag"
  }
}
