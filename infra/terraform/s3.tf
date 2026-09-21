# Three zones, one lifecycle story each:
#   raw       - landed daily, replayed for 30 days, then cheap storage, then gone
#   processed - run manifests + idempotency markers: small, kept for a year (audit trail)
#   reports   - per-agent CSVs: the objects agents download; short-lived by design
#
# Tiering is only applied where the objects are larger than the 128 KB minimum that the
# infrequent-access classes bill: the raw part files qualify (~500 KB each), the per-agent reports
# (~1.4 KB) and the dispatch markers (~400 B) do not - tiering those *increases* the bill.

resource "aws_s3_bucket" "raw" {
  bucket        = local.raw_bucket
  force_destroy = var.environment != "prod"

  tags = { Zone = "raw" }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "raw-tiering-and-expiry"
    status = "Enabled"

    filter {
      prefix = local.raw_prefix
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }

    expiration {
      days = 400
    }

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = 60
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket" "processed" {
  bucket        = local.processed_bucket
  force_destroy = var.environment != "prod"

  tags = { Zone = "processed" }
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id

  rule {
    id     = "state-retention"
    status = "Enabled"

    filter {
      prefix = local.state_prefix
    }

    # No STANDARD_IA transition here on purpose: a dispatch marker is a few hundred bytes and S3
    # bills a 128 KB minimum per object in the infrequent-access classes, so tiering 4,000 markers a
    # day *increases* the bill (see docs/COST.md). Expiry is the only lever that helps.
    expiration {
      days = 365
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "quarantine-short-retention"
    status = "Enabled"

    filter {
      prefix = local.quarantine_prefix
    }

    expiration {
      days = 90
    }
  }
}

resource "aws_s3_bucket" "reports" {
  bucket        = local.reports_bucket
  force_destroy = var.environment != "prod"

  tags = { Zone = "reports" }
}

resource "aws_s3_bucket_versioning" "reports" {
  bucket = aws_s3_bucket.reports.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "reports" {
  bucket = aws_s3_bucket.reports.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "reports" {
  bucket                  = aws_s3_bucket.reports.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "reports" {
  bucket = aws_s3_bucket.reports.id

  rule {
    id     = "report-expiry"
    status = "Enabled"

    filter {
      prefix = local.reports_prefix
    }

    # No STANDARD_IA transition on purpose. A per-agent report is ~1.4 KB, and the infrequent-access
    # classes bill a 128 KB minimum per object: tiering 4,000 reports a day would bill 61 GB-month
    # instead of 0.7 GB-month (and add a transition request per object) to save nothing, because the
    # objects expire 120 days later anyway. See docs/COST.md for the arithmetic.
    expiration {
      days = 120
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

# Pre-signed links are handed to agents, so the bucket must not be public and must not be logged
# into CloudTrail data events by default (data events cost money; see docs/COST.md).
resource "aws_s3_bucket_ownership_controls" "reports" {
  bucket = aws_s3_bucket.reports.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}
