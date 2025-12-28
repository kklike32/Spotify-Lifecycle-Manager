# -----------------------------------------------------------------------------
# S3 Bucket: Raw Events (Cold Storage)
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "raw_events" {
  bucket = "${var.raw_bucket_name}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "${var.project_name}-raw-events"
    Description = "Cold storage for partitioned play events (JSONL)"
  }
}

resource "aws_s3_bucket_versioning" "raw_events" {
  bucket = aws_s3_bucket.raw_events.id

  versioning_configuration {
    status = "Disabled" # Cost optimization: no versioning for append-only data
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_events" {
  bucket = aws_s3_bucket.raw_events.id

  rule {
    id     = "transition-to-infrequent-access"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER_INSTANT_RETRIEVAL"
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw_events" {
  bucket = aws_s3_bucket.raw_events.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# -----------------------------------------------------------------------------
# S3 Bucket: Dashboard (Static Website)
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "dashboard" {
  bucket = "${var.dashboard_bucket_name}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name        = "${var.project_name}-dashboard"
    Description = "Static dashboard website (HTML/CSS/JS)"
  }
}

resource "aws_s3_bucket_versioning" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  versioning_configuration {
    status = "Disabled"
  }
}

resource "aws_s3_bucket_website_configuration" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  index_document {
    suffix = "index.html"
  }

  error_document {
    key = "index.html"
  }
}

resource "aws_s3_bucket_public_access_block" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_policy" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "PublicReadGetObject"
        Effect    = "Allow"
        Principal = "*"
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.dashboard.arn}/*"
      }
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.dashboard]
}

# -----------------------------------------------------------------------------
# Data Sources
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}
