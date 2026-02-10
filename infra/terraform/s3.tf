# -----------------------------------------------------------------------------
# S3 Bucket: Raw Events (Cold Storage)
# -----------------------------------------------------------------------------

resource "aws_s3_bucket" "raw_events" {
  bucket = var.raw_bucket_name

  tags = {
    Name        = "${var.project_name}-raw-events"
    Description = "Cold storage for partitioned play events in JSONL format"
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
    id     = "transition-and-expire"
    status = "Enabled"

    filter {}

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER_IR"
    }

    expiration {
      days = 730 # Delete after 2 years (hard cap on storage growth)
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

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
  bucket = var.dashboard_bucket_name

  tags = {
    Name        = "${var.project_name}-dashboard"
    Description = "Static dashboard website with HTML CSS JS"
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

resource "aws_s3_bucket_cors_configuration" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    expose_headers  = ["ETag"]
    max_age_seconds = 3600
  }
}

# -----------------------------------------------------------------------------
# Data Sources
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}
