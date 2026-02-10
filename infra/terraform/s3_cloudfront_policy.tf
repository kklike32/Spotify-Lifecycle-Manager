# -----------------------------------------------------------------------------
# S3 Dashboard Bucket Access: Private + CloudFront OAC-only Reads
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "dashboard_cloudfront_read" {
  count = var.cloudfront_enabled ? 1 : 0

  statement {
    sid    = "AllowCloudFrontServicePrincipalReadOnly"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.dashboard.arn}/*"
    ]

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.dashboard[0].arn]
    }
  }
}

resource "aws_s3_bucket_public_access_block" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "dashboard" {
  bucket = aws_s3_bucket.dashboard.id

  policy = var.cloudfront_enabled ? data.aws_iam_policy_document.dashboard_cloudfront_read[0].json : jsonencode({
    Version   = "2012-10-17"
    Statement = []
  })

  depends_on = [
    aws_cloudfront_distribution.dashboard,
    aws_s3_bucket_public_access_block.dashboard
  ]
}
