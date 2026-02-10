# -----------------------------------------------------------------------------
# CloudFront: Private S3 Origin via OAC
# -----------------------------------------------------------------------------

locals {
  dashboard_cloudfront_origin_id = "s3-dashboard-origin"
}

resource "aws_cloudfront_origin_access_control" "dashboard" {
  count = var.cloudfront_enabled ? 1 : 0

  name                              = "${var.project_name}-dashboard-oac"
  description                       = "OAC for private dashboard S3 origin"
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

resource "aws_cloudfront_origin_request_policy" "dashboard_minimal" {
  count = var.cloudfront_enabled ? 1 : 0

  name    = "${var.project_name}-dashboard-minimal-origin-request"
  comment = "Do not forward cookies/query strings/headers by default"

  cookies_config {
    cookie_behavior = "none"
  }

  headers_config {
    header_behavior = "none"
  }

  query_strings_config {
    query_string_behavior = "none"
  }
}

resource "aws_cloudfront_cache_policy" "dashboard_html" {
  count = var.cloudfront_enabled ? 1 : 0

  name        = "${var.project_name}-dashboard-html"
  comment     = "Short cache for HTML"
  default_ttl = 60
  max_ttl     = 300
  min_ttl     = 0

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }

    headers_config {
      header_behavior = "none"
    }

    query_strings_config {
      query_string_behavior = "none"
    }

    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
  }
}

resource "aws_cloudfront_cache_policy" "dashboard_json" {
  count = var.cloudfront_enabled ? 1 : 0

  name        = "${var.project_name}-dashboard-json"
  comment     = "Short cache for JSON"
  default_ttl = 60
  max_ttl     = 300
  min_ttl     = 0

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }

    headers_config {
      header_behavior = "none"
    }

    query_strings_config {
      query_string_behavior = "none"
    }

    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
  }
}

resource "aws_cloudfront_cache_policy" "dashboard_css_js" {
  count = var.cloudfront_enabled ? 1 : 0

  name        = "${var.project_name}-dashboard-css-js"
  comment     = "Moderate cache for non-hashed CSS/JS"
  default_ttl = 3600
  max_ttl     = 86400
  min_ttl     = 0

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }

    headers_config {
      header_behavior = "none"
    }

    query_strings_config {
      query_string_behavior = "none"
    }

    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
  }
}

resource "aws_cloudfront_cache_policy" "dashboard_default_assets" {
  count = var.cloudfront_enabled ? 1 : 0

  name        = "${var.project_name}-dashboard-default-assets"
  comment     = "Moderate cache for other static assets"
  default_ttl = 3600
  max_ttl     = 86400
  min_ttl     = 0

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }

    headers_config {
      header_behavior = "none"
    }

    query_strings_config {
      query_string_behavior = "none"
    }

    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
  }
}

resource "aws_cloudfront_distribution" "dashboard" {
  count = var.cloudfront_enabled ? 1 : 0

  enabled             = true
  is_ipv6_enabled     = true
  comment             = "${var.project_name} dashboard distribution"
  default_root_object = var.cloudfront_default_root_object
  price_class         = var.cloudfront_price_class

  origin {
    domain_name              = aws_s3_bucket.dashboard.bucket_regional_domain_name
    origin_id                = local.dashboard_cloudfront_origin_id
    origin_access_control_id = aws_cloudfront_origin_access_control.dashboard[0].id
  }

  default_cache_behavior {
    target_origin_id       = local.dashboard_cloudfront_origin_id
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    cache_policy_id          = aws_cloudfront_cache_policy.dashboard_default_assets[0].id
    origin_request_policy_id = aws_cloudfront_origin_request_policy.dashboard_minimal[0].id
  }

  ordered_cache_behavior {
    path_pattern           = "*.html"
    target_origin_id       = local.dashboard_cloudfront_origin_id
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    cache_policy_id          = aws_cloudfront_cache_policy.dashboard_html[0].id
    origin_request_policy_id = aws_cloudfront_origin_request_policy.dashboard_minimal[0].id
  }

  ordered_cache_behavior {
    path_pattern           = "*.json"
    target_origin_id       = local.dashboard_cloudfront_origin_id
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    cache_policy_id          = aws_cloudfront_cache_policy.dashboard_json[0].id
    origin_request_policy_id = aws_cloudfront_origin_request_policy.dashboard_minimal[0].id
  }

  ordered_cache_behavior {
    path_pattern           = "*.css"
    target_origin_id       = local.dashboard_cloudfront_origin_id
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    cache_policy_id          = aws_cloudfront_cache_policy.dashboard_css_js[0].id
    origin_request_policy_id = aws_cloudfront_origin_request_policy.dashboard_minimal[0].id
  }

  ordered_cache_behavior {
    path_pattern           = "*.js"
    target_origin_id       = local.dashboard_cloudfront_origin_id
    viewer_protocol_policy = "redirect-to-https"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    compress               = true

    cache_policy_id          = aws_cloudfront_cache_policy.dashboard_css_js[0].id
    origin_request_policy_id = aws_cloudfront_origin_request_policy.dashboard_minimal[0].id
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    cloudfront_default_certificate = true
  }

  dynamic "logging_config" {
    for_each = var.cloudfront_enable_logging ? [1] : []

    content {
      bucket          = var.cloudfront_log_bucket_domain_name
      include_cookies = false
      prefix          = var.cloudfront_log_prefix
    }
  }

  lifecycle {
    precondition {
      condition     = !var.cloudfront_enable_logging || trimspace(var.cloudfront_log_bucket_domain_name) != ""
      error_message = "cloudfront_log_bucket_domain_name must be set when cloudfront_enable_logging is true."
    }
  }

  depends_on = [aws_cloudfront_origin_access_control.dashboard]
}
