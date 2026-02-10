# -----------------------------------------------------------------------------
# CloudFront Configuration (Dashboard)
# -----------------------------------------------------------------------------

variable "cloudfront_enabled" {
  description = "Whether to create a CloudFront distribution for the dashboard"
  type        = bool
  default     = true
}

variable "cloudfront_price_class" {
  description = "CloudFront price class (cost guardrail: keep PriceClass_100)"
  type        = string
  default     = "PriceClass_100"

  validation {
    condition     = contains(["PriceClass_100", "PriceClass_200", "PriceClass_All"], var.cloudfront_price_class)
    error_message = "cloudfront_price_class must be one of PriceClass_100, PriceClass_200, or PriceClass_All."
  }
}

variable "cloudfront_default_root_object" {
  description = "Default object served at the CloudFront root path"
  type        = string
  default     = "index.html"
}

variable "cloudfront_enable_logging" {
  description = "Enable CloudFront standard logs (disabled by default to avoid S3 logging costs)"
  type        = bool
  default     = false
}

variable "cloudfront_log_bucket_domain_name" {
  description = "S3 bucket domain name for CloudFront logs (for example: my-log-bucket.s3.amazonaws.com). Required only when cloudfront_enable_logging=true"
  type        = string
  default     = ""
}

variable "cloudfront_log_prefix" {
  description = "Prefix for CloudFront log objects when logging is enabled"
  type        = string
  default     = "cloudfront/"
}
