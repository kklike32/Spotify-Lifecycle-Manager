# -----------------------------------------------------------------------------
# CloudFront Outputs
# -----------------------------------------------------------------------------

output "cloudfront_distribution_id" {
  description = "ID of the CloudFront distribution serving the dashboard"
  value       = var.cloudfront_enabled ? aws_cloudfront_distribution.dashboard[0].id : ""
}

output "cloudfront_domain_name" {
  description = "Domain name of the CloudFront distribution serving the dashboard"
  value       = var.cloudfront_enabled ? aws_cloudfront_distribution.dashboard[0].domain_name : ""
}
