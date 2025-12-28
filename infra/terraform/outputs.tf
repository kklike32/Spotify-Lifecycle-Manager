# -----------------------------------------------------------------------------
# DynamoDB Table Outputs
# -----------------------------------------------------------------------------

output "hot_table_name" {
  description = "Name of the DynamoDB table for play events (with TTL)"
  value       = aws_dynamodb_table.play_events.name
}

output "hot_table_arn" {
  description = "ARN of the DynamoDB table for play events"
  value       = aws_dynamodb_table.play_events.arn
}

output "tracks_table_name" {
  description = "Name of the DynamoDB table for track metadata"
  value       = aws_dynamodb_table.tracks.name
}

output "tracks_table_arn" {
  description = "ARN of the DynamoDB table for track metadata"
  value       = aws_dynamodb_table.tracks.arn
}

output "artists_table_name" {
  description = "Name of the DynamoDB table for artist metadata"
  value       = aws_dynamodb_table.artists.name
}

output "artists_table_arn" {
  description = "ARN of the DynamoDB table for artist metadata"
  value       = aws_dynamodb_table.artists.arn
}

output "state_table_name" {
  description = "Name of the DynamoDB table for pipeline state"
  value       = aws_dynamodb_table.state.name
}

output "state_table_arn" {
  description = "ARN of the DynamoDB table for pipeline state"
  value       = aws_dynamodb_table.state.arn
}

# -----------------------------------------------------------------------------
# S3 Bucket Outputs
# -----------------------------------------------------------------------------

output "raw_bucket_name" {
  description = "Name of the S3 bucket for cold storage"
  value       = aws_s3_bucket.raw_events.id
}

output "raw_bucket_arn" {
  description = "ARN of the S3 bucket for cold storage"
  value       = aws_s3_bucket.raw_events.arn
}

output "dashboard_bucket_name" {
  description = "Name of the S3 bucket for dashboard files"
  value       = aws_s3_bucket.dashboard.id
}

output "dashboard_bucket_arn" {
  description = "ARN of the S3 bucket for dashboard files"
  value       = aws_s3_bucket.dashboard.arn
}

output "dashboard_website_endpoint" {
  description = "S3 website endpoint for the dashboard"
  value       = aws_s3_bucket_website_configuration.dashboard.website_endpoint
}

output "dashboard_website_url" {
  description = "Full URL for the dashboard website"
  value       = "http://${aws_s3_bucket_website_configuration.dashboard.website_endpoint}"
}

# -----------------------------------------------------------------------------
# Lambda Function Outputs
# -----------------------------------------------------------------------------

output "ingest_lambda_arn" {
  description = "ARN of the ingestion Lambda function"
  value       = aws_lambda_function.ingest.arn
}

output "ingest_lambda_name" {
  description = "Name of the ingestion Lambda function"
  value       = aws_lambda_function.ingest.function_name
}

output "enrich_lambda_arn" {
  description = "ARN of the enrichment Lambda function"
  value       = aws_lambda_function.enrich.arn
}

output "enrich_lambda_name" {
  description = "Name of the enrichment Lambda function"
  value       = aws_lambda_function.enrich.function_name
}

output "playlist_lambda_arn" {
  description = "ARN of the weekly playlist Lambda function"
  value       = aws_lambda_function.playlist.arn
}

output "playlist_lambda_name" {
  description = "Name of the weekly playlist Lambda function"
  value       = aws_lambda_function.playlist.function_name
}

output "aggregate_lambda_arn" {
  description = "ARN of the aggregation Lambda function"
  value       = aws_lambda_function.aggregate.arn
}

output "aggregate_lambda_name" {
  description = "Name of the aggregation Lambda function"
  value       = aws_lambda_function.aggregate.function_name
}

# -----------------------------------------------------------------------------
# EventBridge Outputs
# -----------------------------------------------------------------------------

output "ingest_schedule_arn" {
  description = "ARN of the EventBridge schedule for ingestion"
  value       = aws_cloudwatch_event_rule.ingest_trigger.arn
}

output "enrich_schedule_arn" {
  description = "ARN of the EventBridge schedule for enrichment"
  value       = aws_cloudwatch_event_rule.enrich_trigger.arn
}

output "playlist_schedule_arn" {
  description = "ARN of the EventBridge schedule for weekly playlists"
  value       = aws_cloudwatch_event_rule.playlist_trigger.arn
}

output "aggregate_schedule_arn" {
  description = "ARN of the EventBridge schedule for aggregation"
  value       = aws_cloudwatch_event_rule.aggregate_trigger.arn
}

# -----------------------------------------------------------------------------
# CloudWatch Alarms Outputs
# -----------------------------------------------------------------------------

output "ingest_error_alarm_arn" {
  description = "ARN of the CloudWatch alarm for ingestion errors"
  value       = aws_cloudwatch_metric_alarm.ingest_errors.arn
}

output "enrich_error_alarm_arn" {
  description = "ARN of the CloudWatch alarm for enrichment errors"
  value       = aws_cloudwatch_metric_alarm.enrich_errors.arn
}

output "playlist_error_alarm_arn" {
  description = "ARN of the CloudWatch alarm for playlist errors"
  value       = aws_cloudwatch_metric_alarm.playlist_errors.arn
}

output "aggregate_error_alarm_arn" {
  description = "ARN of the CloudWatch alarm for aggregation errors"
  value       = aws_cloudwatch_metric_alarm.aggregate_errors.arn
}

# -----------------------------------------------------------------------------
# IAM Role Outputs
# -----------------------------------------------------------------------------

output "lambda_role_arn" {
  description = "ARN of the IAM role used by Lambda functions"
  value       = aws_iam_role.lambda_execution.arn
}

output "lambda_role_name" {
  description = "Name of the IAM role used by Lambda functions"
  value       = aws_iam_role.lambda_execution.name
}

# -----------------------------------------------------------------------------
# Cost Guardrails Outputs
# -----------------------------------------------------------------------------

output "budget_name" {
  description = "Name of the AWS budget for cost alerts"
  value       = var.budget_notification_email != "" ? aws_budgets_budget.monthly[0].budget_name : "Not configured"
}

output "budget_limit" {
  description = "Monthly budget limit (USD)"
  value       = var.budget_limit_monthly
}

# -----------------------------------------------------------------------------
# Deployment Summary
# -----------------------------------------------------------------------------

output "deployment_summary" {
  description = "Summary of deployed resources"
  value = {
    environment    = var.environment
    region         = var.aws_region
    project_name   = var.project_name
    dynamodb_tables = {
      play_events = aws_dynamodb_table.play_events.name
      tracks      = aws_dynamodb_table.tracks.name
      artists     = aws_dynamodb_table.artists.name
      state       = aws_dynamodb_table.state.name
    }
    s3_buckets = {
      raw_events = aws_s3_bucket.raw_events.id
      dashboard  = aws_s3_bucket.dashboard.id
    }
    lambda_functions = {
      ingest    = aws_lambda_function.ingest.function_name
      enrich    = aws_lambda_function.enrich.function_name
      playlist  = aws_lambda_function.playlist.function_name
      aggregate = aws_lambda_function.aggregate.function_name
    }
    schedules = {
      ingest    = var.ingest_schedule
      enrich    = var.enrich_schedule
      playlist  = var.playlist_schedule
      aggregate = var.aggregate_schedule
    }
    dashboard_url = "http://${aws_s3_bucket_website_configuration.dashboard.website_endpoint}"
  }
}
