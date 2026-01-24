# -----------------------------------------------------------------------------
# AWS Region and Environment
# -----------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region for all resources (cost-conscious choice)"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (development/production)"
  type        = string
  default     = "production"

  validation {
    condition     = contains(["development", "production"], var.environment)
    error_message = "Environment must be 'development' or 'production'."
  }
}

# -----------------------------------------------------------------------------
# Naming and Tags
# -----------------------------------------------------------------------------

variable "project_name" {
  description = "Project name prefix for all resources"
  type        = string
  default     = "spotify-lifecycle"
}

# -----------------------------------------------------------------------------
# DynamoDB Configuration
# -----------------------------------------------------------------------------

variable "hot_table_name" {
  description = "DynamoDB table name for recent play events (with TTL)"
  type        = string
  default     = "spotify-play-events"
}

variable "tracks_table_name" {
  description = "DynamoDB table name for track metadata cache"
  type        = string
  default     = "spotify-tracks"
}

variable "artists_table_name" {
  description = "DynamoDB table name for artist metadata cache"
  type        = string
  default     = "spotify-artists"
}

variable "state_table_name" {
  description = "DynamoDB table name for pipeline state (cursors, run IDs)"
  type        = string
  default     = "spotify-state"
}

variable "hot_table_ttl_days" {
  description = "TTL for play events in hot table (days)"
  type        = number
  default     = 7
}

# -----------------------------------------------------------------------------
# S3 Configuration
# -----------------------------------------------------------------------------

variable "raw_bucket_name" {
  description = "S3 bucket name for cold storage (partitioned JSONL)"
  type        = string
  default     = "spotify-raw-events-kk"
}

variable "dashboard_bucket_name" {
  description = "S3 bucket name for static dashboard files"
  type        = string
  default     = "spotify-dashboard-kk"
}

# -----------------------------------------------------------------------------
# Lambda Configuration
# -----------------------------------------------------------------------------

variable "lambda_runtime" {
  description = "Python runtime version for Lambda functions"
  type        = string
  default     = "python3.12"
}

variable "ingest_lambda_timeout" {
  description = "Timeout for ingest Lambda (seconds)"
  type        = number
  default     = 60
}

variable "ingest_lambda_memory" {
  description = "Memory allocation for ingest Lambda (MB)"
  type        = number
  default     = 256
}

variable "enrich_lambda_timeout" {
  description = "Timeout for enrich Lambda (seconds)"
  type        = number
  default     = 120
}

variable "enrich_lambda_memory" {
  description = "Memory allocation for enrich Lambda (MB)"
  type        = number
  default     = 256
}

variable "playlist_lambda_timeout" {
  description = "Timeout for weekly playlist Lambda (seconds)"
  type        = number
  default     = 180
}

variable "playlist_lambda_memory" {
  description = "Memory allocation for weekly playlist Lambda (MB)"
  type        = number
  default     = 256
}

variable "aggregate_lambda_timeout" {
  description = "Timeout for aggregate Lambda (seconds)"
  type        = number
  default     = 300
}

variable "aggregate_lambda_memory" {
  description = "Memory allocation for aggregate Lambda (MB)"
  type        = number
  default     = 512
}

# -----------------------------------------------------------------------------
# Scheduling Configuration
# -----------------------------------------------------------------------------

variable "ingest_schedule" {
  description = "EventBridge schedule for ingestion (rate expression)"
  type        = string
  default     = "rate(1 hour)"
}

variable "enrich_schedule" {
  description = "EventBridge schedule for enrichment (cron expression)"
  type        = string
  default     = "cron(5 * * * ? *)" # Every hour at 5 minutes past (offset from ingest)
}

variable "playlist_schedule" {
  description = "EventBridge schedule for weekly playlists (cron expression)"
  type        = string
  default     = "cron(0 8 ? * MON *)" # Monday 8am UTC
}

variable "aggregate_schedule" {
  description = "EventBridge schedule for aggregation (cron expression)"
  type        = string
  default     = "cron(1 8 * * ? *)" # Daily 08:01 UTC
}

# -----------------------------------------------------------------------------
# CloudWatch Logs Configuration
# -----------------------------------------------------------------------------

variable "log_retention_days" {
  description = "CloudWatch Logs retention period (days)"
  type        = number
  default     = 7
}

# -----------------------------------------------------------------------------
# Cost Guardrails
# -----------------------------------------------------------------------------

variable "budget_limit_monthly" {
  description = "Monthly budget limit (USD) for cost alerts"
  type        = number
  default     = 5
}

variable "budget_threshold_percent" {
  description = "Budget threshold percentage for alerts (e.g., 80 = alert at 80%)"
  type        = number
  default     = 80
}

variable "budget_notification_email" {
  description = "Email address for budget alert notifications"
  type        = string
  default     = ""
}

# -----------------------------------------------------------------------------
# Spotify API Configuration (Secrets)
# -----------------------------------------------------------------------------

variable "spotify_client_id" {
  description = "Spotify API client ID (stored in SSM Parameter Store)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "spotify_client_secret" {
  description = "Spotify API client secret (stored in SSM Parameter Store)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "spotify_refresh_token" {
  description = "Spotify OAuth refresh token (stored in SSM Parameter Store)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "source_playlist_id" {
  description = "Spotify playlist ID to source tracks from (for weekly playlists)"
  type        = string
  default     = ""
}

variable "user_id" {
  description = "Spotify user ID (default: me)"
  type        = string
  default     = "me"
}

variable "lookback_days" {
  description = "Number of days to look back for recent plays"
  type        = number
  default     = 7
}

variable "daily_trend_days" {
  description = "Number of days to include in the daily trend series"
  type        = number
  default     = 365
}
