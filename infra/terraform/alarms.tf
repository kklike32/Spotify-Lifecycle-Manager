# -----------------------------------------------------------------------------
# CloudWatch Alarms: Lambda Function Errors
# -----------------------------------------------------------------------------

# Ingest Lambda Error Alarm
resource "aws_cloudwatch_metric_alarm" "ingest_errors" {
  alarm_name          = "${var.project_name}-ingest-errors"
  alarm_description   = "Alert when ingestion Lambda has errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600 # 1 hour
  statistic           = "Sum"
  threshold           = 2 # Alert if 2+ errors in 1 hour
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.ingest.function_name
  }

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-ingest-errors"
    Description = "Ingestion Lambda error alarm"
  }
}

# Enrich Lambda Error Alarm
resource "aws_cloudwatch_metric_alarm" "enrich_errors" {
  alarm_name          = "${var.project_name}-enrich-errors"
  alarm_description   = "Alert when enrichment Lambda has errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600
  statistic           = "Sum"
  threshold           = 2
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.enrich.function_name
  }

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-enrich-errors"
    Description = "Enrichment Lambda error alarm"
  }
}

# Playlist Lambda Error Alarm
resource "aws_cloudwatch_metric_alarm" "playlist_errors" {
  alarm_name          = "${var.project_name}-playlist-errors"
  alarm_description   = "Alert when weekly playlist Lambda has errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600
  statistic           = "Sum"
  threshold           = 1 # Alert on any error (weekly runs only)
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.playlist.function_name
  }

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-playlist-errors"
    Description = "Weekly playlist Lambda error alarm"
  }
}

# Aggregate Lambda Error Alarm
resource "aws_cloudwatch_metric_alarm" "aggregate_errors" {
  alarm_name          = "${var.project_name}-aggregate-errors"
  alarm_description   = "Alert when aggregation Lambda has errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.aggregate.function_name
  }

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-aggregate-errors"
    Description = "Aggregation Lambda error alarm"
  }
}

# -----------------------------------------------------------------------------
# SNS Topic for Alarms (optional, created if email provided)
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "alarms" {
  count = var.budget_notification_email != "" ? 1 : 0

  name = "${var.project_name}-alarms"

  tags = {
    Name        = "${var.project_name}-alarms"
    Description = "SNS topic for CloudWatch and Budget alarms"
  }
}

resource "aws_sns_topic_subscription" "alarms_email" {
  count = var.budget_notification_email != "" ? 1 : 0

  topic_arn = aws_sns_topic.alarms[0].arn
  protocol  = "email"
  endpoint  = var.budget_notification_email
}

# -----------------------------------------------------------------------------
# CloudWatch Log Metric Filters (Aggregate Lambda)
# -----------------------------------------------------------------------------

# Aggregate total play count alarm (uses EMF auto-created metric)
resource "aws_cloudwatch_metric_alarm" "aggregate_total_play_count_high" {
  alarm_name          = "${var.project_name}-aggregate-total-play-count-high"
  alarm_description   = "Alert when aggregate total_play_count exceeds expected threshold"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "aggregate_total_play_count"
  namespace           = var.project_name
  period              = 300
  statistic           = "Maximum"
  threshold           = 2000
  treat_missing_data  = "notBreaching"

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-aggregate-total-play-count-high"
    Description = "Aggregate Lambda total_play_count anomaly"
  }
}

resource "aws_cloudwatch_log_metric_filter" "aggregate_summary_rejected" {
  name           = "${var.project_name}-aggregate-summary-rejected"
  log_group_name = aws_cloudwatch_log_group.aggregate.name
  pattern        = "\"summary rejected\""

  metric_transformation {
    name      = "aggregate_summary_rejected"
    namespace = var.project_name
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "aggregate_summary_rejected_alarm" {
  alarm_name          = "${var.project_name}-aggregate-summary-rejected"
  alarm_description   = "Alert when aggregate drops implausible summaries"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.aggregate_summary_rejected.metric_transformation[0].name
  namespace           = var.project_name
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-aggregate-summary-rejected"
    Description = "Aggregate summary rejection alarm"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Log Metric Filters (Ingest Lambda) for daily summary mismatch
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_metric_filter" "ingest_summary_mismatch" {
  name           = "${var.project_name}-ingest-summary-mismatch"
  log_group_name = aws_cloudwatch_log_group.ingest.name
  pattern        = "\"daily summary mismatch detected\""

  metric_transformation {
    name      = "ingest_summary_mismatch"
    namespace = var.project_name
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingest_summary_mismatch_alarm" {
  alarm_name          = "${var.project_name}-ingest-summary-mismatch"
  alarm_description   = "Alert when ingest rewrites daily summaries due to mismatches"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.ingest_summary_mismatch.metric_transformation[0].name
  namespace           = var.project_name
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-ingest-summary-mismatch"
    Description = "Ingest daily summary mismatch alarm"
  }
}
