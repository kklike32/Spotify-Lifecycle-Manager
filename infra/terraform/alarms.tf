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
