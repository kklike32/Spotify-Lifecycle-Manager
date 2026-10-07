# -----------------------------------------------------------------------------
# CloudWatch Log Metric Filters: caught application errors (lambda_failed)
# -----------------------------------------------------------------------------
# _handle_error() in lambda_handler.py catches exceptions and returns a normal
# dict (statusCode 500) instead of re-raising, so these invocations never
# increment the built-in AWS/Lambda "Errors" metric. These filters catch that
# case from the "lambda_failed" log key shared by every handler, and are
# combined into each function's existing error alarm below (no new alarm
# resources, so no additional per-alarm cost).

resource "aws_cloudwatch_log_metric_filter" "ingest_lambda_failed" {
  name           = "${var.project_name}-ingest-lambda-failed"
  log_group_name = aws_cloudwatch_log_group.ingest.name
  pattern        = "\"lambda_failed\""

  metric_transformation {
    name      = "ingest_lambda_failed"
    namespace = var.project_name
    value     = "1"
  }
}

resource "aws_cloudwatch_log_metric_filter" "enrich_lambda_failed" {
  name           = "${var.project_name}-enrich-lambda-failed"
  log_group_name = aws_cloudwatch_log_group.enrich.name
  pattern        = "\"lambda_failed\""

  metric_transformation {
    name      = "enrich_lambda_failed"
    namespace = var.project_name
    value     = "1"
  }
}

resource "aws_cloudwatch_log_metric_filter" "playlist_lambda_failed" {
  name           = "${var.project_name}-playlist-lambda-failed"
  log_group_name = aws_cloudwatch_log_group.playlist.name
  pattern        = "\"lambda_failed\""

  metric_transformation {
    name      = "playlist_lambda_failed"
    namespace = var.project_name
    value     = "1"
  }
}

resource "aws_cloudwatch_log_metric_filter" "aggregate_lambda_failed" {
  name           = "${var.project_name}-aggregate-lambda-failed"
  log_group_name = aws_cloudwatch_log_group.aggregate.name
  pattern        = "\"lambda_failed\""

  metric_transformation {
    name      = "aggregate_lambda_failed"
    namespace = var.project_name
    value     = "1"
  }
}

resource "aws_cloudwatch_log_metric_filter" "backfill_lambda_failed" {
  name           = "${var.project_name}-backfill-lambda-failed"
  log_group_name = aws_cloudwatch_log_group.backfill.name
  pattern        = "\"lambda_failed\""

  metric_transformation {
    name      = "backfill_lambda_failed"
    namespace = var.project_name
    value     = "1"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Alarms: Lambda Function Errors
# -----------------------------------------------------------------------------
# Each alarm sums the built-in AWS/Lambda "Errors" metric (runtime crashes,
# timeouts, OOM) with the "lambda_failed" log metric above (caught application
# errors, e.g. expired Spotify refresh token) via metric math, so either
# failure mode trips the same alarm and reaches the same SNS subscription.

# Ingest Lambda Error Alarm
resource "aws_cloudwatch_metric_alarm" "ingest_errors" {
  alarm_name          = "${var.project_name}-ingest-errors"
  alarm_description   = "Alert when ingestion Lambda has errors (runtime crashes or caught application errors)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 2 # Alert if 2+ errors in 1 hour
  treat_missing_data  = "notBreaching"

  metric_query {
    id          = "e1"
    expression  = "m1 + FILL(m2, 0)"
    label       = "ingest_total_errors"
    return_data = true
  }

  metric_query {
    id = "m1"
    metric {
      metric_name = "Errors"
      namespace   = "AWS/Lambda"
      period      = 3600
      stat        = "Sum"
      dimensions = {
        FunctionName = aws_lambda_function.ingest.function_name
      }
    }
  }

  metric_query {
    id = "m2"
    metric {
      metric_name = aws_cloudwatch_log_metric_filter.ingest_lambda_failed.metric_transformation[0].name
      namespace   = var.project_name
      period      = 3600
      stat        = "Sum"
    }
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
  alarm_description   = "Alert when enrichment Lambda has errors (runtime crashes or caught application errors)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 2
  treat_missing_data  = "notBreaching"

  metric_query {
    id          = "e1"
    expression  = "m1 + FILL(m2, 0)"
    label       = "enrich_total_errors"
    return_data = true
  }

  metric_query {
    id = "m1"
    metric {
      metric_name = "Errors"
      namespace   = "AWS/Lambda"
      period      = 3600
      stat        = "Sum"
      dimensions = {
        FunctionName = aws_lambda_function.enrich.function_name
      }
    }
  }

  metric_query {
    id = "m2"
    metric {
      metric_name = aws_cloudwatch_log_metric_filter.enrich_lambda_failed.metric_transformation[0].name
      namespace   = var.project_name
      period      = 3600
      stat        = "Sum"
    }
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
  alarm_description   = "Alert when weekly playlist Lambda has errors (runtime crashes or caught application errors)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 1 # Alert on any error (weekly runs only)
  treat_missing_data  = "notBreaching"

  metric_query {
    id          = "e1"
    expression  = "m1 + FILL(m2, 0)"
    label       = "playlist_total_errors"
    return_data = true
  }

  metric_query {
    id = "m1"
    metric {
      metric_name = "Errors"
      namespace   = "AWS/Lambda"
      period      = 3600
      stat        = "Sum"
      dimensions = {
        FunctionName = aws_lambda_function.playlist.function_name
      }
    }
  }

  metric_query {
    id = "m2"
    metric {
      metric_name = aws_cloudwatch_log_metric_filter.playlist_lambda_failed.metric_transformation[0].name
      namespace   = var.project_name
      period      = 3600
      stat        = "Sum"
    }
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
  alarm_description   = "Alert when aggregation Lambda has errors (runtime crashes or caught application errors)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"

  metric_query {
    id          = "e1"
    expression  = "m1 + FILL(m2, 0)"
    label       = "aggregate_total_errors"
    return_data = true
  }

  metric_query {
    id = "m1"
    metric {
      metric_name = "Errors"
      namespace   = "AWS/Lambda"
      period      = 3600
      stat        = "Sum"
      dimensions = {
        FunctionName = aws_lambda_function.aggregate.function_name
      }
    }
  }

  metric_query {
    id = "m2"
    metric {
      metric_name = aws_cloudwatch_log_metric_filter.aggregate_lambda_failed.metric_transformation[0].name
      namespace   = var.project_name
      period      = 3600
      stat        = "Sum"
    }
  }

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-aggregate-errors"
    Description = "Aggregation Lambda error alarm"
  }
}

# Backfill Lambda Error Alarm
resource "aws_cloudwatch_metric_alarm" "backfill_errors" {
  alarm_name          = "${var.project_name}-backfill-errors"
  alarm_description   = "Alert when backfill Lambda has errors (runtime crashes or caught application errors)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"

  metric_query {
    id          = "e1"
    expression  = "m1 + FILL(m2, 0)"
    label       = "backfill_total_errors"
    return_data = true
  }

  metric_query {
    id = "m1"
    metric {
      metric_name = "Errors"
      namespace   = "AWS/Lambda"
      period      = 3600
      stat        = "Sum"
      dimensions = {
        FunctionName = aws_lambda_function.backfill.function_name
      }
    }
  }

  metric_query {
    id = "m2"
    metric {
      metric_name = aws_cloudwatch_log_metric_filter.backfill_lambda_failed.metric_transformation[0].name
      namespace   = var.project_name
      period      = 3600
      stat        = "Sum"
    }
  }

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-backfill-errors"
    Description = "Backfill Lambda error alarm"
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

# Aggregate total play count alarm (legacy static threshold mode)
resource "aws_cloudwatch_metric_alarm" "aggregate_total_play_count_high_static" {
  count               = var.aggregate_alarm_mode == "static" ? 1 : 0
  alarm_name          = "${var.project_name}-aggregate-total-play-count-high"
  alarm_description   = "Legacy static threshold alert when aggregate total_play_count exceeds expected threshold"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "aggregate_total_play_count"
  namespace           = var.project_name
  period              = 300
  statistic           = "Maximum"
  threshold           = var.aggregate_total_play_count_threshold
  treat_missing_data  = "notBreaching"

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-aggregate-total-play-count-high"
    Description = "Aggregate Lambda total_play_count anomaly"
  }
}

# Aggregate total play count anomaly alarm (recommended mode)
resource "aws_cloudwatch_metric_alarm" "aggregate_total_play_count_high_anomaly" {
  count               = var.aggregate_alarm_mode == "anomaly" ? 1 : 0
  alarm_name          = "${var.project_name}-aggregate-total-play-count-high"
  alarm_description   = "Alert when aggregate total_play_count deviates from normal growth trend"
  comparison_operator = "LessThanLowerOrGreaterThanUpperThreshold"
  evaluation_periods  = 1
  threshold_metric_id = "ad1"
  treat_missing_data  = "notBreaching"

  metric_query {
    id = "ad1"

    expression  = "ANOMALY_DETECTION_BAND(m1, ${var.aggregate_anomaly_sensitivity})"
    label       = "aggregate_total_play_count_expected_band"
    return_data = true
  }

  metric_query {
    id = "m1"

    metric {
      metric_name = "aggregate_total_play_count"
      namespace   = var.project_name
      period      = 300
      stat        = "Maximum"
    }
    return_data = true
  }

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
# NOTE: This alarm ONLY fires when counts DECREASE (unexpected data loss).
# Normal count INCREASES from new events arriving are logged as INFO and do not trigger alarms.
# The metric is published only when a log line matches, so the alarm is usually
# INSUFFICIENT_DATA. datapoints_to_alarm applies to OK -> ALARM only; one
# matching line still transitions INSUFFICIENT_DATA -> ALARM and pages.

resource "aws_cloudwatch_log_metric_filter" "ingest_summary_mismatch" {
  name           = "${var.project_name}-ingest-summary-mismatch"
  log_group_name = aws_cloudwatch_log_group.ingest.name
  pattern        = "\"count DECREASED\""

  metric_transformation {
    name      = "ingest_summary_mismatch"
    namespace = var.project_name
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingest_summary_mismatch_alarm" {
  alarm_name          = "${var.project_name}-ingest-summary-mismatch"
  alarm_description   = "Alert when daily summary counts DECREASE unexpectedly (data loss/bug). Search ingest logs for 'count DECREASED'."
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 3 # 15 minutes lookback
  datapoints_to_alarm = 2 # OK -> ALARM only; one INSUFFICIENT_DATA hit still pages
  metric_name         = aws_cloudwatch_log_metric_filter.ingest_summary_mismatch.metric_transformation[0].name
  namespace           = var.project_name
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "missing" # honest: no data -> insufficient, not OK

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-ingest-summary-mismatch"
    Description = "Ingest daily summary count decrease alarm - data loss detector"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Log Metric Filters (Ingest Lambda) for summary write failures
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_metric_filter" "ingest_summary_write_failed" {
  name           = "${var.project_name}-ingest-summary-write-failed"
  log_group_name = aws_cloudwatch_log_group.ingest.name
  pattern        = "\"daily_summary_write_failed\""

  metric_transformation {
    name      = "ingest_summary_write_failed"
    namespace = var.project_name
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingest_summary_write_failed_alarm" {
  alarm_name          = "${var.project_name}-ingest-summary-write-failed"
  alarm_description   = "Alert when daily summary writes fail during ingestion - may cause missing dashboard data"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.ingest_summary_write_failed.metric_transformation[0].name
  namespace           = var.project_name
  period              = 3600 # 1 hour window
  statistic           = "Sum"
  threshold           = 3 # Alert if 3+ failures in an hour
  treat_missing_data  = "notBreaching"

  alarm_actions = var.budget_notification_email != "" ? [aws_sns_topic.alarms[0].arn] : []

  tags = {
    Name        = "${var.project_name}-ingest-summary-write-failed"
    Description = "Daily summary write failure alarm - critical for dashboard accuracy"
  }
}
