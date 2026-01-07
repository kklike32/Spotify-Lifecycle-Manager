# -----------------------------------------------------------------------------
# EventBridge Rule: Ingest (Hourly)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "ingest_trigger" {
  name                = "${var.project_name}-ingest-trigger"
  description         = "Trigger ingestion Lambda every hour"
  schedule_expression = var.ingest_schedule

  tags = {
    Name        = "${var.project_name}-ingest-trigger"
    Description = "Hourly ingestion trigger"
  }
}

resource "aws_cloudwatch_event_target" "ingest_trigger" {
  rule      = aws_cloudwatch_event_rule.ingest_trigger.name
  target_id = "IngestLambda"
  arn       = aws_lambda_function.ingest.arn
}

resource "aws_lambda_permission" "ingest_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingest_trigger.arn
}

# -----------------------------------------------------------------------------
# EventBridge Rule: Enrich (Hourly, offset 5 minutes)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "enrich_trigger" {
  name                = "${var.project_name}-enrich-trigger"
  description         = "Trigger enrichment Lambda every hour (5 minutes after ingest)"
  schedule_expression = var.enrich_schedule

  tags = {
    Name        = "${var.project_name}-enrich-trigger"
    Description = "Hourly enrichment trigger"
  }
}

resource "aws_cloudwatch_event_target" "enrich_trigger" {
  rule      = aws_cloudwatch_event_rule.enrich_trigger.name
  target_id = "EnrichLambda"
  arn       = aws_lambda_function.enrich.arn
}

resource "aws_lambda_permission" "enrich_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.enrich.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.enrich_trigger.arn
}

# -----------------------------------------------------------------------------
# EventBridge Rule: Weekly Playlist (Monday 8am UTC)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "playlist_trigger" {
  name                = "${var.project_name}-playlist-trigger"
  description         = "Trigger weekly playlist Lambda every Monday at 8am UTC"
  schedule_expression = var.playlist_schedule

  tags = {
    Name        = "${var.project_name}-playlist-trigger"
    Description = "Weekly playlist trigger"
  }
}

resource "aws_cloudwatch_event_target" "playlist_trigger" {
  rule      = aws_cloudwatch_event_rule.playlist_trigger.name
  target_id = "PlaylistLambda"
  arn       = aws_lambda_function.playlist.arn
}

resource "aws_lambda_permission" "playlist_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.playlist.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.playlist_trigger.arn
}

# -----------------------------------------------------------------------------
# EventBridge Rule: Aggregate (Daily 2am UTC)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "aggregate_trigger" {
  name                = "${var.project_name}-aggregate-trigger"
  description         = "Trigger aggregation Lambda daily at 2am UTC"
  schedule_expression = var.aggregate_schedule

  tags = {
    Name        = "${var.project_name}-aggregate-trigger"
    Description = "Daily aggregation trigger"
  }
}

resource "aws_cloudwatch_event_target" "aggregate_trigger" {
  rule      = aws_cloudwatch_event_rule.aggregate_trigger.name
  target_id = "AggregateLambda"
  arn       = aws_lambda_function.aggregate.arn
}

resource "aws_lambda_permission" "aggregate_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.aggregate.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.aggregate_trigger.arn
}

# -----------------------------------------------------------------------------
# EventBridge Rule: Backfill (Daily 1:50am UTC, before aggregation)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "backfill_trigger" {
  name                = "${var.project_name}-backfill-trigger"
  description         = "Trigger backfill Lambda daily at 1:50am UTC"
  schedule_expression = "cron(50 1 * * ? *)" # 10 minutes before aggregation

  tags = {
    Name = "${var.project_name}-backfill-trigger"
  }
}

resource "aws_cloudwatch_event_target" "backfill_trigger" {
  rule      = aws_cloudwatch_event_rule.backfill_trigger.name
  target_id = "BackfillLambda"
  arn       = aws_lambda_function.backfill.arn
}

resource "aws_lambda_permission" "backfill_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.backfill.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.backfill_trigger.arn
}
