# -----------------------------------------------------------------------------
# Lambda Deployment Package (shared across all functions)
# -----------------------------------------------------------------------------

# Package the Lambda code (assuming deployment from repository root)
data "archive_file" "lambda_package" {
  type        = "zip"
  source_dir  = "${path.module}/../../src"
  output_path = "${path.module}/.terraform/lambda_package.zip"

  excludes = [
    "__pycache__",
    "*.pyc",
    "*.pyo",
    ".pytest_cache",
    ".ruff_cache",
    "tests"
  ]
}

# -----------------------------------------------------------------------------
# Lambda Function: Ingest
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "ingest" {
  function_name = "${var.project_name}-ingest"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "spotify_lifecycle.lambda_handler.ingest_handler"
  runtime       = var.lambda_runtime
  timeout       = var.ingest_lambda_timeout
  memory_size   = var.ingest_lambda_memory

  filename         = data.archive_file.lambda_package.output_path
  source_code_hash = data.archive_file.lambda_package.output_base64sha256

  environment {
    variables = {
      ENVIRONMENT              = var.environment
      HOT_TABLE_NAME           = aws_dynamodb_table.play_events.name
      TRACKS_TABLE_NAME        = aws_dynamodb_table.tracks.name
      ARTISTS_TABLE_NAME       = aws_dynamodb_table.artists.name
      STATE_TABLE_NAME         = aws_dynamodb_table.state.name
      RAW_BUCKET_NAME          = aws_s3_bucket.raw_events.id
      DASHBOARD_BUCKET_NAME    = aws_s3_bucket.dashboard.id
      AWS_REGION               = var.aws_region
      LOOKBACK_DAYS            = var.lookback_days
      SOURCE_PLAYLIST_ID       = var.source_playlist_id
      USER_ID                  = var.user_id
      # Secrets loaded from SSM Parameter Store at runtime
      SPOTIFY_CLIENT_ID_PARAM  = "/${var.project_name}/spotify/client_id"
      SPOTIFY_CLIENT_SECRET_PARAM = "/${var.project_name}/spotify/client_secret"
      SPOTIFY_REFRESH_TOKEN_PARAM = "/${var.project_name}/spotify/refresh_token"
    }
  }

  tags = {
    Name        = "${var.project_name}-ingest"
    Description = "Fetch recent plays from Spotify API"
  }
}

resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/aws/lambda/${aws_lambda_function.ingest.function_name}"
  retention_in_days = var.log_retention_days

  tags = {
    Name        = "${var.project_name}-ingest-logs"
    Description = "Logs for ingestion Lambda (${var.log_retention_days}-day retention)"
  }
}

# -----------------------------------------------------------------------------
# Lambda Function: Enrich
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "enrich" {
  function_name = "${var.project_name}-enrich"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "spotify_lifecycle.lambda_handler.enrich_handler"
  runtime       = var.lambda_runtime
  timeout       = var.enrich_lambda_timeout
  memory_size   = var.enrich_lambda_memory

  filename         = data.archive_file.lambda_package.output_path
  source_code_hash = data.archive_file.lambda_package.output_base64sha256

  environment {
    variables = {
      ENVIRONMENT              = var.environment
      HOT_TABLE_NAME           = aws_dynamodb_table.play_events.name
      TRACKS_TABLE_NAME        = aws_dynamodb_table.tracks.name
      ARTISTS_TABLE_NAME       = aws_dynamodb_table.artists.name
      STATE_TABLE_NAME         = aws_dynamodb_table.state.name
      RAW_BUCKET_NAME          = aws_s3_bucket.raw_events.id
      DASHBOARD_BUCKET_NAME    = aws_s3_bucket.dashboard.id
      AWS_REGION               = var.aws_region
      LOOKBACK_DAYS            = var.lookback_days
      SOURCE_PLAYLIST_ID       = var.source_playlist_id
      USER_ID                  = var.user_id
      SPOTIFY_CLIENT_ID_PARAM  = "/${var.project_name}/spotify/client_id"
      SPOTIFY_CLIENT_SECRET_PARAM = "/${var.project_name}/spotify/client_secret"
      SPOTIFY_REFRESH_TOKEN_PARAM = "/${var.project_name}/spotify/refresh_token"
    }
  }

  tags = {
    Name        = "${var.project_name}-enrich"
    Description = "Enrich track and artist metadata"
  }
}

resource "aws_cloudwatch_log_group" "enrich" {
  name              = "/aws/lambda/${aws_lambda_function.enrich.function_name}"
  retention_in_days = var.log_retention_days

  tags = {
    Name        = "${var.project_name}-enrich-logs"
    Description = "Logs for enrichment Lambda (${var.log_retention_days}-day retention)"
  }
}

# -----------------------------------------------------------------------------
# Lambda Function: Weekly Playlist
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "playlist" {
  function_name = "${var.project_name}-playlist"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "spotify_lifecycle.lambda_handler.playlist_handler"
  runtime       = var.lambda_runtime
  timeout       = var.playlist_lambda_timeout
  memory_size   = var.playlist_lambda_memory

  filename         = data.archive_file.lambda_package.output_path
  source_code_hash = data.archive_file.lambda_package.output_base64sha256

  environment {
    variables = {
      ENVIRONMENT              = var.environment
      HOT_TABLE_NAME           = aws_dynamodb_table.play_events.name
      TRACKS_TABLE_NAME        = aws_dynamodb_table.tracks.name
      ARTISTS_TABLE_NAME       = aws_dynamodb_table.artists.name
      STATE_TABLE_NAME         = aws_dynamodb_table.state.name
      RAW_BUCKET_NAME          = aws_s3_bucket.raw_events.id
      DASHBOARD_BUCKET_NAME    = aws_s3_bucket.dashboard.id
      AWS_REGION               = var.aws_region
      LOOKBACK_DAYS            = var.lookback_days
      SOURCE_PLAYLIST_ID       = var.source_playlist_id
      USER_ID                  = var.user_id
      SPOTIFY_CLIENT_ID_PARAM  = "/${var.project_name}/spotify/client_id"
      SPOTIFY_CLIENT_SECRET_PARAM = "/${var.project_name}/spotify/client_secret"
      SPOTIFY_REFRESH_TOKEN_PARAM = "/${var.project_name}/spotify/refresh_token"
    }
  }

  tags = {
    Name        = "${var.project_name}-playlist"
    Description = "Create weekly playlists from unheard tracks"
  }
}

resource "aws_cloudwatch_log_group" "playlist" {
  name              = "/aws/lambda/${aws_lambda_function.playlist.function_name}"
  retention_in_days = var.log_retention_days

  tags = {
    Name        = "${var.project_name}-playlist-logs"
    Description = "Logs for playlist Lambda (${var.log_retention_days}-day retention)"
  }
}

# -----------------------------------------------------------------------------
# Lambda Function: Aggregate
# -----------------------------------------------------------------------------

resource "aws_lambda_function" "aggregate" {
  function_name = "${var.project_name}-aggregate"
  role          = aws_iam_role.lambda_execution.arn
  handler       = "spotify_lifecycle.lambda_handler.aggregate_handler"
  runtime       = var.lambda_runtime
  timeout       = 180  # 3 minutes (down from 300s)
  memory_size   = 256  # 256 MB (down from 512MB)

  filename         = data.archive_file.lambda_package.output_path
  source_code_hash = data.archive_file.lambda_package.output_base64sha256

  environment {
    variables = {
      ENVIRONMENT              = var.environment
      HOT_TABLE_NAME           = aws_dynamodb_table.play_events.name
      TRACKS_TABLE_NAME        = aws_dynamodb_table.tracks.name
      ARTISTS_TABLE_NAME       = aws_dynamodb_table.artists.name
      STATE_TABLE_NAME         = aws_dynamodb_table.state.name
      RAW_BUCKET_NAME          = aws_s3_bucket.raw_events.id
      DASHBOARD_BUCKET_NAME    = aws_s3_bucket.dashboard.id
      AWS_REGION               = var.aws_region
      LOOKBACK_DAYS            = var.lookback_days
      SOURCE_PLAYLIST_ID       = var.source_playlist_id
      USER_ID                  = var.user_id
      SPOTIFY_CLIENT_ID_PARAM  = "/${var.project_name}/spotify/client_id"
      SPOTIFY_CLIENT_SECRET_PARAM = "/${var.project_name}/spotify/client_secret"
      SPOTIFY_REFRESH_TOKEN_PARAM = "/${var.project_name}/spotify/refresh_token"
    }
  }

  tags = {
    Name        = "${var.project_name}-aggregate"
    Description = "Precompute dashboard analytics"
  }
}

resource "aws_cloudwatch_log_group" "aggregate" {
  name              = "/aws/lambda/${aws_lambda_function.aggregate.function_name}"
  retention_in_days = var.log_retention_days

  tags = {
    Name        = "${var.project_name}-aggregate-logs"
    Description = "Logs for aggregation Lambda (${var.log_retention_days}-day retention)"
  }
}
