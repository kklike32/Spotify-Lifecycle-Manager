# -----------------------------------------------------------------------------
# DynamoDB Tables
# -----------------------------------------------------------------------------

# Hot Store: Recent play events with TTL
resource "aws_dynamodb_table" "play_events" {
  name         = var.hot_table_name
  billing_mode = "PAY_PER_REQUEST" # On-demand, no provisioned capacity
  hash_key     = "dedup_key"

  attribute {
    name = "dedup_key"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true # Enable for data safety (no cost for 35-day retention)
  }

  tags = {
    Name        = "${var.project_name}-play-events"
    Description = "Recent play events with ${var.hot_table_ttl_days}-day TTL"
  }
}

# Cache: Track metadata
resource "aws_dynamodb_table" "tracks" {
  name         = var.tracks_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "track_id"

  attribute {
    name = "track_id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = false # Cache data, not critical
  }

  tags = {
    Name        = "${var.project_name}-tracks"
    Description = "Cached track metadata without TTL"
  }
}

# Cache: Artist metadata
resource "aws_dynamodb_table" "artists" {
  name         = var.artists_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "artist_id"

  attribute {
    name = "artist_id"
    type = "S"
  }

  point_in_time_recovery {
    enabled = false # Cache data, not critical
  }

  tags = {
    Name        = "${var.project_name}-artists"
    Description = "Cached artist metadata without TTL"
  }
}

# State: Pipeline state (cursors, run IDs)
resource "aws_dynamodb_table" "state" {
  name         = var.state_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "key"

  attribute {
    name = "key"
    type = "S"
  }

  point_in_time_recovery {
    enabled = false # Small state data, not critical
  }

  tags = {
    Name        = "${var.project_name}-state"
    Description = "Pipeline state for cursors and run IDs"
  }
}
