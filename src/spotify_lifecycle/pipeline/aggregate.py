"""Analyst stage: Builds analytics and aggregates."""

from datetime import datetime, timedelta

from spotify_lifecycle.storage.dynamo import DynamoDBClient
from spotify_lifecycle.storage.s3 import S3Client


def build_dashboard_data(
    dynamo_client: DynamoDBClient,
    s3_client: S3Client,
    hot_table_name: str,
    dashboard_bucket_name: str,
    lookback_days: int = 30,
) -> dict:
    """Build precomputed dashboard data.

    Args:
        dynamo_client: DynamoDB client
        s3_client: S3 client
        hot_table_name: DynamoDB table with play events
        dashboard_bucket_name: S3 bucket for dashboard data
        lookback_days: Number of days to include in aggregates

    Returns:
        Dashboard data dictionary
    """
    # Query recent plays
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)

    plays = dynamo_client.query_plays_by_date_range(
        hot_table_name,
        start_date.isoformat(),
        end_date.isoformat(),
    )

    # Build aggregates
    track_counts = {}
    for play in plays:
        track_id = play["track_id"]
        track_counts[track_id] = track_counts.get(track_id, 0) + 1

    # Sort by count
    top_tracks = sorted(track_counts.items(), key=lambda x: x[1], reverse=True)[:20]

    dashboard_data = {
        "generated_at": datetime.now().isoformat(),
        "lookback_days": lookback_days,
        "total_plays": len(plays),
        "unique_tracks": len(track_counts),
        "top_tracks": [{"track_id": tid, "play_count": count} for tid, count in top_tracks],
    }

    # Write to S3
    s3_client.write_dashboard_data(dashboard_bucket_name, dashboard_data)

    return dashboard_data
