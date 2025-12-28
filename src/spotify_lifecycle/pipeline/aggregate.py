"""Analyst stage: Builds analytics and aggregates.

This module computes comprehensive analytics from play history and generates
a single dashboard_data.json artifact. All analytics are precomputed to avoid
live queries in the browser (zero query cost).

Design Principles:
- Compute once, read many times (S3 static serving)
- No unbounded queries (explicit limits on Top-N)
- Schema-validated output (Pydantic DashboardData model)
- Idempotent: safe to retry (same input → same output)

Cost Model:
- Compute: 1-2 seconds Lambda execution (nightly)
- Storage: ~100KB JSON file in S3 (negligible)
- Reads: Free via S3 static website or CloudFront
"""

from collections import defaultdict
from datetime import datetime, timedelta, timezone

from spotify_lifecycle.models import DashboardData
from spotify_lifecycle.storage.dynamo import DynamoDBClient
from spotify_lifecycle.storage.s3 import S3DashboardStore


def build_dashboard_data(
    dynamo_client: DynamoDBClient,
    s3_client: S3DashboardStore,
    hot_table_name: str,
    tracks_table_name: str,
    artists_table_name: str,
    dashboard_bucket_name: str,
    lookback_days: int = 90,
) -> DashboardData:
    """Build precomputed dashboard data with full analytics.

    This function performs all analytics computation in a single pass:
    1. Query play events from DynamoDB hot storage
    2. Enrich with cached track/artist metadata
    3. Compute top tracks, artists, genres
    4. Build daily trends and hourly distribution
    5. Validate output with Pydantic model
    6. Write to S3 as JSON

    Args:
        dynamo_client: DynamoDB client
        s3_client: S3 dashboard store client
        hot_table_name: DynamoDB table with play events
        tracks_table_name: DynamoDB table with track metadata
        artists_table_name: DynamoDB table with artist metadata
        dashboard_bucket_name: S3 bucket for dashboard data
        lookback_days: Number of days to include in aggregates (default: 90)

    Returns:
        DashboardData: Validated dashboard data model

    Raises:
        ValueError: If output data fails schema validation

    Cost Impact:
        - DynamoDB reads: ~1-2 RCU per 1000 plays (scan with filter)
        - Lambda compute: 1-2 seconds execution (nightly)
        - S3 write: 1 PUT request (~$0.000005)
        - Total: < $0.01 per month for 10K plays/month

    Idempotency:
        Same input data always produces same output. Safe to retry.
        Output includes generation timestamp but data is deterministic.
    """
    # Query recent plays with explicit date range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=lookback_days)

    plays = dynamo_client.query_plays_by_date_range(
        hot_table_name,
        start_date.isoformat(),
        end_date.isoformat(),
    )

    # Build raw aggregates (first pass over plays)
    track_counts: dict[str, int] = defaultdict(int)
    artist_counts: dict[str, int] = defaultdict(int)
    daily_counts: dict[str, int] = defaultdict(int)
    hourly_counts: dict[int, int] = defaultdict(int)

    for play in plays:
        track_id = play["track_id"]
        played_at = datetime.fromisoformat(play["played_at"])

        # Count plays per track
        track_counts[track_id] += 1

        # Count plays per day (date only, no time)
        daily_counts[played_at.date().isoformat()] += 1

        # Count plays per hour (0-23)
        hourly_counts[played_at.hour] += 1

    # Enrich top tracks with metadata (limit to top 50)
    top_track_ids = sorted(track_counts.items(), key=lambda x: x[1], reverse=True)[:50]
    top_tracks_enriched = []

    for track_id, play_count in top_track_ids:
        metadata = dynamo_client.get_track_metadata(tracks_table_name, track_id)
        if metadata:
            # Get artist names (will be resolved after artist metadata is fetched)
            artist_ids = metadata.get("artist_ids", [])
            # Count plays per artist (from track metadata)
            for artist_id in artist_ids:
                artist_counts[artist_id] += play_count

            top_tracks_enriched.append(
                {
                    "track_id": track_id,
                    "track_name": metadata.get("name", "Unknown"),
                    "artist_ids": artist_ids,  # Will resolve to names later
                    "album_name": metadata.get("album_name", "Unknown"),
                    "play_count": play_count,
                }
            )
        else:
            # Track metadata not cached (shouldn't happen if enrichment ran)
            top_tracks_enriched.append(
                {
                    "track_id": track_id,
                    "track_name": "Unknown",
                    "artist_ids": [],
                    "album_name": "Unknown",
                    "play_count": play_count,
                }
            )

    # Enrich top artists with metadata (limit to top 50)
    top_artist_ids = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:50]
    top_artists_enriched = []
    genre_counts: dict[str, int] = defaultdict(int)

    # Build artist_id -> name mapping for track artist resolution
    artist_name_map: dict[str, str] = {}

    for artist_id, play_count in top_artist_ids:
        metadata = dynamo_client.get_artist_metadata(artists_table_name, artist_id)
        if metadata:
            artist_name = metadata.get("name", "Unknown")
            genres = metadata.get("genres", [])
            artist_name_map[artist_id] = artist_name

            top_artists_enriched.append(
                {
                    "artist_id": artist_id,
                    "artist_name": artist_name,
                    "genres": genres,
                    "play_count": play_count,
                }
            )

            # Count plays per genre (from artist metadata)
            for genre in genres:
                genre_counts[genre] += play_count
        else:
            artist_name_map[artist_id] = "Unknown"
            top_artists_enriched.append(
                {
                    "artist_id": artist_id,
                    "artist_name": "Unknown",
                    "genres": [],
                    "play_count": play_count,
                }
            )

    # Now resolve artist names in top_tracks
    for track in top_tracks_enriched:
        artist_ids = track.pop("artist_ids", [])
        # Get first artist name, or join multiple
        artist_names = [artist_name_map.get(aid, "Unknown") for aid in artist_ids]
        track["artist_name"] = ", ".join(artist_names) if artist_names else "Unknown"

    # Build top genres (limit to top 20)
    top_genres_sorted = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:20]
    top_genres = [{"genre": genre, "play_count": count} for genre, count in top_genres_sorted]

    # Build daily plays (ensure all days in range have entries)
    daily_plays = []
    current_date = start_date.date()
    while current_date <= end_date.date():
        date_str = current_date.isoformat()
        daily_plays.append({"date": date_str, "play_count": daily_counts.get(date_str, 0)})
        current_date += timedelta(days=1)

    # Build hourly distribution (ensure all hours 0-23 have entries)
    hourly_distribution = [
        {"hour": hour, "play_count": hourly_counts.get(hour, 0)} for hour in range(24)
    ]

    # Build metadata (dashboard expects this format)
    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_play_count": len(plays),
        "unique_track_count": len(track_counts),
        "unique_artist_count": len(artist_counts),
        "genre_count": len(genre_counts),
        "date_range_start": start_date.isoformat(),
        "date_range_end": end_date.isoformat(),
    }

    # Build validated dashboard data model
    dashboard_data = DashboardData(
        generated_at=datetime.now(timezone.utc),
        time_range={"start": start_date, "end": end_date},
        metadata=metadata,
        top_tracks=top_tracks_enriched,
        top_artists=top_artists_enriched,
        daily_plays=daily_plays,
        hourly_distribution=hourly_distribution,
        top_genres=top_genres,
    )

    # Write to S3 (validated JSON)
    s3_client.write_dashboard_data(dashboard_bucket_name, dashboard_data.model_dump(mode="json"))

    return dashboard_data
