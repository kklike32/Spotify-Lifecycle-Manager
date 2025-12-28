"""Recorder stage: Ingests Spotify play history."""

import hashlib
from datetime import datetime
from typing import Optional

from spotify_lifecycle.models import PlayEvent
from spotify_lifecycle.spotify.client import SpotifyClient
from spotify_lifecycle.storage.dynamo import DynamoDBClient
from spotify_lifecycle.storage.s3 import S3ColdStore


def compute_dedup_key(track_id: str, played_at: datetime) -> str:
    """Compute stable dedup key for a play event.

    Args:
        track_id: Spotify track ID
        played_at: When track was played

    Returns:
        Hex digest of key components
    """
    key_str = f"{track_id}#{played_at.isoformat()}"
    return hashlib.sha256(key_str.encode()).hexdigest()


def ingest_recently_played(
    spotify_client: SpotifyClient,
    dynamo_client: DynamoDBClient,
    s3_client: S3ColdStore,
    hot_table_name: str,
    raw_bucket_name: str,
    limit: int = 50,
    before: Optional[int] = None,
) -> dict:
    """Ingest recently played tracks from Spotify.

    Args:
        spotify_client: Authenticated Spotify client
        dynamo_client: DynamoDB client
        s3_client: S3 client
        hot_table_name: DynamoDB table for recent plays
        raw_bucket_name: S3 bucket for raw events
        limit: Number of tracks to fetch
        before: Timestamp in milliseconds for pagination

    Returns:
        Summary of ingestion (counts, timestamps)
    """
    # Fetch from Spotify
    response = spotify_client.get_recently_played(limit=limit, before=before)

    events = []
    for item in response.get("items", []):
        track = item["track"]
        played_at = datetime.fromisoformat(item["played_at"].replace("Z", "+00:00"))

        event = PlayEvent(
            track_id=track["id"],
            played_at=played_at,
            user_id=spotify_client.sp.current_user()["id"],
            context=item.get("context", {}).get("uri"),
        )

        dedup_key = compute_dedup_key(event.track_id, event.played_at)

        # Write to hot store
        dynamo_client.write_play_event(hot_table_name, event, dedup_key)

        # Collect for raw store
        events.append(
            {
                "dedup_key": dedup_key,
                "track_id": event.track_id,
                "played_at": event.played_at.isoformat(),
                "user_id": event.user_id,
                "context": event.context,
            }
        )

    # Write to cold store (partitioned by date)
    if events:
        date = datetime.now()
        s3_client.write_raw_events(raw_bucket_name, date, events)

    return {
        "items_fetched": len(response.get("items", [])),
        "items_written": len(events),
        "cursor": response.get("cursors", {}).get("before"),
    }
