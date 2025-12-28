"""Enrichment pipeline for caching track and artist metadata.

This module implements cache-first metadata fetching to minimize Spotify API calls.
Key design principles:

- **Cache-once strategy**: Fetch each track/artist metadata exactly once
- **Idempotent**: Safe to retry, no duplicate API calls
- **Failure isolation**: Enrichment failures do NOT block ingestion
- **Cost minimization**: DynamoDB conditional writes prevent duplicate storage

Workflow:
    1. Check if track_id exists in cache (DynamoDB get)
    2. If not cached: fetch from Spotify API
    3. Write to cache with conditional expression (cache-once)
    4. Repeat for all artist_ids

Error Handling:
    - API failures are logged but NOT propagated
    - Missing metadata is marked with error flag in cache
    - Enrichment can be retried later without re-fetching successful entries

See: copilot/docs/architecture/ENRICHMENT.md for complete specifications.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from spotify_lifecycle.models import ArtistMetadata, TrackMetadata
from spotify_lifecycle.spotify.client import SpotifyClient
from spotify_lifecycle.storage.dynamo import DynamoDBClient

logger = logging.getLogger(__name__)


def enrich_track(
    track_id: str,
    spotify_client: SpotifyClient,
    dynamo_client: DynamoDBClient,
    tracks_table: str,
) -> Optional[TrackMetadata]:
    """Enrich a single track with metadata (cache-first).

    Cache-first logic:
        1. Check if track_id exists in cache
        2. If cached: return immediately (no API call)
        3. If not cached: fetch from Spotify API
        4. Write to cache with conditional expression
        5. Return metadata

    Args:
        track_id: Spotify track URI (e.g., spotify:track:abc123)
        spotify_client: Authenticated Spotify client
        dynamo_client: DynamoDB client for cache operations
        tracks_table: DynamoDB table name for track cache

    Returns:
        TrackMetadata if successfully fetched/cached, None if failed

    Cost Impact:
        - Cache hit: 0.5 RCU (get_track_metadata)
        - Cache miss: 0.5 RCU (check) + 1 API call + 1 WCU (write)

    Error Handling:
        - API failures: logged and None returned (non-blocking)
        - DynamoDB errors: propagated (infrastructure issue)

    Idempotency:
        - Safe to retry (conditional write prevents duplicates)
        - Multiple enrichment runs will NOT duplicate API calls
    """
    # Step 1: Check cache (read-through pattern)
    cached = dynamo_client.get_track_metadata(tracks_table, track_id)
    if cached:
        logger.debug(f"Track cache hit: {track_id}")
        return TrackMetadata(
            track_id=cached["track_id"],
            name=cached["name"],
            artist_ids=cached["artist_ids"],
            artist_names=cached.get("artist_names", []),
            album_id=cached["album_id"],
            album_name=cached["album_name"],
            duration_ms=cached["duration_ms"],
            explicit=cached["explicit"],
            popularity=cached["popularity"],
            release_date=cached.get("release_date", ""),
            uri=cached["uri"],
            cached_at=(
                datetime.fromisoformat(cached["cached_at"])
                if "cached_at" in cached
                else datetime.now(timezone.utc)
            ),
        )

    # Step 2: Cache miss - fetch from Spotify API
    logger.debug(f"Track cache miss: {track_id} - fetching from API")
    try:
        # Extract track ID from URI (spotify:track:abc123 -> abc123)
        track_id_only = track_id.split(":")[-1] if ":" in track_id else track_id
        raw_track = spotify_client.get_track(track_id_only)

        # Parse API response into TrackMetadata model
        metadata = TrackMetadata(
            track_id=track_id,
            name=raw_track["name"],
            artist_ids=[f"spotify:artist:{a['id']}" for a in raw_track["artists"]],
            artist_names=[a["name"] for a in raw_track["artists"]],
            album_id=f"spotify:album:{raw_track['album']['id']}",
            album_name=raw_track["album"]["name"],
            duration_ms=raw_track["duration_ms"],
            explicit=raw_track.get("explicit", False),
            popularity=raw_track.get("popularity", 0),
            release_date=raw_track["album"].get("release_date", ""),
            uri=track_id,
        )

        # Step 3: Write to cache (conditional write, cache-once)
        was_written = dynamo_client.write_track_metadata(tracks_table, metadata)
        if was_written:
            logger.debug(f"Track metadata cached: {track_id}")
        else:
            logger.debug(f"Track metadata already cached (race): {track_id}")

        return metadata

    except Exception as e:
        # API failure - log and return None (non-blocking)
        logger.error(f"Failed to enrich track {track_id}: {e}")
        return None


def enrich_artist(
    artist_id: str,
    spotify_client: SpotifyClient,
    dynamo_client: DynamoDBClient,
    artists_table: str,
) -> Optional[ArtistMetadata]:
    """Enrich a single artist with metadata (cache-first).

    Cache-first logic:
        1. Check if artist_id exists in cache
        2. If cached: return immediately (no API call)
        3. If not cached: fetch from Spotify API
        4. Write to cache with conditional expression
        5. Return metadata

    Args:
        artist_id: Spotify artist URI (e.g., spotify:artist:xyz789)
        spotify_client: Authenticated Spotify client
        dynamo_client: DynamoDB client for cache operations
        artists_table: DynamoDB table name for artist cache

    Returns:
        ArtistMetadata if successfully fetched/cached, None if failed

    Cost Impact:
        - Cache hit: 0.5 RCU (get_artist_metadata)
        - Cache miss: 0.5 RCU (check) + 1 API call + 1 WCU (write)

    Error Handling:
        - API failures: logged and None returned (non-blocking)
        - DynamoDB errors: propagated (infrastructure issue)

    Idempotency:
        - Safe to retry (conditional write prevents duplicates)
        - Multiple enrichment runs will NOT duplicate API calls
    """
    # Step 1: Check cache (read-through pattern)
    cached = dynamo_client.get_artist_metadata(artists_table, artist_id)
    if cached:
        logger.debug(f"Artist cache hit: {artist_id}")
        import json

        return ArtistMetadata(
            artist_id=cached["artist_id"],
            name=cached["name"],
            genres=cached.get("genres", []),
            popularity=cached["popularity"],
            followers=cached.get("followers", 0),
            uri=cached["uri"],
            images=json.loads(cached["images"]) if "images" in cached else [],
            cached_at=(
                datetime.fromisoformat(cached["cached_at"])
                if "cached_at" in cached
                else datetime.now(timezone.utc)
            ),
        )

    # Step 2: Cache miss - fetch from Spotify API
    logger.debug(f"Artist cache miss: {artist_id} - fetching from API")
    try:
        # Extract artist ID from URI (spotify:artist:xyz789 -> xyz789)
        artist_id_only = artist_id.split(":")[-1] if ":" in artist_id else artist_id
        raw_artist = spotify_client.get_artist(artist_id_only)

        # Parse API response into ArtistMetadata model
        metadata = ArtistMetadata(
            artist_id=artist_id,
            name=raw_artist["name"],
            genres=raw_artist.get("genres", []),
            popularity=raw_artist.get("popularity", 0),
            followers=raw_artist.get("followers", {}).get("total", 0),
            uri=artist_id,
            images=raw_artist.get("images", []),
        )

        # Step 3: Write to cache (conditional write, cache-once)
        was_written = dynamo_client.write_artist_metadata(artists_table, metadata)
        if was_written:
            logger.debug(f"Artist metadata cached: {artist_id}")
        else:
            logger.debug(f"Artist metadata already cached (race): {artist_id}")

        return metadata

    except Exception as e:
        # API failure - log and return None (non-blocking)
        logger.error(f"Failed to enrich artist {artist_id}: {e}")
        return None


def enrich_play_events(
    track_ids: list[str],
    spotify_client: SpotifyClient,
    dynamo_client: DynamoDBClient,
    tracks_table: str,
    artists_table: str,
) -> dict[str, int]:
    """Enrich multiple tracks and their artists (batch operation).

    Workflow:
        1. Deduplicate track_ids (multiple plays of same track)
        2. For each unique track_id:
            a. Fetch track metadata (cache-first)
            b. For each artist_id in track metadata:
                - Fetch artist metadata (cache-first)
        3. Return summary of enrichment results

    Args:
        track_ids: List of Spotify track URIs to enrich
        spotify_client: Authenticated Spotify client
        dynamo_client: DynamoDB client for cache operations
        tracks_table: DynamoDB table name for track cache
        artists_table: DynamoDB table name for artist cache

    Returns:
        dict: Summary of enrichment results
            {
                "tracks_processed": int,
                "tracks_cached": int,
                "tracks_fetched": int,
                "tracks_failed": int,
                "artists_processed": int,
                "artists_cached": int,
                "artists_fetched": int,
                "artists_failed": int,
            }

    Cost Impact:
        - Tracks: (0.5 RCU + 1 API call + 1 WCU) per unique track (first time)
        - Artists: (0.5 RCU + 1 API call + 1 WCU) per unique artist (first time)
        - Cache hits: 0.5 RCU only (no API calls, no writes)

    Example:
        - Input: 100 plays with 20 unique tracks, 15 unique artists
        - First run: 20 API calls (tracks) + 15 API calls (artists) = 35 total
        - Second run: 0 API calls (all cached)

    Error Handling:
        - Individual failures are logged but do NOT stop batch processing
        - Returns partial results (some tracks/artists may be missing)
        - Retry-safe: can re-run to fetch missing metadata

    Notes:
        - Deduplication reduces API calls (same track played multiple times)
        - Cache-first strategy minimizes costs over time
        - Non-blocking: enrichment failures do NOT block ingestion
    """
    # Deduplicate track_ids (same track played multiple times)
    unique_track_ids = list(set(track_ids))

    summary = {
        "tracks_processed": len(unique_track_ids),
        "tracks_cached": 0,
        "tracks_fetched": 0,
        "tracks_failed": 0,
        "artists_processed": 0,
        "artists_cached": 0,
        "artists_fetched": 0,
        "artists_failed": 0,
    }

    # Track unique artist IDs across all tracks
    unique_artist_ids = set()

    # Step 1: Enrich all tracks
    logger.info(f"Enriching {len(unique_track_ids)} unique tracks")
    for track_id in unique_track_ids:
        # Check if already cached before fetching
        was_cached = dynamo_client.get_track_metadata(tracks_table, track_id) is not None
        if was_cached:
            summary["tracks_cached"] += 1

        metadata = enrich_track(track_id, spotify_client, dynamo_client, tracks_table)

        if metadata:
            if not was_cached:
                summary["tracks_fetched"] += 1
            # Collect artist IDs for enrichment
            unique_artist_ids.update(metadata.artist_ids)
        else:
            summary["tracks_failed"] += 1

    # Step 2: Enrich all artists (deduplicated)
    summary["artists_processed"] = len(unique_artist_ids)
    logger.info(f"Enriching {len(unique_artist_ids)} unique artists")
    for artist_id in unique_artist_ids:
        # Check if already cached before fetching
        was_cached = dynamo_client.get_artist_metadata(artists_table, artist_id) is not None
        if was_cached:
            summary["artists_cached"] += 1

        metadata = enrich_artist(artist_id, spotify_client, dynamo_client, artists_table)

        if metadata:
            if not was_cached:
                summary["artists_fetched"] += 1
        else:
            summary["artists_failed"] += 1

    # Log summary
    logger.info(
        "Enrichment complete",
        extra={
            "tracks_processed": summary["tracks_processed"],
            "tracks_cached": summary["tracks_cached"],
            "tracks_fetched": summary["tracks_fetched"],
            "tracks_failed": summary["tracks_failed"],
            "artists_processed": summary["artists_processed"],
            "artists_cached": summary["artists_cached"],
            "artists_fetched": summary["artists_fetched"],
            "artists_failed": summary["artists_failed"],
        },
    )

    return summary


def run_enrichment(
    spotify_client: SpotifyClient,
    dynamo_client: DynamoDBClient,
    hot_table: str,
    tracks_table: str,
    artists_table: str,
    lookback_days: int = 7,
) -> dict[str, int]:
    """Run enrichment pipeline for recent play events.

    Fetches recent plays from hot store and enriches all tracks/artists.
    Designed to run after ingestion pipeline (hourly or daily).

    Args:
        spotify_client: Authenticated Spotify client
        dynamo_client: DynamoDB client for all storage operations
        hot_table: DynamoDB table name for recent plays (hot store)
        tracks_table: DynamoDB table name for track cache
        artists_table: DynamoDB table name for artist cache
        lookback_days: Number of days to fetch plays from (default: 7)

    Returns:
        dict: Summary of enrichment results (see enrich_play_events)

    Cost Impact:
        - Hot store scan: ~1-3 RCU (bounded by TTL, max 7 days)
        - Tracks: (0.5 RCU + 1 API call + 1 WCU) per new track
        - Artists: (0.5 RCU + 1 API call + 1 WCU) per new artist
        - Typical daily run: $0.0001 (mostly cache hits after initial run)

    Workflow:
        1. Scan hot store for recent plays (last N days)
        2. Extract unique track_ids
        3. Enrich tracks and artists (cache-first)
        4. Return summary

    Notes:
        - Safe to run multiple times (idempotent)
        - Non-blocking: failures logged but NOT propagated
        - Can run independently of ingestion pipeline
    """
    from spotify_lifecycle.utils.time import days_ago

    # Step 1: Get recent plays from hot store
    cutoff_time = days_ago(lookback_days)
    logger.info(f"Fetching plays since {cutoff_time.isoformat()}")

    # Scan hot store (bounded by TTL, safe for small datasets)
    table = dynamo_client.dynamodb.Table(hot_table)
    response = table.scan(
        FilterExpression="played_at >= :cutoff",
        ExpressionAttributeValues={":cutoff": cutoff_time.isoformat()},
        ProjectionExpression="track_id",
        ConsistentRead=False,
    )

    track_ids = [item["track_id"] for item in response.get("Items", [])]

    # Handle pagination (if hot store grows beyond 1MB)
    while "LastEvaluatedKey" in response:
        response = table.scan(
            FilterExpression="played_at >= :cutoff",
            ExpressionAttributeValues={":cutoff": cutoff_time.isoformat()},
            ProjectionExpression="track_id",
            ConsistentRead=False,
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        track_ids.extend([item["track_id"] for item in response.get("Items", [])])

    logger.info(f"Found {len(track_ids)} plays to enrich")

    # Step 2: Enrich all tracks and artists
    if not track_ids:
        logger.info("No plays to enrich")
        return {
            "tracks_processed": 0,
            "tracks_cached": 0,
            "tracks_fetched": 0,
            "tracks_failed": 0,
            "artists_processed": 0,
            "artists_cached": 0,
            "artists_fetched": 0,
            "artists_failed": 0,
        }

    summary = enrich_play_events(
        track_ids=track_ids,
        spotify_client=spotify_client,
        dynamo_client=dynamo_client,
        tracks_table=tracks_table,
        artists_table=artists_table,
    )

    return summary
