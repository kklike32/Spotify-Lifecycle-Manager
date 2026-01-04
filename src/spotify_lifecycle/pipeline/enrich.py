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


# =============================================================================
# LOCAL FILE SUPPORT (DISABLED - FUTURE USE)
# =============================================================================
# NOTE: Spotify's recently_played() API does NOT return local files.
# Per Spotify docs: "Local files aren't returned from Spotify's Web API."
# These functions are preserved for future use when playlist-based ingestion
# is implemented to detect and track local files (e.g., Juice WRLD unreleased).
#
# To enable local file tracking:
# 1. Implement playlist endpoint scanning in ingest.py
# 2. Uncomment _parse_local_file_metadata() and _send_unreleased_track_alert()
# 3. Uncomment local file check in enrich_track() (line ~273)
# 4. Deploy SNS infrastructure (infra/terraform/sns.tf)
# =============================================================================


# def _parse_local_file_metadata(track_uri: str) -> TrackMetadata:
#     """Parse metadata from local file URI.
#
#     Local file URIs follow format: spotify:local:artist:album:track:duration
#     All fields are URL-encoded (+ for spaces, %XX for special chars).
#     Empty strings indicate missing metadata (not null).
#
#     Per Spotify docs: https://developer.spotify.com/documentation/web-api/concepts/playlists#local-files
#     Example: spotify:local:Juice+WRLD::Eye+Contact:249
#
#     Args:
#         track_uri: Local file URI (spotify:local:...)
#
#     Returns:
#         TrackMetadata with parsed fields
#     """
#     from urllib.parse import unquote_plus
#
#     # Split URI - format: spotify:local:artist:album:track:duration
#     parts = track_uri.split(":")
#
#     # Extract duration (last field, always numeric in seconds)
#     duration_ms = 0
#     if len(parts) > 2 and parts[-1].isdigit():
#         duration_ms = int(parts[-1]) * 1000  # Convert seconds to ms
#         parts = parts[:-1]  # Remove duration
#
#     # Parse artist:album:track from remaining parts
#     # Format: ['spotify', 'local', artist, album, track]
#     if len(parts) >= 5:
#         # Standard format with all fields
#         artist_name = unquote_plus(parts[2]) if parts[2] else "Unknown Artist"
#         album_name = unquote_plus(parts[3]) if parts[3] else "Local Files"
#         track_name = unquote_plus(":".join(parts[4:])) if any(parts[4:]) else "Unknown Track"
#     elif len(parts) >= 3:
#         # Fallback: join everything after 'local' and split
#         remaining = ":".join(parts[2:])
#         remaining_parts = remaining.split(":")
#         artist_name = unquote_plus(remaining_parts[0]) if remaining_parts[0] else "Unknown Artist"
#         album_name = (
#             unquote_plus(remaining_parts[1])
#             if len(remaining_parts) > 1 and remaining_parts[1]
#             else "Local Files"
#         )
#         track_name = (
#             unquote_plus(":".join(remaining_parts[2:]))
#             if len(remaining_parts) > 2 and any(remaining_parts[2:])
#             else "Unknown Track"
#         )
#     else:
#         artist_name = "Unknown Artist"
#         album_name = "Local Files"
#         track_name = "Unknown Track"
#
#     logger.info(
#         f"Parsed local file: '{track_name}' by '{artist_name}' from '{album_name}'",
#         extra={
#             "track_uri": track_uri,
#             "parsed_artist": artist_name,
#             "parsed_track": track_name,
#             "parsed_album": album_name,
#             "duration_ms": duration_ms,
#         },
#     )
#
#     # Check if this is an unreleased Juice WRLD track
#     unreleased_tracks = [
#         "Alkaline",
#         "Autograph (On My Line)",
#         "Bottle",
#         "Confide",
#         "Eye Contact",
#         "Let Her Leave",
#         "London Tipton",
#         "Lost Her",
#         "Moonlight",
#         "My Fault",
#         "No Love No Trust",
#         "Old Me",
#         "Paranoid",
#         "Rainbow",
#         "Soda Pop",
#         "Worth It",
#         "You Don't Love Me",
#     ]
#
#     if "Juice WRLD" in artist_name or "juice wrld" in artist_name.lower():
#         # Check if track name matches any unreleased track
#         for unreleased in unreleased_tracks:
#             if unreleased.lower() in track_name.lower():
#                 logger.info(
#                     f"UNRELEASED TRACK DETECTED: {track_name} by {artist_name}",
#                     extra={
#                         "track_uri": track_uri,
#                         "track_name": track_name,
#                         "artist": artist_name,
#                     },
#                 )
#                 _send_unreleased_track_alert(track_name, artist_name, track_uri)
#                 break
#
#     return TrackMetadata(
#         track_id=track_uri,
#         name=track_name,
#         artist_ids=["spotify:local:artist"],  # Local files don't have real artist IDs
#         artist_names=[artist_name],
#         album_id="spotify:local:album",  # Local files don't have real album IDs
#         album_name=album_name,
#         duration_ms=duration_ms,
#         explicit=False,  # Can't determine from URI
#         popularity=0,  # Local files have no popularity
#         release_date="",  # Can't determine from URI
#         uri=track_uri,
#     )


# def _send_unreleased_track_alert(track_name: str, artist_name: str, track_uri: str) -> None:
#     """Send SNS alert for unreleased track detection.
#
#     Args:
#         track_name: Name of the detected unreleased track
#         artist_name: Artist name
#         track_uri: Full Spotify local URI
#     """
#     import os
#     import boto3
#
#     sns_topic_arn = os.getenv("UNRELEASED_TRACKS_SNS_TOPIC")
#     if not sns_topic_arn:
#         logger.warning(
#             "UNRELEASED_TRACKS_SNS_TOPIC not configured, skipping alert"
#         )
#         return
#
#     try:
#         sns = boto3.client("sns")
#         message = f"""🎵 Unreleased Track Detected!
#
# Track: {track_name}
# Artist: {artist_name}
# URI: {track_uri}
#
# This unreleased Juice WRLD track was just played and logged.
#
# Timestamp: {datetime.now(timezone.utc).isoformat()}
# """
#
#         sns.publish(
#             TopicArn=sns_topic_arn,
#             Subject=f"🎵 Unreleased Track: {track_name}",
#             Message=message,
#         )
#         logger.info(f"SNS alert sent for unreleased track: {track_name}")
#     except Exception as e:
#         logger.error(f"Failed to send SNS alert: {e}")


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
        # Handle corrupted cache data (empty artist_names)
        artist_names = cached.get("artist_names", [])
        artist_ids = cached.get("artist_ids", [])

        if not artist_names:
            logger.error(
                f"CORRUPTED CACHE: Track {track_id} has empty artist_names",
                extra={"track_id": track_id, "cached_data": cached},
            )
            # Use track name as identifier
            artist_names = [f"[Cached - No Artist - {cached.get('name', 'Unknown')}]"]
            artist_ids = ["spotify:artist:unknown"]
        elif not artist_ids:
            # artist_names exists but artist_ids missing
            artist_ids = ["spotify:artist:unknown"] * len(artist_names)
        elif len(artist_ids) != len(artist_names):
            # Mismatch in lengths - pad to match
            logger.warning(f"artist_ids/names length mismatch for {track_id}")
            if len(artist_ids) < len(artist_names):
                artist_ids += ["spotify:artist:unknown"] * (len(artist_names) - len(artist_ids))
            else:
                artist_names += ["Unknown Artist"] * (len(artist_ids) - len(artist_names))

        return TrackMetadata(
            track_id=cached["track_id"],
            name=cached["name"],
            artist_ids=artist_ids,
            artist_names=artist_names,
            album_id=cached.get("album_id", "spotify:album:unknown"),
            album_name=cached.get("album_name", "Unknown Album"),
            duration_ms=cached.get("duration_ms", 0),
            explicit=cached.get("explicit", False),
            popularity=cached.get("popularity", 0),
            release_date=cached.get("release_date", ""),
            uri=cached.get("uri", track_id),
            cached_at=(
                datetime.fromisoformat(cached["cached_at"])
                if "cached_at" in cached
                else datetime.now(timezone.utc)
            ),
        )

    # Step 2: Cache miss - fetch from Spotify API
    logger.debug(f"Track cache miss: {track_id} - fetching from API")

    # NOTE: Local file support disabled (recently_played API doesn't return local files)
    # If you see spotify:local: URIs here, they came from playlist endpoints (future use)
    # if track_id.startswith("spotify:local:"):
    #     logger.info(f"Local file detected: {track_id}")
    #     return _parse_local_file_metadata(track_id)

    try:
        # Extract track ID from URI (spotify:track:abc123 -> abc123)
        track_id_only = track_id.split(":")[-1] if ":" in track_id else track_id
        raw_track = spotify_client.get_track(track_id_only)

        # Handle edge case: tracks with no artists (rare, but happens)
        artist_ids = [f"spotify:artist:{a['id']}" for a in raw_track["artists"]]
        artist_names = [a["name"] for a in raw_track["artists"]]

        # Spotify tracks should always have artists - if empty, this is abnormal
        # Could be: podcast episode, unavailable track, deleted track, or API issue
        if not artist_names:
            logger.error(
                f"Track {track_id} has NO ARTISTS - Investigate!",
                extra={
                    "track_id": track_id,
                    "track_name": raw_track.get("name", "UNKNOWN"),
                    "track_type": raw_track.get("type", "UNKNOWN"),
                    "is_playable": raw_track.get("is_playable", "UNKNOWN"),
                    "available_markets": len(raw_track.get("available_markets", [])),
                    "album_name": raw_track.get("album", {}).get("name", "UNKNOWN"),
                    "raw_artists_field": raw_track.get("artists", []),
                },
            )
            # Use track name as identifier since we can't rely on artist
            artist_names = [f"[No Artist - {raw_track.get('name', 'Unknown Track')}]"]
            artist_ids = ["spotify:artist:unknown"]

        # Parse API response into TrackMetadata model
        metadata = TrackMetadata(
            track_id=track_id,
            name=raw_track["name"],
            artist_ids=artist_ids,
            artist_names=artist_names,
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
