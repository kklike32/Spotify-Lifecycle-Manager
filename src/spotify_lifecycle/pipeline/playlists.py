"""DJ stage: Creates weekly curated playlists.

This module implements the weekly playlist automation feature (the "DJ").
It creates playlists of unheard tracks by:
1. Fetching tracks from a source playlist (e.g., Liked Songs)
2. Filtering out tracks played in last N days (lookback window)
3. Creating a new playlist with the remaining tracks (idempotent)

Key Design Principles:
- Idempotent: Safe to retry, creates playlist exactly once per week
- Auditable: Logs track counts at each step for debugging
- Cost-aware: Queries hot store only (bounded by TTL)
- Simple set-diff: Source tracks - recently played tracks

See: copilot/docs/logic/PLAYLIST_RULES.md for selection rules
See: copilot/docs/runbooks/PLAYLISTS.md for operations guide
"""

import logging
from datetime import datetime, timezone

from spotify_lifecycle.models import PlaylistState
from spotify_lifecycle.spotify.client import SpotifyClient
from spotify_lifecycle.storage.dynamo import DynamoDBClient
from spotify_lifecycle.utils.hashing import make_playlist_state_key, make_week_id

logger = logging.getLogger(__name__)


def create_weekly_playlist(
    spotify_client: SpotifyClient,
    dynamo_client: DynamoDBClient,
    source_playlist_id: str,
    lookback_days: int = 7,
    hot_table_name: str = "spotify-play-events",
    state_table_name: str = "spotify-state",
    user_id: str = "me",
) -> dict:
    """Create a new weekly playlist with tracks not played recently.

    This is the main entry point for weekly playlist creation. It:
    1. Checks if playlist for this week already exists (idempotency)
    2. Fetches all tracks from source playlist (paginated)
    3. Queries recently played tracks from hot store (lookback window)
    4. Computes set difference: source - recent
    5. Creates new playlist with naming convention: "Weekly Unheard — YYYY-WXX"
    6. Adds candidate tracks to new playlist
    7. Records playlist state to prevent duplicates on retry

    Args:
        spotify_client: Authenticated Spotify client
        dynamo_client: DynamoDB client
        source_playlist_id: Playlist ID to source tracks from (e.g., Liked Songs)
        lookback_days: Only include tracks not played in last N days (default: 7)
        hot_table_name: DynamoDB hot store table name
        state_table_name: DynamoDB state table name for idempotency
        user_id: Spotify user ID (default: 'me')

    Returns:
        Summary of playlist creation:
        {
            "playlist_id": "spotify:playlist:xyz" or None if already exists,
            "playlist_name": "Weekly Unheard — 2025-W52",
            "week_id": "2025-W52",
            "tracks_added": 42,
            "created_at": "2025-12-28T10:30:00Z",
            "source_count": 150,
            "recent_count": 108,
            "candidate_count": 42,
            "already_exists": False
        }

    Raises:
        RuntimeError: If Spotify API calls fail
        ValueError: If source playlist is empty or invalid

    Notes:
        - Idempotent: Safe to call multiple times per week (creates playlist once)
        - Auditable: Logs counts at each step (source, recent, candidates)
        - Cost: 1 scan (hot store) + 1 read (state) + 1 write (state) per run
        - Empty playlists: If all tracks recently played, creates empty playlist
    """
    # Step 1: Check if weekly playlist already exists (idempotency)
    now = datetime.now(timezone.utc)
    week_id = make_week_id(now)
    state_key = make_playlist_state_key(week_id)

    existing_state = dynamo_client.get_playlist_state(state_table_name, state_key)
    if existing_state:
        logger.info(
            "Weekly playlist already exists (idempotent skip)",
            extra={
                "week_id": week_id,
                "playlist_id": existing_state.playlist_id,
                "track_count": existing_state.track_count,
            },
        )
        return {
            "playlist_id": existing_state.playlist_id,
            "playlist_name": f"Weekly Unheard — {week_id}",
            "week_id": week_id,
            "tracks_added": existing_state.track_count,
            "created_at": existing_state.created_at.isoformat(),
            "source_count": 0,  # Not computed on retry
            "recent_count": 0,
            "candidate_count": existing_state.track_count,
            "already_exists": True,
        }

    # Step 2: Fetch source playlist tracks (paginated)
    logger.info("Fetching source playlist", extra={"source_playlist_id": source_playlist_id})
    source_track_ids = spotify_client.get_playlist_tracks(source_playlist_id)

    if not source_track_ids:
        raise ValueError(f"Source playlist is empty: {source_playlist_id}")

    logger.info("Source playlist fetched", extra={"track_count": len(source_track_ids)})

    # Step 3: Query recently played tracks from hot store
    logger.info("Querying recently played tracks", extra={"lookback_days": lookback_days})
    recent_track_ids = dynamo_client.get_recently_played_track_ids(hot_table_name, lookback_days)
    logger.info("Recent plays fetched", extra={"recent_count": len(recent_track_ids)})

    # Step 4: Compute set difference (source - recent)
    candidate_track_ids = _compute_candidates(source_track_ids, recent_track_ids)
    logger.info("Candidates computed", extra={"candidate_count": len(candidate_track_ids)})

    # Step 5: Create new playlist with naming convention
    playlist_name = f"Weekly Unheard — {week_id}"
    playlist_description = (
        f"Tracks from source playlist not played in last {lookback_days} days. "
        f"Generated on {now.strftime('%Y-%m-%d')}."
    )

    logger.info("Creating playlist", extra={"name": playlist_name})
    playlist = spotify_client.create_playlist(
        user_id=user_id,
        name=playlist_name,
        description=playlist_description,
        public=False,  # Private by default
    )
    playlist_id = playlist["uri"]

    # Step 6: Add candidate tracks to playlist
    if candidate_track_ids:
        logger.info("Adding tracks to playlist", extra={"track_count": len(candidate_track_ids)})
        spotify_client.add_tracks_to_playlist(playlist_id, candidate_track_ids)
    else:
        logger.warning(
            "No candidate tracks (all recently played)",
            extra={"source_count": len(source_track_ids), "recent_count": len(recent_track_ids)},
        )

    # Step 7: Record playlist state (idempotency)
    playlist_state = PlaylistState(
        state_key=state_key,
        week_id=week_id,
        playlist_id=playlist_id,
        created_at=now,
        track_count=len(candidate_track_ids),
        source_playlist_id=source_playlist_id,
    )

    state_written = dynamo_client.write_playlist_state(state_table_name, playlist_state)
    if not state_written:
        # Race condition: another process created playlist concurrently
        logger.warning(
            "Playlist state already exists (race condition)",
            extra={"week_id": week_id, "playlist_id": playlist_id},
        )

    logger.info(
        "Weekly playlist created",
        extra={
            "playlist_id": playlist_id,
            "week_id": week_id,
            "tracks_added": len(candidate_track_ids),
        },
    )

    return {
        "playlist_id": playlist_id,
        "playlist_name": playlist_name,
        "week_id": week_id,
        "tracks_added": len(candidate_track_ids),
        "created_at": now.isoformat(),
        "source_count": len(source_track_ids),
        "recent_count": len(recent_track_ids),
        "candidate_count": len(candidate_track_ids),
        "already_exists": False,
    }


def _compute_candidates(source_track_ids: list[str], recent_track_ids: set[str]) -> list[str]:
    """Compute candidate tracks (set difference: source - recent).

    Args:
        source_track_ids: All tracks from source playlist
        recent_track_ids: Set of recently played track IDs

    Returns:
        List of candidate track IDs (preserves source playlist order)

    Notes:
        - Preserves order from source playlist (no shuffling)
        - Deduplicates tracks within source playlist (set conversion)
        - Returns empty list if all tracks recently played
    """
    # Convert source to set for efficient lookup
    source_set = set(source_track_ids)

    # Compute difference
    candidates_set = source_set - recent_track_ids

    # Preserve order from source playlist (filter source list)
    candidates = [tid for tid in source_track_ids if tid in candidates_set]

    return candidates
