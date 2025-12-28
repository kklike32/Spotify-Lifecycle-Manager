"""Hashing utilities for event identification and idempotency.

This module provides deterministic key generation for deduplication and idempotency.
All functions are pure (no side effects) and produce consistent outputs for same inputs.

See: copilot/docs/architecture/IDEMPOTENCY.md for detailed strategy.
"""

import hashlib
from datetime import datetime
from typing import Optional


def sha256_hash(data: str) -> str:
    """Compute SHA256 hash of a string.

    Args:
        data: String to hash

    Returns:
        64-character hex digest

    Examples:
        >>> sha256_hash("hello")
        '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'
    """
    return hashlib.sha256(data.encode()).hexdigest()


def make_play_id(played_at: datetime, track_id: str, context: Optional[str] = None) -> str:
    """Generate deterministic play event ID for deduplication.

    This is the core idempotency function for play events. Same inputs always
    produce the same output, enabling safe retries and overlap fetching.

    Args:
        played_at: When track was played (UTC)
        track_id: Spotify track URI
        context: Optional playback context (playlist, album, etc.)

    Returns:
        64-character hex string (SHA256)

    Examples:
        >>> from datetime import datetime
        >>> make_play_id(
        ...     played_at=datetime(2025, 12, 27, 14, 30, 0),
        ...     track_id="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
        ...     context="spotify:playlist:37i9dQZF1DXcBWIGoYBM5M"
        ... )
        'a1b2c3d4e5f6...'

    Notes:
        - Context is normalized to "none" if null/empty (consistent representation)
        - Timestamp precision: ISO format (no sub-second precision)
        - Collision resistance: SHA256 (~10^77 possible hashes)
        - If same track played twice in same minute with no context → deduplicated
          (acceptable for MVP; Spotify API doesn't guarantee sub-second precision)
    """
    # Normalize inputs
    played_at_str = played_at.isoformat()
    context_str = context if context and context.strip() else "none"

    # Canonical form (order matters for determinism)
    key_parts = [played_at_str, track_id, context_str]
    key_string = ":".join(key_parts)

    # Hash
    return sha256_hash(key_string)


def make_track_cache_key(track_id: str) -> str:
    """Generate cache key for track metadata.

    Spotify track URIs are already globally unique, so no additional hashing needed.

    Args:
        track_id: Spotify track URI

    Returns:
        Normalized track URI (same as input after validation)

    Raises:
        ValueError: If track_id is not a valid Spotify track URI

    Examples:
        >>> make_track_cache_key("spotify:track:6rqhFgbbKwnb9MLmUQDhG6")
        'spotify:track:6rqhFgbbKwnb9MLmUQDhG6'

        >>> make_track_cache_key("invalid")
        Traceback (most recent call last):
        ...
        ValueError: Invalid track ID: invalid
    """
    # Validation
    if not track_id.startswith("spotify:track:"):
        raise ValueError(f"Invalid track ID: {track_id}")

    return track_id


def make_artist_cache_key(artist_id: str) -> str:
    """Generate cache key for artist metadata.

    Spotify artist URIs are already globally unique, so no additional hashing needed.

    Args:
        artist_id: Spotify artist URI

    Returns:
        Normalized artist URI (same as input after validation)

    Raises:
        ValueError: If artist_id is not a valid Spotify artist URI

    Examples:
        >>> make_artist_cache_key("spotify:artist:0OdUWJ0sBjDrqHygGUXeCF")
        'spotify:artist:0OdUWJ0sBjDrqHygGUXeCF'

        >>> make_artist_cache_key("invalid")
        Traceback (most recent call last):
        ...
        ValueError: Invalid artist ID: invalid
    """
    # Validation
    if not artist_id.startswith("spotify:artist:"):
        raise ValueError(f"Invalid artist ID: {artist_id}")

    return artist_id


def make_week_id(date: datetime) -> str:
    """Generate ISO week identifier for weekly playlists.

    Uses ISO 8601 week date system (week starts Monday).
    Handles year transitions correctly (week 1 of year may start in previous year).

    Args:
        date: Any datetime in the target week

    Returns:
        ISO week string in format YYYY-WXX

    Examples:
        >>> make_week_id(datetime(2025, 12, 27))
        '2025-W52'

        >>> make_week_id(datetime(2025, 1, 1))
        '2025-W01'

        >>> make_week_id(datetime(2024, 12, 30))  # Monday of week 1, 2025
        '2025-W01'

    Notes:
        - Uses ISO calendar (not Gregorian)
        - Week 1 is the first week with a Thursday in the new year
        - Ensures consistent weekly playlist naming
    """
    iso_calendar = date.isocalendar()
    year = iso_calendar[0]
    week = iso_calendar[1]
    return f"{year}-W{week:02d}"


def make_playlist_state_key(week_id: str) -> str:
    """Generate state key for weekly playlist tracking.

    Args:
        week_id: ISO week identifier (YYYY-WXX)

    Returns:
        State key for DynamoDB (weekly_playlist_YYYY_WXX)

    Examples:
        >>> make_playlist_state_key("2025-W52")
        'weekly_playlist_2025_W52'
    """
    # Replace dash with underscore for cleaner key
    week_id_normalized = week_id.replace("-", "_")
    return f"weekly_playlist_{week_id_normalized}"
