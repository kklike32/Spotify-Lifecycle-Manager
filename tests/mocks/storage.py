"""In-memory mock storage for deterministic testing.

These mocks implement the same interface as DynamoDBClient but use
in-memory dictionaries instead of AWS resources. This enables:
- Deterministic tests (no external dependencies)
- Fast tests (no network calls)
- No AWS credentials required
- Full control over test scenarios (race conditions, failures, etc.)
"""

import time
from datetime import datetime
from typing import Optional

from spotify_lifecycle.models import ArtistMetadata, PlayEvent, TrackMetadata


class InMemoryHotStore:
    """In-memory implementation of hot store for testing.

    Mimics DynamoDB behavior including:
    - Conditional writes (idempotency)
    - TTL expiration (automatic cleanup)
    - Primary key uniqueness
    """

    def __init__(self):
        """Initialize empty in-memory stores."""
        self.play_events: dict[str, dict] = {}  # dedup_key -> event
        self.tracks: dict[str, dict] = {}  # track_id -> metadata
        self.artists: dict[str, dict] = {}  # artist_id -> metadata

    def write_play_event(
        self, table_name: str, event: PlayEvent, dedup_key: str, ttl_days: int = 7
    ) -> bool:
        """Write play event with conditional write and TTL.

        Args:
            table_name: Ignored (in-memory)
            event: PlayEvent to store
            dedup_key: Unique key for deduplication
            ttl_days: Days before expiration

        Returns:
            bool: True if written, False if already exists
        """
        if dedup_key in self.play_events:
            return False  # Already exists (conditional write failed)

        ttl_timestamp = int(time.time() + (ttl_days * 24 * 60 * 60))

        self.play_events[dedup_key] = {
            "dedup_key": dedup_key,
            "track_id": event.track_id,
            "played_at": event.played_at.isoformat(),
            "user_id": event.user_id,
            "context": event.context or "",
            "ttl": ttl_timestamp,
        }
        return True

    def exists(self, table_name: str, key_name: str, key_value: str) -> bool:
        """Check if item exists.

        Args:
            table_name: Ignored (in-memory)
            key_name: Key attribute name ("dedup_key", "track_id", etc.)
            key_value: Value to check

        Returns:
            bool: True if exists
        """
        if key_name == "dedup_key":
            return key_value in self.play_events
        elif key_name == "track_id":
            return key_value in self.tracks
        elif key_name == "artist_id":
            return key_value in self.artists
        return False

    def write_track_metadata(
        self, table_name: str, metadata: TrackMetadata, overwrite_existing: bool = False
    ) -> bool:
        """Cache track metadata with conditional write.

        Args:
            table_name: Ignored (in-memory)
            metadata: TrackMetadata to cache
            overwrite_existing: If True, overwrite existing entry (used for repairs)

        Returns:
            bool: True if written, False if already exists
        """
        if metadata.track_id in self.tracks and not overwrite_existing:
            return False  # Already cached

        self.tracks[metadata.track_id] = {
            "track_id": metadata.track_id,
            "name": metadata.name,
            "artist_ids": metadata.artist_ids,
            "artist_names": metadata.artist_names,
            "album_id": metadata.album_id,
            "album_name": metadata.album_name,
            "duration_ms": metadata.duration_ms,
            "explicit": metadata.explicit,
            "popularity": metadata.popularity,
            "release_date": metadata.release_date,
            "uri": metadata.uri,
            "cached_at": metadata.cached_at.isoformat(),
            "version": metadata.version,
        }
        return True

    def write_artist_metadata(self, table_name: str, metadata: ArtistMetadata) -> bool:
        """Cache artist metadata with conditional write.

        Args:
            table_name: Ignored (in-memory)
            metadata: ArtistMetadata to cache

        Returns:
            bool: True if written, False if already exists
        """
        if metadata.artist_id in self.artists:
            return False  # Already cached

        self.artists[metadata.artist_id] = {
            "artist_id": metadata.artist_id,
            "name": metadata.name,
            "genres": metadata.genres,
            "popularity": metadata.popularity,
            "uri": metadata.uri,
            "images": metadata.images,
        }
        return True

    def get_track_metadata(self, table_name: str, track_id: str) -> Optional[dict]:
        """Get cached track metadata.

        Args:
            table_name: Ignored (in-memory)
            track_id: Track ID to lookup

        Returns:
            Track metadata dict or None
        """
        return self.tracks.get(track_id)

    def get_artist_metadata(self, table_name: str, artist_id: str) -> Optional[dict]:
        """Get cached artist metadata.

        Args:
            table_name: Ignored (in-memory)
            artist_id: Artist ID to lookup

        Returns:
            Artist metadata dict or None
        """
        return self.artists.get(artist_id)

    def query_plays_by_date_range(
        self, table_name: str, start_date: str, end_date: str
    ) -> list[dict]:
        """Query play events within date range.

        Args:
            table_name: Ignored (in-memory)
            start_date: ISO format start date
            end_date: ISO format end date

        Returns:
            List of play events in range
        """
        return [
            event
            for event in self.play_events.values()
            if start_date <= event["played_at"] <= end_date
        ]

    def cleanup_expired(self) -> int:
        """Remove expired events (simulate TTL deletion).

        Returns:
            int: Number of items deleted
        """
        now = int(time.time())
        expired_keys = [key for key, event in self.play_events.items() if event["ttl"] < now]
        for key in expired_keys:
            del self.play_events[key]
        return len(expired_keys)


class InMemoryStateStore:
    """In-memory implementation of state store for testing.

    Mimics DynamoDB state operations including:
    - Cursor tracking (ingestion progress)
    - Weekly run tracking (playlist idempotency)
    - Race-safe conditional updates
    """

    def __init__(self):
        """Initialize empty state storage."""
        self.state: dict[str, dict] = {}  # state_key -> state_data

    def get_ingestion_cursor(self, table_name: str, user_id: str) -> Optional[str]:
        """Get last ingestion cursor.

        Args:
            table_name: Ignored (in-memory)
            user_id: Spotify user ID

        Returns:
            Cursor value (ISO timestamp) or None
        """
        state_key = f"ingestion_cursor#{user_id}"
        item = self.state.get(state_key)
        return item.get("cursor_value") if item else None

    def set_ingestion_cursor(
        self, table_name: str, user_id: str, cursor: str, prev_cursor: Optional[str] = None
    ) -> bool:
        """Set ingestion cursor with race-safe update.

        Args:
            table_name: Ignored (in-memory)
            user_id: Spotify user ID
            cursor: New cursor value
            prev_cursor: Expected previous cursor (for race detection)

        Returns:
            bool: True if updated, False if race condition
        """
        state_key = f"ingestion_cursor#{user_id}"

        if prev_cursor is not None:
            # Conditional update: check current value matches expected
            current = self.state.get(state_key)
            if current and current.get("cursor_value") != prev_cursor:
                return False  # Race condition detected

        self.state[state_key] = {
            "state_key": state_key,
            "cursor_value": cursor,
            "updated_at": datetime.utcnow().isoformat(),
        }
        return True

    def check_weekly_run_exists(self, table_name: str, week_id: str) -> bool:
        """Check if weekly run already completed.

        Args:
            table_name: Ignored (in-memory)
            week_id: Week identifier (e.g., "2025-W52")

        Returns:
            bool: True if run exists
        """
        state_key = f"weekly_run#{week_id}"
        return state_key in self.state

    def record_weekly_run(
        self, table_name: str, week_id: str, playlist_id: str, track_count: int
    ) -> bool:
        """Record weekly run with conditional write.

        Args:
            table_name: Ignored (in-memory)
            week_id: Week identifier
            playlist_id: Created playlist ID
            track_count: Number of tracks added

        Returns:
            bool: True if recorded, False if already exists
        """
        state_key = f"weekly_run#{week_id}"

        if state_key in self.state:
            return False  # Already exists (idempotent)

        self.state[state_key] = {
            "state_key": state_key,
            "playlist_id": playlist_id,
            "track_count": track_count,
            "created_at": datetime.utcnow().isoformat(),
        }
        return True
