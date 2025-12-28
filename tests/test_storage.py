"""Tests for storage layer (hot store, state store, idempotency).

These tests verify:
- Conditional writes and idempotency guarantees
- TTL expiration behavior
- State race condition handling
- Retry safety (duplicate operations)
- Cache-once strategy for metadata
"""

# Import mocks from local directory
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from spotify_lifecycle.models import ArtistMetadata, PlayEvent, TrackMetadata
from spotify_lifecycle.utils.hashing import make_play_id

sys.path.insert(0, str(Path(__file__).parent))
from mocks import InMemoryHotStore, InMemoryStateStore

# ====================
# Hot Store Tests
# ====================


def test_write_play_event_once():
    """Test that duplicate play events are rejected (idempotent write)."""
    store = InMemoryHotStore()

    track_id = "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp"
    played_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    event = PlayEvent(
        play_id=make_play_id(played_at, track_id),
        track_id=track_id,
        played_at=played_at,
        user_id="user456",
        context="playlist",
    )
    dedup_key = make_play_id(event.played_at, event.track_id)

    # First write should succeed
    result1 = store.write_play_event("hot_table", event, dedup_key)
    assert result1 is True

    # Duplicate write should fail (conditional write)
    result2 = store.write_play_event("hot_table", event, dedup_key)
    assert result2 is False

    # Verify only one event exists
    assert len(store.play_events) == 1


def test_write_play_event_with_ttl():
    """Test that play events include TTL for automatic expiration."""
    store = InMemoryHotStore()

    track_id = "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp"
    played_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    event = PlayEvent(
        play_id=make_play_id(played_at, track_id),
        track_id=track_id,
        played_at=played_at,
        user_id="user456",
        context=None,
    )
    dedup_key = make_play_id(event.played_at, event.track_id)

    # Write event with 7-day TTL
    store.write_play_event("hot_table", event, dedup_key, ttl_days=7)

    # Verify TTL is set
    stored_event = store.play_events[dedup_key]
    assert "ttl" in stored_event
    assert stored_event["ttl"] > time.time()  # Future timestamp
    assert stored_event["ttl"] < time.time() + (8 * 24 * 60 * 60)  # Within 8 days


def test_ttl_expiration_cleanup():
    """Test that expired events are removed by TTL cleanup."""
    store = InMemoryHotStore()

    # Create event with 0-day TTL (expires immediately)
    track_id = "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp"
    played_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    event = PlayEvent(
        play_id=make_play_id(played_at, track_id),
        track_id=track_id,
        played_at=played_at,
        user_id="user456",
        context=None,
    )
    dedup_key = make_play_id(event.played_at, event.track_id)

    # Manually set expired TTL
    store.play_events[dedup_key] = {
        "dedup_key": dedup_key,
        "track_id": event.track_id,
        "played_at": event.played_at.isoformat(),
        "user_id": event.user_id,
        "context": "",
        "ttl": int(time.time() - 1),  # Already expired
    }

    # Run cleanup
    deleted_count = store.cleanup_expired()
    assert deleted_count == 1
    assert len(store.play_events) == 0


def test_exists_check():
    """Test explicit exists() check for idempotency."""
    store = InMemoryHotStore()

    track_id = "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp"
    played_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    event = PlayEvent(
        play_id=make_play_id(played_at, track_id),
        track_id=track_id,
        played_at=played_at,
        user_id="user456",
        context=None,
    )
    dedup_key = make_play_id(event.played_at, event.track_id)

    # Before write: should not exist
    assert store.exists("hot_table", "dedup_key", dedup_key) is False

    # Write event
    store.write_play_event("hot_table", event, dedup_key)

    # After write: should exist
    assert store.exists("hot_table", "dedup_key", dedup_key) is True


def test_write_track_metadata_once():
    """Test that track metadata is cached only once (cache-once strategy)."""
    store = InMemoryHotStore()

    metadata = TrackMetadata(
        track_id="spotify:track:3n3Ppam7vgaVa1iaRUc9Lp",
        name="Test Song",
        artist_ids=["spotify:artist:4Z8W4fKeB5YxbusRsdQVPb"],
        artist_names=["Test Artist"],
        album_id="spotify:album:2ODvWsOgouMbaA5xf0RkJe",
        album_name="Test Album",
        release_date="2025-01-01",
        duration_ms=180000,
        explicit=False,
        popularity=75,
        uri="spotify:track:3n3Ppam7vgaVa1iaRUc9Lp",
    )

    # First write should succeed
    result1 = store.write_track_metadata("tracks_table", metadata)
    assert result1 is True

    # Duplicate write should fail (already cached)
    result2 = store.write_track_metadata("tracks_table", metadata)
    assert result2 is False

    # Verify only one entry exists
    assert len(store.tracks) == 1


def test_write_artist_metadata_once():
    """Test that artist metadata is cached only once (cache-once strategy)."""
    store = InMemoryHotStore()

    metadata = ArtistMetadata(
        artist_id="spotify:artist:4Z8W4fKeB5YxbusRsdQVPb",
        name="Test Artist",
        genres=["rock", "indie"],
        popularity=80,
        followers=1000000,
        uri="spotify:artist:4Z8W4fKeB5YxbusRsdQVPb",
        images=[{"url": "https://example.com/img.jpg", "height": 640, "width": 640}],
    )

    # First write should succeed
    result1 = store.write_artist_metadata("artists_table", metadata)
    assert result1 is True

    # Duplicate write should fail (already cached)
    result2 = store.write_artist_metadata("artists_table", metadata)
    assert result2 is False

    # Verify only one entry exists
    assert len(store.artists) == 1


def test_get_track_metadata():
    """Test retrieving cached track metadata."""
    store = InMemoryHotStore()

    track_id = "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp"
    metadata = TrackMetadata(
        track_id=track_id,
        name="Test Song",
        artist_ids=["spotify:artist:4Z8W4fKeB5YxbusRsdQVPb"],
        artist_names=["Test Artist"],
        album_id="spotify:album:2ODvWsOgouMbaA5xf0RkJe",
        album_name="Test Album",
        release_date="2025-01-01",
        duration_ms=180000,
        explicit=False,
        popularity=75,
        uri=track_id,
    )

    # Before caching: should return None
    assert store.get_track_metadata("tracks_table", track_id) is None

    # Cache metadata
    store.write_track_metadata("tracks_table", metadata)

    # After caching: should return metadata
    cached = store.get_track_metadata("tracks_table", track_id)
    assert cached is not None
    assert cached["track_id"] == track_id
    assert cached["name"] == "Test Song"


def test_get_artist_metadata():
    """Test retrieving cached artist metadata."""
    store = InMemoryHotStore()

    artist_id = "spotify:artist:4Z8W4fKeB5YxbusRsdQVPb"
    metadata = ArtistMetadata(
        artist_id=artist_id,
        name="Test Artist",
        genres=["rock", "indie"],
        popularity=80,
        followers=1000000,
        uri=artist_id,
        images=[],
    )

    # Before caching: should return None
    assert store.get_artist_metadata("artists_table", artist_id) is None

    # Cache metadata
    store.write_artist_metadata("artists_table", metadata)

    # After caching: should return metadata
    cached = store.get_artist_metadata("artists_table", artist_id)
    assert cached is not None
    assert cached["artist_id"] == artist_id
    assert cached["name"] == "Test Artist"


def test_query_plays_by_date_range():
    """Test querying play events within date range."""
    store = InMemoryHotStore()

    # Write events on different dates
    track_ids = [
        "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp",
        "spotify:track:4iV5W9uYEdYUVa79Axb7Rh",
        "spotify:track:1301WleyT98MSxVHPZCA6M",
        "spotify:track:2takcwOaAZWiXQijPHIx7B",
        "spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
    ]
    for day in range(1, 6):
        track_id = track_ids[day - 1]
        played_at = datetime(2025, 1, day, 12, 0, 0, tzinfo=timezone.utc)
        event = PlayEvent(
            play_id=make_play_id(played_at, track_id),
            track_id=track_id,
            played_at=played_at,
            user_id="user456",
            context=None,
        )
        dedup_key = make_play_id(event.played_at, event.track_id)
        store.write_play_event("hot_table", event, dedup_key)

    # Query for Jan 2-4 (should return 3 events)
    results = store.query_plays_by_date_range(
        "hot_table", "2025-01-02T00:00:00+00:00", "2025-01-04T23:59:59+00:00"
    )

    assert len(results) == 3
    result_track_ids = {r["track_id"] for r in results}
    assert result_track_ids == {track_ids[1], track_ids[2], track_ids[3]}


# ====================
# State Store Tests
# ====================


def test_get_ingestion_cursor_initial():
    """Test that initial cursor is None (first run)."""
    store = InMemoryStateStore()

    cursor = store.get_ingestion_cursor("state_table", "user456")
    assert cursor is None


def test_set_and_get_ingestion_cursor():
    """Test setting and retrieving ingestion cursor."""
    store = InMemoryStateStore()

    # Set cursor
    result = store.set_ingestion_cursor("state_table", "user456", "2025-01-01T12:00:00+00:00")
    assert result is True

    # Get cursor
    cursor = store.get_ingestion_cursor("state_table", "user456")
    assert cursor == "2025-01-01T12:00:00+00:00"


def test_set_ingestion_cursor_race_condition():
    """Test race-safe cursor update with conditional expression."""
    store = InMemoryStateStore()

    # Set initial cursor
    store.set_ingestion_cursor("state_table", "user456", "2025-01-01T12:00:00+00:00")

    # Simulate concurrent update attempt with stale prev_cursor
    result = store.set_ingestion_cursor(
        "state_table",
        "user456",
        "2025-01-01T14:00:00+00:00",
        prev_cursor="2025-01-01T10:00:00+00:00",  # Wrong prev_cursor
    )
    assert result is False  # Race condition detected

    # Verify cursor was not updated
    cursor = store.get_ingestion_cursor("state_table", "user456")
    assert cursor == "2025-01-01T12:00:00+00:00"  # Still original value


def test_set_ingestion_cursor_success_with_prev():
    """Test successful cursor update with correct prev_cursor."""
    store = InMemoryStateStore()

    # Set initial cursor
    store.set_ingestion_cursor("state_table", "user456", "2025-01-01T12:00:00+00:00")

    # Update with correct prev_cursor
    result = store.set_ingestion_cursor(
        "state_table",
        "user456",
        "2025-01-01T14:00:00+00:00",
        prev_cursor="2025-01-01T12:00:00+00:00",  # Correct prev_cursor
    )
    assert result is True

    # Verify cursor was updated
    cursor = store.get_ingestion_cursor("state_table", "user456")
    assert cursor == "2025-01-01T14:00:00+00:00"


def test_check_weekly_run_not_exists():
    """Test that weekly run check returns False initially."""
    store = InMemoryStateStore()

    exists = store.check_weekly_run_exists("state_table", "2025-W01")
    assert exists is False


def test_record_weekly_run_once():
    """Test that weekly run is recorded only once (idempotent)."""
    store = InMemoryStateStore()

    # First record should succeed
    result1 = store.record_weekly_run("state_table", "2025-W01", "playlist123", 50)
    assert result1 is True

    # Duplicate record should fail (idempotent)
    result2 = store.record_weekly_run("state_table", "2025-W01", "playlist456", 60)
    assert result2 is False

    # Verify weekly run exists
    exists = store.check_weekly_run_exists("state_table", "2025-W01")
    assert exists is True


def test_weekly_runs_independent_by_week():
    """Test that weekly runs are tracked independently per week."""
    store = InMemoryStateStore()

    # Record runs for different weeks
    store.record_weekly_run("state_table", "2025-W01", "playlist123", 50)
    store.record_weekly_run("state_table", "2025-W02", "playlist456", 60)

    # Both should exist independently
    assert store.check_weekly_run_exists("state_table", "2025-W01") is True
    assert store.check_weekly_run_exists("state_table", "2025-W02") is True
    assert store.check_weekly_run_exists("state_table", "2025-W03") is False


# ====================
# Retry Safety Tests
# ====================


def test_retry_play_event_write_is_safe():
    """Test that retrying a failed write operation is safe (idempotent)."""
    store = InMemoryHotStore()

    track_id = "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp"
    played_at = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    event = PlayEvent(
        play_id=make_play_id(played_at, track_id),
        track_id=track_id,
        played_at=played_at,
        user_id="user456",
        context=None,
    )
    dedup_key = make_play_id(event.played_at, event.track_id)

    # Simulate retry scenario: write 3 times
    results = [store.write_play_event("hot_table", event, dedup_key) for _ in range(3)]

    # First write succeeds, subsequent writes fail (idempotent)
    assert results == [True, False, False]
    assert len(store.play_events) == 1


def test_retry_track_metadata_write_is_safe():
    """Test that retrying metadata write is safe (cache-once)."""
    store = InMemoryHotStore()

    metadata = TrackMetadata(
        track_id="spotify:track:3n3Ppam7vgaVa1iaRUc9Lp",
        name="Test Song",
        artist_ids=["spotify:artist:4Z8W4fKeB5YxbusRsdQVPb"],
        artist_names=["Test Artist"],
        album_id="spotify:album:2ODvWsOgouMbaA5xf0RkJe",
        album_name="Test Album",
        release_date="2025-01-01",
        duration_ms=180000,
        explicit=False,
        popularity=75,
        uri="spotify:track:3n3Ppam7vgaVa1iaRUc9Lp",
    )

    # Simulate retry scenario: write 3 times
    results = [store.write_track_metadata("tracks_table", metadata) for _ in range(3)]

    # First write succeeds, subsequent writes fail (cache-once)
    assert results == [True, False, False]
    assert len(store.tracks) == 1


def test_retry_weekly_run_record_is_safe():
    """Test that retrying weekly run recording is safe (idempotent)."""
    store = InMemoryStateStore()

    # Simulate retry scenario: record 3 times
    results = [
        store.record_weekly_run("state_table", "2025-W01", "playlist123", 50) for _ in range(3)
    ]

    # First record succeeds, subsequent records fail (idempotent)
    assert results == [True, False, False]
    assert len([k for k in store.state if k.startswith("weekly_run#")]) == 1
