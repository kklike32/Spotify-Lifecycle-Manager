"""Tests for storage layer (hot store, state store, cold store, idempotency).

These tests verify:
- Conditional writes and idempotency guarantees
- TTL expiration behavior
- State race condition handling
- Retry safety (duplicate operations)
- Cache-once strategy for metadata
- Cold store partition paths and JSONL serialization
- Append-only behavior for data lake
"""

# Import mocks from local directory
import json
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


def test_write_track_metadata_overwrite_repairs_missing_fields():
    """Existing track cache entries can be repaired with overwrite flag."""
    store = InMemoryHotStore()

    original = TrackMetadata(
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

    store.write_track_metadata("tracks_table", original)

    repaired = original.model_copy(
        update={
            "artist_names": ["Fixed Artist"],
            "release_date": "2025-02-02",
            "popularity": 80,
        }
    )

    # Overwrite existing entry
    result = store.write_track_metadata("tracks_table", repaired, overwrite_existing=True)

    cached = store.get_track_metadata("tracks_table", original.track_id)
    assert result is True
    assert cached["artist_names"] == ["Fixed Artist"]
    assert cached["release_date"] == "2025-02-02"
    assert cached["popularity"] == 80


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


# ====================
# Cold Store Tests (S3)
# ====================


def test_cold_store_partition_path_generation():
    """Test that partition paths follow dt=YYYY-MM-DD format."""
    from unittest.mock import MagicMock

    from spotify_lifecycle.storage.s3 import S3ColdStore

    store = S3ColdStore()
    store.s3 = MagicMock()  # Mock S3 client

    track_id = "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp"
    played_at = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
    events = [
        PlayEvent(
            play_id=make_play_id(played_at, track_id),
            track_id=track_id,
            played_at=played_at,
            user_id="user456",
            context=None,
        )
    ]

    # Write events for specific date
    partition_date = datetime(2025, 1, 15)
    key = store.write_play_events("test-bucket", partition_date, events)

    # Verify partition path format
    assert key.startswith("dt=2025-01-15/")
    assert key.endswith(".jsonl")
    assert "events_" in key

    # Verify S3 put_object was called
    store.s3.put_object.assert_called_once()
    call_args = store.s3.put_object.call_args
    assert call_args[1]["Bucket"] == "test-bucket"
    assert call_args[1]["Key"] == key
    assert call_args[1]["ContentType"] == "application/x-jsonlines"


def test_cold_store_jsonl_serialization():
    """Test that events are serialized as JSONL (one JSON per line)."""
    from unittest.mock import MagicMock

    from spotify_lifecycle.storage.s3 import S3ColdStore

    store = S3ColdStore()
    store.s3 = MagicMock()

    # Create multiple events
    events = []
    for i in range(3):
        track_id = f"spotify:track:track{i}"
        played_at = datetime(2025, 1, 15, 12, i, 0, tzinfo=timezone.utc)
        events.append(
            PlayEvent(
                play_id=make_play_id(played_at, track_id),
                track_id=track_id,
                played_at=played_at,
                user_id="user456",
                context=None,
            )
        )

    # Write events
    partition_date = datetime(2025, 1, 15)
    store.write_play_events("test-bucket", partition_date, events)

    # Extract serialized body
    call_args = store.s3.put_object.call_args
    body = call_args[1]["Body"].decode("utf-8")

    # Verify JSONL format (one JSON per line, trailing newline)
    lines = body.strip().split("\n")
    assert len(lines) == 3

    # Verify each line is valid JSON
    for line in lines:
        event_dict = json.loads(line)
        assert "play_id" in event_dict
        assert "track_id" in event_dict
        assert "played_at" in event_dict

    # Verify trailing newline
    assert body.endswith("\n")


def test_cold_store_append_only_behavior():
    """Test that writes never overwrite (append-only with unique timestamps)."""
    from unittest.mock import MagicMock, patch

    from spotify_lifecycle.storage.s3 import S3ColdStore

    store = S3ColdStore()
    store.s3 = MagicMock()

    track_id = "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp"
    played_at = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        PlayEvent(
            play_id=make_play_id(played_at, track_id),
            track_id=track_id,
            played_at=played_at,
            user_id="user456",
            context=None,
        )
    ]

    partition_date = datetime(2025, 1, 15)

    # Mock datetime to control timestamp
    with patch("spotify_lifecycle.storage.s3.datetime") as mock_datetime:
        # First write at 10:00:00
        mock_datetime.utcnow.return_value = datetime(2025, 1, 15, 10, 0, 0)
        key1 = store.write_play_events("test-bucket", partition_date, events)

        # Second write at 10:00:01 (1 second later)
        mock_datetime.utcnow.return_value = datetime(2025, 1, 15, 10, 0, 1)
        key2 = store.write_play_events("test-bucket", partition_date, events)

    # Verify different keys (no overwrite)
    assert key1 != key2
    assert key1 == "dt=2025-01-15/events_100000.jsonl"
    assert key2 == "dt=2025-01-15/events_100001.jsonl"

    # Verify both writes called S3 (no overwrite)
    assert store.s3.put_object.call_count == 2


def test_cold_store_rejects_empty_events():
    """Test that writing empty events list raises ValueError."""
    from spotify_lifecycle.storage.s3 import S3ColdStore

    store = S3ColdStore()
    partition_date = datetime(2025, 1, 15)

    # Attempt to write empty list
    import pytest

    with pytest.raises(ValueError, match="Cannot write empty events list"):
        store.write_play_events("test-bucket", partition_date, [])


def test_cold_store_read_date_range():
    """Test reading events from multiple partitions within date range."""
    from unittest.mock import MagicMock

    from spotify_lifecycle.storage.s3 import S3ColdStore

    store = S3ColdStore()
    store.s3 = MagicMock()

    # Mock list_objects_v2 paginator
    mock_paginator = MagicMock()
    store.s3.get_paginator.return_value = mock_paginator

    # Simulate 3 days of data
    mock_paginator.paginate.side_effect = [
        # Day 1 (2025-01-15)
        [{"Contents": [{"Key": "dt=2025-01-15/events_120000.jsonl"}]}],
        # Day 2 (2025-01-16)
        [{"Contents": [{"Key": "dt=2025-01-16/events_120000.jsonl"}]}],
        # Day 3 (2025-01-17)
        [{"Contents": [{"Key": "dt=2025-01-17/events_120000.jsonl"}]}],
    ]

    # Mock get_object for each file
    def mock_get_object(Bucket, Key):
        event = {
            "version": "1.0.0",
            "play_id": "test-id",
            "track_id": "spotify:track:test",
            "played_at": "2025-01-15T12:00:00+00:00",
            "user_id": "user456",
            "context": None,
            "ingested_at": "2025-01-15T12:00:00+00:00",
        }
        return {"Body": MagicMock(read=lambda: json.dumps(event).encode("utf-8"))}

    store.s3.get_object.side_effect = mock_get_object

    # Read events from 3-day range
    start_date = datetime(2025, 1, 15)
    end_date = datetime(2025, 1, 17)
    events = list(store.read_play_events("test-bucket", start_date, end_date))

    # Verify 3 events read (one per file)
    assert len(events) == 3

    # Verify all are PlayEvent objects
    for event in events:
        assert isinstance(event, PlayEvent)
        assert event.track_id == "spotify:track:test"


def test_cold_store_partition_stats():
    """Test getting storage statistics for partitions."""
    from unittest.mock import MagicMock

    from spotify_lifecycle.storage.s3 import S3ColdStore

    store = S3ColdStore()
    store.s3 = MagicMock()

    # Mock list_objects_v2 paginator
    mock_paginator = MagicMock()
    store.s3.get_paginator.return_value = mock_paginator

    # Simulate 2 partitions with 3 files total
    mock_paginator.paginate.side_effect = [
        # Day 1: 2 files
        [
            {
                "Contents": [
                    {"Key": "dt=2025-01-15/events_120000.jsonl"},
                    {"Key": "dt=2025-01-15/events_130000.jsonl"},
                ]
            }
        ],
        # Day 2: 1 file
        [{"Contents": [{"Key": "dt=2025-01-16/events_120000.jsonl"}]}],
    ]

    # Mock head_object for sizes
    def mock_head_object(Bucket, Key):
        return {"ContentLength": 1024}  # 1 KB per file

    store.s3.head_object.side_effect = mock_head_object

    # Get stats
    start_date = datetime(2025, 1, 15)
    end_date = datetime(2025, 1, 16)
    stats = store.get_partition_stats("test-bucket", start_date, end_date)

    # Verify stats
    assert stats["partition_count"] == 2  # 2 days
    assert stats["file_count"] == 3  # 3 files total
    assert stats["total_bytes"] == 3072  # 3 * 1024
    assert stats["avg_bytes_per_partition"] == 1536  # 3072 / 2


def test_cold_store_stats_empty_bucket():
    """Test partition stats for empty bucket."""
    from unittest.mock import MagicMock

    from spotify_lifecycle.storage.s3 import S3ColdStore

    store = S3ColdStore()
    store.s3 = MagicMock()

    # Mock empty paginator
    mock_paginator = MagicMock()
    store.s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{}]  # No Contents key

    # Get stats
    start_date = datetime(2025, 1, 15)
    end_date = datetime(2025, 1, 16)
    stats = store.get_partition_stats("test-bucket", start_date, end_date)

    # Verify zero stats
    assert stats["partition_count"] == 0
    assert stats["file_count"] == 0
    assert stats["total_bytes"] == 0
    assert stats["avg_bytes_per_partition"] == 0


def test_dashboard_store_write_and_read():
    """Test dashboard data write/read cycle."""
    from unittest.mock import MagicMock

    from spotify_lifecycle.storage.s3 import S3DashboardStore

    store = S3DashboardStore()
    store.s3 = MagicMock()

    # Write dashboard data
    dashboard_data = {
        "top_tracks": ["track1", "track2"],
        "listening_trends": [{"date": "2025-01-15", "count": 50}],
    }
    store.write_dashboard_data("dashboard-bucket", dashboard_data)

    # Verify write
    store.s3.put_object.assert_called_once()
    call_args = store.s3.put_object.call_args
    assert call_args[1]["Bucket"] == "dashboard-bucket"
    assert call_args[1]["Key"] == "dashboard_data.json"
    assert call_args[1]["ContentType"] == "application/json"

    # Mock read
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(dashboard_data).encode("utf-8")
    store.s3.get_object.return_value = {"Body": mock_body}

    # Read dashboard data
    data = store.read_dashboard_data("dashboard-bucket")
    assert data == dashboard_data


def test_daily_summary_idempotent_replace():
    """Daily summary writes are idempotent on identical input."""
    from unittest.mock import MagicMock

    from spotify_lifecycle.storage.s3 import S3ColdStore

    store = S3ColdStore()
    store.s3 = MagicMock()

    memory: dict[str, dict] = {}

    def fake_put_object(Bucket, Key, Body, ContentType):
        memory[Key] = json.loads(Body.decode("utf-8"))

    store.s3.put_object.side_effect = fake_put_object

    def fake_read(bucket_name, partition_date):
        return memory.get(store._daily_summary_key(partition_date))

    store.read_daily_summary = fake_read

    bucket = "test-bucket"
    partition_date = datetime(2025, 1, 1)
    counts = {"track_a": 5, "track_b": 3}

    key = store.write_daily_summary(bucket, partition_date, counts)
    first = memory[key]

    # Second write with identical counts should be a no-op
    store.write_daily_summary(bucket, partition_date, counts)

    assert first["total_plays"] == 8
    assert memory[key]["total_plays"] == 8
    assert store.s3.put_object.call_count == 1  # second call skipped


def test_daily_summary_replaces_on_mismatch():
    """Daily summary replaces (does not merge) when counts differ."""
    from unittest.mock import MagicMock

    from spotify_lifecycle.storage.s3 import S3ColdStore

    store = S3ColdStore()
    store.s3 = MagicMock()

    memory: dict[str, dict] = {}

    def fake_put_object(Bucket, Key, Body, ContentType):
        memory[Key] = json.loads(Body.decode("utf-8"))

    store.s3.put_object.side_effect = fake_put_object

    def fake_read(bucket_name, partition_date):
        return memory.get(store._daily_summary_key(partition_date))

    store.read_daily_summary = fake_read

    bucket = "test-bucket"
    partition_date = datetime(2025, 1, 2)
    initial_counts = {"track_a": 2}
    updated_counts = {"track_a": 2, "track_b": 4}

    key = store.write_daily_summary(bucket, partition_date, initial_counts)
    first_total = memory[key]["total_plays"]

    # Mismatched counts should trigger replacement, not additive merge
    store.write_daily_summary(bucket, partition_date, updated_counts)
    replaced_total = memory[key]["total_plays"]

    assert first_total == 2
    assert replaced_total == 6
    assert store.s3.put_object.call_count == 2  # second call executed


def test_daily_summary_recalculates_counts_when_missing():
    """Daily summary computes track counts from raw events when not provided."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from spotify_lifecycle.storage.s3 import S3ColdStore

    store = S3ColdStore()
    store.s3 = MagicMock()

    # Capture payload written to S3
    memory: dict[str, dict] = {}

    def fake_put_object(Bucket, Key, Body, ContentType):
        memory[Key] = json.loads(Body.decode("utf-8"))

    store.s3.put_object.side_effect = fake_put_object

    # No existing summary
    store.read_daily_summary = MagicMock(return_value=None)

    # Simulate two raw event files with 3 total plays (2 for track_a, 1 for track_b)
    store._list_partition_keys = MagicMock(
        return_value=["dt=2025-01-03/events_1.jsonl", "dt=2025-01-03/events_2.jsonl"]
    )

    def fake_read_jsonl(bucket, key):
        if key.endswith("events_1.jsonl"):
            yield SimpleNamespace(track_id="spotify:track:a")
            yield SimpleNamespace(track_id="spotify:track:a")
        else:
            yield SimpleNamespace(track_id="spotify:track:b")

    store._read_jsonl_file = fake_read_jsonl

    bucket = "test-bucket"
    partition_date = datetime(2025, 1, 3)

    key = store.write_daily_summary(bucket, partition_date)
    payload = memory[key]

    assert payload["total_plays"] == 3
    assert payload["track_counts"]["spotify:track:a"] == 2
    assert payload["track_counts"]["spotify:track:b"] == 1
    store.s3.put_object.assert_called_once()


def test_daily_summary_deduplicates_play_ids_across_files():
    """Recomputed summaries should not double-count the same play_id across files."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from spotify_lifecycle.storage.s3 import S3ColdStore

    store = S3ColdStore()
    store.s3 = MagicMock()

    memory: dict[str, dict] = {}
    store.s3.put_object.side_effect = lambda Bucket, Key, Body, ContentType: memory.update(
        {Key: json.loads(Body.decode("utf-8"))}
    )
    store.read_daily_summary = MagicMock(return_value=None)

    # Two files containing the same play_id (should count once)
    store._list_partition_keys = MagicMock(
        return_value=["dt=2025-01-04/events_a.jsonl", "dt=2025-01-04/events_b.jsonl"]
    )

    def fake_read_jsonl(bucket, key):
        yield SimpleNamespace(track_id="spotify:track:a", play_id="p1")
        if key.endswith("events_b.jsonl"):
            yield SimpleNamespace(track_id="spotify:track:a", play_id="p1")  # duplicate

    store._read_jsonl_file = fake_read_jsonl

    bucket = "test-bucket"
    partition_date = datetime(2025, 1, 4)
    key = store.write_daily_summary(bucket, partition_date)

    payload = memory[key]
    assert payload["total_plays"] == 1  # deduped
    assert payload["track_counts"]["spotify:track:a"] == 1
    store.s3.put_object.assert_called_once()


def test_dashboard_store_read_missing():
    """Test reading missing dashboard data returns None."""
    from unittest.mock import MagicMock

    from botocore.exceptions import ClientError

    from spotify_lifecycle.storage.s3 import S3DashboardStore

    store = S3DashboardStore()
    store.s3 = MagicMock()

    # Mock NoSuchKey error
    error = ClientError({"Error": {"Code": "NoSuchKey"}}, "get_object")
    store.s3.get_object.side_effect = error

    # Read missing data
    data = store.read_dashboard_data("dashboard-bucket")
    assert data is None
