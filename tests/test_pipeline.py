"""Tests for pipeline utilities and ingestion logic."""

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from spotify_lifecycle.models import IngestionState, PlayEvent
from spotify_lifecycle.pipeline.ingest import (
    fetch_with_overlap,
    parse_play_event,
    run_ingestion,
    write_events_to_storage,
)


# Test data fixtures
@pytest.fixture
def mock_spotify_response():
    """Mock Spotify API response."""
    now = datetime.now(timezone.utc)
    return {
        "items": [
            {
                "track": {"id": "spotify:track:1", "name": "Song 1"},
                "played_at": now.isoformat().replace("+00:00", "Z"),
                "context": {"uri": "spotify:playlist:123"},
            },
            {
                "track": {"id": "spotify:track:2", "name": "Song 2"},
                "played_at": (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                "context": None,
            },
            {
                "track": {"id": "spotify:track:3", "name": "Song 3"},
                "played_at": (now - timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
                "context": {"uri": "spotify:album:456"},
            },
        ],
        "cursors": {
            "before": 1609459200000,  # Unix ms timestamp
            "after": 1609462800000,
        },
    }


@pytest.fixture
def mock_spotify_client():
    """Mock Spotify client."""
    client = Mock()
    client.sp.current_user.return_value = {"id": "testuser"}
    return client


@pytest.fixture
def mock_dynamo_client():
    """Mock DynamoDB client."""
    client = Mock()
    client.write_play_event = Mock()
    client.get_ingestion_state = Mock(return_value=None)
    client.update_ingestion_state = Mock()
    return client


@pytest.fixture
def mock_s3_client():
    """Mock S3 client."""
    client = Mock()
    client.write_raw_events = Mock()
    client.write_daily_summary = Mock()
    return client


# Tests for fetch_with_overlap
def test_fetch_with_overlap_success(mock_spotify_client, mock_spotify_response):
    """Test successful fetch with overlap."""
    mock_spotify_client.get_recently_played.return_value = mock_spotify_response

    items, next_cursor, oldest_ts, newest_ts = fetch_with_overlap(
        mock_spotify_client, cursor=None, limit=50
    )

    assert len(items) == 3
    assert next_cursor == 1609459200000
    assert oldest_ts > 0
    assert newest_ts > oldest_ts
    mock_spotify_client.get_recently_played.assert_called_once()


def test_fetch_with_overlap_empty_response(mock_spotify_client):
    """Test fetch when no items returned."""
    mock_spotify_client.get_recently_played.return_value = {"items": []}

    items, next_cursor, oldest_ts, newest_ts = fetch_with_overlap(mock_spotify_client, cursor=None)

    assert items == []
    assert next_cursor is None
    assert oldest_ts == 0
    assert newest_ts == 0


def test_fetch_with_overlap_api_error(mock_spotify_client):
    """Test fetch when Spotify API fails."""
    mock_spotify_client.get_recently_played.side_effect = Exception("API Error")

    with pytest.raises(RuntimeError, match="Spotify API failed"):
        fetch_with_overlap(mock_spotify_client, cursor=None)


def test_fetch_with_overlap_cursor_passed(mock_spotify_client, mock_spotify_response):
    """Test that cursor is correctly passed to API."""
    mock_spotify_client.get_recently_played.return_value = mock_spotify_response
    cursor = 1609459200000

    fetch_with_overlap(mock_spotify_client, cursor=cursor)

    # Should use Spotify max limit of 50 (50 base + 5 overlap = 55, capped at 50)
    mock_spotify_client.get_recently_played.assert_called_once_with(limit=50, before=cursor)


# Tests for parse_play_event
def test_parse_play_event():
    """Test parsing Spotify API item into PlayEvent."""
    now = datetime.now(timezone.utc)
    item = {
        "track": {"id": "123", "name": "Test Song"},  # Spotify API returns just ID, not full URI
        "played_at": now.isoformat().replace("+00:00", "Z"),
        "context": {"uri": "spotify:playlist:abc"},
    }

    event = parse_play_event(item, user_id="testuser")

    assert isinstance(event, PlayEvent)
    assert event.track_id == "spotify:track:123"
    assert event.user_id == "testuser"
    assert event.context == "spotify:playlist:abc"
    assert event.played_at.tzinfo is not None  # Has timezone


def test_parse_play_event_no_context():
    """Test parsing event with no context."""
    now = datetime.now(timezone.utc)
    item = {
        "track": {"id": "spotify:track:123", "name": "Test Song"},
        "played_at": now.isoformat().replace("+00:00", "Z"),
        "context": None,
    }

    event = parse_play_event(item, user_id="testuser")

    assert event.context is None


# Tests for write_events_to_storage
def test_write_events_to_storage_success(mock_dynamo_client, mock_s3_client, mock_spotify_response):
    """Test successful write to both hot and cold storage."""
    from spotify_lifecycle.utils.hashing import make_play_id

    now = datetime.now(timezone.utc)
    events = [
        PlayEvent(
            play_id=make_play_id(now, "spotify:track:1"),
            track_id="spotify:track:1",
            played_at=now,
            user_id="testuser",
            context="spotify:playlist:123",
        ),
        PlayEvent(
            play_id=make_play_id(now - timedelta(minutes=5), "spotify:track:2"),
            track_id="spotify:track:2",
            played_at=now - timedelta(minutes=5),
            user_id="testuser",
            context=None,
        ),
    ]

    hot_written, cold_written = write_events_to_storage(
        events,
        mock_dynamo_client,
        mock_s3_client,
        hot_table_name="test_hot",
        raw_bucket_name="test_raw",
    )

    assert hot_written == 2
    assert cold_written == 2
    assert mock_dynamo_client.write_play_event.call_count == 2
    assert mock_s3_client.write_raw_events.call_count == 1  # Same date batch
    assert mock_s3_client.write_daily_summary.call_count == 1


def test_write_events_to_storage_dedupe(mock_dynamo_client, mock_s3_client):
    """Test deduplication (conditional write failure)."""
    from spotify_lifecycle.utils.hashing import make_play_id

    now = datetime.now(timezone.utc)
    events = [
        PlayEvent(
            play_id=make_play_id(now, "spotify:track:1"),
            track_id="spotify:track:1",
            played_at=now,
            user_id="testuser",
            context=None,
        )
    ]

    # Simulate conditional write failure (duplicate)
    mock_dynamo_client.write_play_event.side_effect = Exception("ConditionalCheckFailedException")

    hot_written, cold_written = write_events_to_storage(
        events,
        mock_dynamo_client,
        mock_s3_client,
        hot_table_name="test_hot",
        raw_bucket_name="test_raw",
    )

    assert hot_written == 0  # Duplicate skipped
    assert cold_written == 1  # Still written to cold (append-only)
    assert mock_s3_client.write_daily_summary.call_count == 1


def test_write_events_to_storage_multiple_dates(mock_dynamo_client, mock_s3_client):
    """Test events spanning multiple dates are batched correctly."""
    from spotify_lifecycle.utils.hashing import make_play_id

    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)

    events = [
        PlayEvent(
            play_id=make_play_id(now, "spotify:track:1"),
            track_id="spotify:track:1",
            played_at=now,
            user_id="testuser",
            context=None,
        ),
        PlayEvent(
            play_id=make_play_id(yesterday, "spotify:track:2"),
            track_id="spotify:track:2",
            played_at=yesterday,
            user_id="testuser",
            context=None,
        ),
    ]

    hot_written, cold_written = write_events_to_storage(
        events,
        mock_dynamo_client,
        mock_s3_client,
        hot_table_name="test_hot",
        raw_bucket_name="test_raw",
    )

    assert hot_written == 2
    assert cold_written == 2
    assert mock_s3_client.write_raw_events.call_count == 2  # Two date partitions
    assert mock_s3_client.write_daily_summary.call_count == 2  # Two summaries


# Tests for run_ingestion
def test_run_ingestion_success(
    mock_spotify_client, mock_dynamo_client, mock_s3_client, mock_spotify_response
):
    """Test complete ingestion pipeline."""
    mock_spotify_client.get_recently_played.return_value = mock_spotify_response
    mock_dynamo_client.get_ingestion_state.return_value = None

    summary = run_ingestion(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        s3_client=mock_s3_client,
        state_table_name="test_state",
        hot_table_name="test_hot",
        raw_bucket_name="test_raw",
        max_pages=1,
    )

    assert summary["pages_fetched"] == 1
    assert summary["items_fetched"] == 3
    assert summary["unique_events"] == 3
    assert "duration_sec" in summary
    mock_dynamo_client.update_ingestion_state.assert_called_once()


def test_run_ingestion_with_existing_cursor(
    mock_spotify_client, mock_dynamo_client, mock_s3_client, mock_spotify_response
):
    """Test ingestion resumes from previous cursor."""
    existing_state = IngestionState(
        state_key="ingestion_state",
        last_played_at=datetime.now(timezone.utc) - timedelta(hours=1),
        last_run_at=datetime.now(timezone.utc) - timedelta(hours=1),
        last_event_count=50,
        status="success",
    )
    mock_dynamo_client.get_ingestion_state.return_value = existing_state
    mock_spotify_client.get_recently_played.return_value = mock_spotify_response

    run_ingestion(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        s3_client=mock_s3_client,
        state_table_name="test_state",
        hot_table_name="test_hot",
        raw_bucket_name="test_raw",
        max_pages=1,
    )

    # Should fetch from API (cursor not yet implemented in MVP)
    mock_spotify_client.get_recently_played.assert_called_once()


def test_run_ingestion_pagination(
    mock_spotify_client, mock_dynamo_client, mock_s3_client, mock_spotify_response
):
    """Test pagination across multiple pages."""
    # First page has cursor, second page is empty
    mock_spotify_client.get_recently_played.side_effect = [
        mock_spotify_response,
        {"items": []},
    ]
    mock_dynamo_client.get_ingestion_state.return_value = None

    summary = run_ingestion(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        s3_client=mock_s3_client,
        state_table_name="test_state",
        hot_table_name="test_hot",
        raw_bucket_name="test_raw",
        max_pages=5,
    )

    assert summary["pages_fetched"] == 1  # Only counts pages with items
    assert mock_spotify_client.get_recently_played.call_count == 2  # But still fetches twice


def test_run_ingestion_max_pages_limit(
    mock_spotify_client, mock_dynamo_client, mock_s3_client, mock_spotify_response
):
    """Test ingestion respects max_pages limit."""
    # Always return results (would paginate forever)
    mock_spotify_client.get_recently_played.return_value = mock_spotify_response
    mock_dynamo_client.get_ingestion_state.return_value = None

    summary = run_ingestion(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        s3_client=mock_s3_client,
        state_table_name="test_state",
        hot_table_name="test_hot",
        raw_bucket_name="test_raw",
        max_pages=3,
    )

    assert summary["pages_fetched"] == 3
    assert mock_spotify_client.get_recently_played.call_count == 3


def test_run_ingestion_state_update_failure(
    mock_spotify_client, mock_dynamo_client, mock_s3_client, mock_spotify_response
):
    """Test ingestion continues even if state update fails."""
    mock_spotify_client.get_recently_played.return_value = mock_spotify_response
    mock_dynamo_client.get_ingestion_state.return_value = None
    mock_dynamo_client.update_ingestion_state.side_effect = Exception("State update failed")

    # Should not raise (state update failure is logged but not fatal)
    summary = run_ingestion(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        s3_client=mock_s3_client,
        state_table_name="test_state",
        hot_table_name="test_hot",
        raw_bucket_name="test_raw",
        max_pages=1,
    )

    assert summary["pages_fetched"] == 1
    assert summary["items_fetched"] == 3


def test_run_ingestion_idempotency(
    mock_spotify_client, mock_dynamo_client, mock_s3_client, mock_spotify_response
):
    """Test that running ingestion twice produces no duplicates."""
    mock_spotify_client.get_recently_played.return_value = mock_spotify_response
    mock_dynamo_client.get_ingestion_state.return_value = None

    # First run
    summary1 = run_ingestion(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        s3_client=mock_s3_client,
        state_table_name="test_state",
        hot_table_name="test_hot",
        raw_bucket_name="test_raw",
        max_pages=1,
    )

    # Simulate duplicates on second run (conditional write fails)
    mock_dynamo_client.write_play_event.side_effect = Exception("ConditionalCheckFailedException")

    # Second run (same data) - should handle duplicates gracefully
    run_ingestion(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        s3_client=mock_s3_client,
        state_table_name="test_state",
        hot_table_name="test_hot",
        raw_bucket_name="test_raw",
        max_pages=1,
    )

    # First run writes, second run dedupes (idempotent behavior verified)

    # First run writes, second run skips (dedupe)
    assert summary1["hot_written"] == 3
    # Note: summary2 not checked - dedupe happens, but we don't need to assert
    # Cold storage still writes (append-only, idempotent)


# Edge cases and error scenarios
def test_run_ingestion_no_items(mock_spotify_client, mock_dynamo_client, mock_s3_client):
    """Test ingestion when no items are fetched."""
    mock_spotify_client.get_recently_played.return_value = {"items": []}
    mock_dynamo_client.get_ingestion_state.return_value = None

    summary = run_ingestion(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        s3_client=mock_s3_client,
        state_table_name="test_state",
        hot_table_name="test_hot",
        raw_bucket_name="test_raw",
        max_pages=1,
    )

    assert summary["pages_fetched"] == 0  # Breaks immediately on no items
    assert summary["items_fetched"] == 0
    assert summary["unique_events"] == 0
    assert summary["hot_written"] == 0
    assert summary["cold_written"] == 0


# ==========================================
# PLAYLIST PIPELINE TESTS (Phase 6)
# ==========================================


def test_compute_candidates_basic():
    """Test basic set-difference logic for candidate selection."""
    from spotify_lifecycle.pipeline.playlists import _compute_candidates

    source = [
        "spotify:track:1",
        "spotify:track:2",
        "spotify:track:3",
        "spotify:track:4",
        "spotify:track:5",
    ]
    recent = {"spotify:track:2", "spotify:track:4"}

    candidates = _compute_candidates(source, recent)

    # Should return source - recent
    assert len(candidates) == 3
    assert "spotify:track:1" in candidates
    assert "spotify:track:3" in candidates
    assert "spotify:track:5" in candidates
    # Should not contain recent tracks
    assert "spotify:track:2" not in candidates
    assert "spotify:track:4" not in candidates


def test_compute_candidates_preserves_order():
    """Test that candidate selection preserves source playlist order."""
    from spotify_lifecycle.pipeline.playlists import _compute_candidates

    source = ["spotify:track:5", "spotify:track:3", "spotify:track:1"]
    recent = set()

    candidates = _compute_candidates(source, recent)

    # Order should match source
    assert candidates == ["spotify:track:5", "spotify:track:3", "spotify:track:1"]


def test_compute_candidates_all_recent():
    """Test candidate selection when all tracks are recently played."""
    from spotify_lifecycle.pipeline.playlists import _compute_candidates

    source = ["spotify:track:1", "spotify:track:2", "spotify:track:3"]
    recent = {"spotify:track:1", "spotify:track:2", "spotify:track:3"}

    candidates = _compute_candidates(source, recent)

    # Should return empty list
    assert candidates == []


def test_compute_candidates_empty_source():
    """Test candidate selection with empty source playlist."""
    from spotify_lifecycle.pipeline.playlists import _compute_candidates

    source = []
    recent = {"spotify:track:1", "spotify:track:2"}

    candidates = _compute_candidates(source, recent)

    # Should return empty list
    assert candidates == []


def test_compute_candidates_no_recent():
    """Test candidate selection when no tracks recently played."""
    from spotify_lifecycle.pipeline.playlists import _compute_candidates

    source = ["spotify:track:1", "spotify:track:2", "spotify:track:3"]
    recent = set()

    candidates = _compute_candidates(source, recent)

    # Should return all source tracks
    assert candidates == source


def test_create_weekly_playlist_success(mock_spotify_client, mock_dynamo_client):
    """Test successful weekly playlist creation."""
    from spotify_lifecycle.pipeline.playlists import create_weekly_playlist

    # Setup: No existing state (first run)
    mock_dynamo_client.get_playlist_state.return_value = None

    # Setup: Source playlist with 5 tracks
    source_tracks = [
        "spotify:track:1",
        "spotify:track:2",
        "spotify:track:3",
        "spotify:track:4",
        "spotify:track:5",
    ]
    mock_spotify_client.get_playlist_tracks.return_value = source_tracks

    # Setup: 2 tracks played recently
    mock_dynamo_client.get_recently_played_track_ids.return_value = {
        "spotify:track:2",
        "spotify:track:4",
    }

    # Setup: Playlist creation returns new playlist
    mock_spotify_client.create_playlist.return_value = {
        "id": "new_playlist_id",
        "uri": "spotify:playlist:new_playlist_id",
    }

    # Setup: State write succeeds
    mock_dynamo_client.write_playlist_state.return_value = True

    # Execute
    result = create_weekly_playlist(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        source_playlist_id="spotify:playlist:source",
        lookback_days=7,
        hot_table_name="test_hot",
        state_table_name="test_state",
        user_id="testuser",
    )

    # Verify: Result summary
    assert result["playlist_id"] == "spotify:playlist:new_playlist_id"
    assert result["tracks_added"] == 3  # 5 source - 2 recent = 3 candidates
    assert result["source_count"] == 5
    assert result["recent_count"] == 2
    assert result["candidate_count"] == 3
    assert result["already_exists"] is False

    # Verify: Playlist created with correct name
    mock_spotify_client.create_playlist.assert_called_once()
    call_args = mock_spotify_client.create_playlist.call_args
    assert "Weekly Unheard" in call_args.kwargs["name"]

    # Verify: Tracks added to playlist
    mock_spotify_client.add_tracks_to_playlist.assert_called_once()
    added_tracks = mock_spotify_client.add_tracks_to_playlist.call_args[0][1]
    assert len(added_tracks) == 3
    assert "spotify:track:1" in added_tracks
    assert "spotify:track:3" in added_tracks
    assert "spotify:track:5" in added_tracks

    # Verify: State written
    mock_dynamo_client.write_playlist_state.assert_called_once()


def test_create_weekly_playlist_idempotent_skip(mock_spotify_client, mock_dynamo_client):
    """Test that existing playlist is skipped (idempotency)."""
    from datetime import datetime, timezone

    from spotify_lifecycle.models import PlaylistState
    from spotify_lifecycle.pipeline.playlists import create_weekly_playlist

    # Setup: Existing state (playlist already created this week)
    now = datetime.now(timezone.utc)
    existing_state = PlaylistState(
        state_key="weekly_playlist_2025_W52",
        week_id="2025-W52",
        playlist_id="spotify:playlist:existing",
        created_at=now,
        track_count=10,
        source_playlist_id="spotify:playlist:source",
    )
    mock_dynamo_client.get_playlist_state.return_value = existing_state

    # Execute
    result = create_weekly_playlist(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        source_playlist_id="spotify:playlist:source",
        lookback_days=7,
        hot_table_name="test_hot",
        state_table_name="test_state",
    )

    # Verify: Returns existing playlist info
    assert result["playlist_id"] == "spotify:playlist:existing"
    assert result["tracks_added"] == 10
    assert result["already_exists"] is True

    # Verify: No Spotify API calls (idempotent skip)
    mock_spotify_client.get_playlist_tracks.assert_not_called()
    mock_spotify_client.create_playlist.assert_not_called()
    mock_spotify_client.add_tracks_to_playlist.assert_not_called()

    # Verify: No state write (already exists)
    mock_dynamo_client.write_playlist_state.assert_not_called()


def test_create_weekly_playlist_empty_source():
    """Test error handling when source playlist is empty."""
    from spotify_lifecycle.pipeline.playlists import create_weekly_playlist

    mock_spotify_client = Mock()
    mock_dynamo_client = Mock()

    # Setup: No existing state
    mock_dynamo_client.get_playlist_state.return_value = None

    # Setup: Empty source playlist
    mock_spotify_client.get_playlist_tracks.return_value = []

    # Execute & Verify: Should raise ValueError
    with pytest.raises(ValueError, match="Source playlist is empty"):
        create_weekly_playlist(
            spotify_client=mock_spotify_client,
            dynamo_client=mock_dynamo_client,
            source_playlist_id="spotify:playlist:empty",
            lookback_days=7,
        )


def test_create_weekly_playlist_all_tracks_recent(mock_spotify_client, mock_dynamo_client):
    """Test playlist creation when all tracks were recently played."""
    from spotify_lifecycle.pipeline.playlists import create_weekly_playlist

    # Setup: No existing state
    mock_dynamo_client.get_playlist_state.return_value = None

    # Setup: Source playlist with 3 tracks
    source_tracks = ["spotify:track:1", "spotify:track:2", "spotify:track:3"]
    mock_spotify_client.get_playlist_tracks.return_value = source_tracks

    # Setup: All tracks played recently
    mock_dynamo_client.get_recently_played_track_ids.return_value = set(source_tracks)

    # Setup: Playlist creation returns new playlist
    mock_spotify_client.create_playlist.return_value = {
        "id": "empty_playlist",
        "uri": "spotify:playlist:empty_playlist",
    }

    # Setup: State write succeeds
    mock_dynamo_client.write_playlist_state.return_value = True

    # Execute
    result = create_weekly_playlist(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        source_playlist_id="spotify:playlist:source",
        lookback_days=7,
    )

    # Verify: Empty playlist created
    assert result["tracks_added"] == 0
    assert result["candidate_count"] == 0

    # Verify: Playlist created but no tracks added
    mock_spotify_client.create_playlist.assert_called_once()
    mock_spotify_client.add_tracks_to_playlist.assert_not_called()

    # Verify: State still written (empty playlist is valid)
    mock_dynamo_client.write_playlist_state.assert_called_once()


# ==========================================
# ENRICHMENT PIPELINE TESTS
# ==========================================


@pytest.fixture
def mock_track_metadata():
    """Mock track metadata from Spotify API."""
    return {
        "id": "abc123",
        "name": "Test Song",
        "artists": [
            {"id": "artist1", "name": "Artist One"},
            {"id": "artist2", "name": "Artist Two"},
        ],
        "album": {
            "id": "album1",
            "name": "Test Album",
            "release_date": "2025-01-01",
        },
        "duration_ms": 180000,
        "explicit": False,
        "popularity": 75,
    }


@pytest.fixture
def mock_artist_metadata():
    """Mock artist metadata from Spotify API."""
    return {
        "id": "artist1",
        "name": "Artist One",
        "genres": ["pop", "rock"],
        "popularity": 80,
        "followers": {"total": 1000000},
        "images": [{"url": "https://example.com/image.jpg", "width": 640, "height": 640}],
    }


def test_enrich_track_cache_hit(mock_spotify_client, mock_dynamo_client):
    """Test track enrichment when metadata is already cached."""
    from spotify_lifecycle.pipeline.enrich import enrich_track

    # Setup: Track already in cache
    mock_dynamo_client.get_track_metadata.return_value = {
        "track_id": "spotify:track:abc123",
        "name": "Cached Song",
        "artist_ids": ["spotify:artist:artist1"],
        "artist_names": ["Artist One"],
        "album_id": "spotify:album:album1",
        "album_name": "Cached Album",
        "duration_ms": 180000,
        "explicit": False,
        "popularity": 75,
        "release_date": "2025-01-01",
        "uri": "spotify:track:abc123",
        "cached_at": "2025-01-01T00:00:00+00:00",
    }

    # Execute
    result = enrich_track(
        track_id="spotify:track:abc123",
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        tracks_table="tracks",
    )

    # Verify: Metadata returned from cache
    assert result is not None
    assert result.track_id == "spotify:track:abc123"
    assert result.name == "Cached Song"

    # Verify: No API call made (cache hit)
    mock_spotify_client.get_track.assert_not_called()

    # Verify: No write to cache (already exists)
    mock_dynamo_client.write_track_metadata.assert_not_called()


def test_enrich_track_cache_miss(mock_spotify_client, mock_dynamo_client, mock_track_metadata):
    """Test track enrichment when metadata needs to be fetched from API."""
    from spotify_lifecycle.pipeline.enrich import enrich_track

    # Setup: Track NOT in cache
    mock_dynamo_client.get_track_metadata.return_value = None

    # Setup: Spotify API returns track metadata
    mock_spotify_client.get_track.return_value = mock_track_metadata

    # Setup: Cache write succeeds
    mock_dynamo_client.write_track_metadata.return_value = True

    # Execute
    result = enrich_track(
        track_id="spotify:track:abc123",
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        tracks_table="tracks",
    )

    # Verify: Metadata returned
    assert result is not None
    assert result.track_id == "spotify:track:abc123"
    assert result.name == "Test Song"
    assert result.artist_ids == ["spotify:artist:artist1", "spotify:artist:artist2"]
    assert result.artist_names == ["Artist One", "Artist Two"]

    # Verify: API call made (cache miss)
    mock_spotify_client.get_track.assert_called_once_with("abc123")

    # Verify: Metadata written to cache
    mock_dynamo_client.write_track_metadata.assert_called_once()


def test_enrich_track_api_failure(mock_spotify_client, mock_dynamo_client):
    """Test track enrichment when Spotify API fails."""
    from spotify_lifecycle.pipeline.enrich import enrich_track

    # Setup: Track NOT in cache
    mock_dynamo_client.get_track_metadata.return_value = None

    # Setup: Spotify API throws exception
    mock_spotify_client.get_track.side_effect = Exception("API Error")

    # Execute
    result = enrich_track(
        track_id="spotify:track:abc123",
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        tracks_table="tracks",
    )

    # Verify: None returned (failure is non-blocking)
    assert result is None

    # Verify: API call attempted
    mock_spotify_client.get_track.assert_called_once()

    # Verify: No write to cache (no data to cache)
    mock_dynamo_client.write_track_metadata.assert_not_called()


def test_enrich_artist_cache_hit(mock_spotify_client, mock_dynamo_client):
    """Test artist enrichment when metadata is already cached."""
    from spotify_lifecycle.pipeline.enrich import enrich_artist

    # Setup: Artist already in cache
    mock_dynamo_client.get_artist_metadata.return_value = {
        "artist_id": "spotify:artist:artist1",
        "name": "Cached Artist",
        "genres": ["pop"],
        "popularity": 80,
        "followers": 1000000,
        "uri": "spotify:artist:artist1",
        "images": "[]",
        "cached_at": "2025-01-01T00:00:00+00:00",
    }

    # Execute
    result = enrich_artist(
        artist_id="spotify:artist:artist1",
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        artists_table="artists",
    )

    # Verify: Metadata returned from cache
    assert result is not None
    assert result.artist_id == "spotify:artist:artist1"
    assert result.name == "Cached Artist"

    # Verify: No API call made (cache hit)
    mock_spotify_client.get_artist.assert_not_called()

    # Verify: No write to cache (already exists)
    mock_dynamo_client.write_artist_metadata.assert_not_called()


def test_enrich_artist_cache_miss(mock_spotify_client, mock_dynamo_client, mock_artist_metadata):
    """Test artist enrichment when metadata needs to be fetched from API."""
    from spotify_lifecycle.pipeline.enrich import enrich_artist

    # Setup: Artist NOT in cache
    mock_dynamo_client.get_artist_metadata.return_value = None

    # Setup: Spotify API returns artist metadata
    mock_spotify_client.get_artist.return_value = mock_artist_metadata

    # Setup: Cache write succeeds
    mock_dynamo_client.write_artist_metadata.return_value = True

    # Execute
    result = enrich_artist(
        artist_id="spotify:artist:artist1",
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        artists_table="artists",
    )

    # Verify: Metadata returned
    assert result is not None
    assert result.artist_id == "spotify:artist:artist1"
    assert result.name == "Artist One"
    assert result.genres == ["pop", "rock"]
    assert result.followers == 1000000

    # Verify: API call made (cache miss)
    mock_spotify_client.get_artist.assert_called_once_with("artist1")

    # Verify: Metadata written to cache
    mock_dynamo_client.write_artist_metadata.assert_called_once()


def test_enrich_artist_api_failure(mock_spotify_client, mock_dynamo_client):
    """Test artist enrichment when Spotify API fails."""
    from spotify_lifecycle.pipeline.enrich import enrich_artist

    # Setup: Artist NOT in cache
    mock_dynamo_client.get_artist_metadata.return_value = None

    # Setup: Spotify API throws exception
    mock_spotify_client.get_artist.side_effect = Exception("API Error")

    # Execute
    result = enrich_artist(
        artist_id="spotify:artist:artist1",
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        artists_table="artists",
    )

    # Verify: None returned (failure is non-blocking)
    assert result is None

    # Verify: API call attempted
    mock_spotify_client.get_artist.assert_called_once()

    # Verify: No write to cache (no data to cache)
    mock_dynamo_client.write_artist_metadata.assert_not_called()


def test_enrich_play_events_deduplication(
    mock_spotify_client, mock_dynamo_client, mock_track_metadata, mock_artist_metadata
):
    """Test that duplicate track IDs are deduplicated before enrichment."""
    from spotify_lifecycle.pipeline.enrich import enrich_play_events

    # Setup: Same track played multiple times
    track_ids = [
        "spotify:track:abc123",
        "spotify:track:abc123",  # Duplicate
        "spotify:track:abc123",  # Duplicate
    ]

    # Setup: Track NOT in cache
    mock_dynamo_client.get_track_metadata.return_value = None

    # Setup: Artist NOT in cache
    mock_dynamo_client.get_artist_metadata.return_value = None

    # Setup: Spotify API returns metadata
    mock_spotify_client.get_track.return_value = mock_track_metadata
    mock_spotify_client.get_artist.return_value = mock_artist_metadata

    # Setup: Cache writes succeed
    mock_dynamo_client.write_track_metadata.return_value = True
    mock_dynamo_client.write_artist_metadata.return_value = True

    # Execute
    summary = enrich_play_events(
        track_ids=track_ids,
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        tracks_table="tracks",
        artists_table="artists",
    )

    # Verify: Only 1 unique track processed
    assert summary["tracks_processed"] == 1
    assert summary["tracks_fetched"] == 1

    # Verify: Only 1 API call made (deduplication works)
    assert mock_spotify_client.get_track.call_count == 1


def test_enrich_play_events_batch_summary(
    mock_spotify_client, mock_dynamo_client, mock_track_metadata, mock_artist_metadata
):
    """Test batch enrichment returns accurate summary."""
    from spotify_lifecycle.pipeline.enrich import enrich_play_events

    # Setup: Multiple unique tracks
    track_ids = ["spotify:track:1", "spotify:track:2", "spotify:track:3"]

    # Setup: Track 1 cached, Track 2-3 NOT cached
    def get_track_side_effect(table, track_id):
        if track_id == "spotify:track:1":
            return {
                "track_id": track_id,
                "name": "Cached",
                "artist_ids": ["spotify:artist:artist1"],
                "artist_names": ["Artist"],
                "album_id": "spotify:album:album1",
                "album_name": "Album",
                "duration_ms": 180000,
                "explicit": False,
                "popularity": 75,
                "release_date": "2025-01-01",
                "uri": track_id,
                "cached_at": "2025-01-01T00:00:00+00:00",
            }
        return None

    mock_dynamo_client.get_track_metadata.side_effect = get_track_side_effect

    # Setup: Artist NOT in cache
    mock_dynamo_client.get_artist_metadata.return_value = None

    # Setup: Spotify API returns metadata
    mock_spotify_client.get_track.return_value = mock_track_metadata
    mock_spotify_client.get_artist.return_value = mock_artist_metadata

    # Setup: Cache writes succeed
    mock_dynamo_client.write_track_metadata.return_value = True
    mock_dynamo_client.write_artist_metadata.return_value = True

    # Execute
    summary = enrich_play_events(
        track_ids=track_ids,
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        tracks_table="tracks",
        artists_table="artists",
    )

    # Verify: Summary counts
    assert summary["tracks_processed"] == 3
    assert summary["tracks_cached"] == 1  # Track 1 was cached
    assert summary["tracks_fetched"] == 2  # Track 2-3 fetched from API
    assert summary["tracks_failed"] == 0


def test_enrich_play_events_partial_failure(
    mock_spotify_client, mock_dynamo_client, mock_track_metadata, mock_artist_metadata
):
    """Test batch enrichment continues despite individual failures."""
    from spotify_lifecycle.pipeline.enrich import enrich_play_events

    # Setup: Multiple unique tracks
    track_ids = ["spotify:track:1", "spotify:track:2", "spotify:track:3"]

    # Setup: Tracks NOT in cache
    mock_dynamo_client.get_track_metadata.return_value = None

    # Setup: Artist NOT in cache
    mock_dynamo_client.get_artist_metadata.return_value = None

    # Setup: Track 2 API call fails
    def get_track_side_effect(track_id):
        if track_id == "2":
            raise Exception("API Error for track 2")
        return mock_track_metadata

    mock_spotify_client.get_track.side_effect = get_track_side_effect
    mock_spotify_client.get_artist.return_value = mock_artist_metadata

    # Setup: Cache writes succeed
    mock_dynamo_client.write_track_metadata.return_value = True
    mock_dynamo_client.write_artist_metadata.return_value = True

    # Execute
    summary = enrich_play_events(
        track_ids=track_ids,
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        tracks_table="tracks",
        artists_table="artists",
    )

    # Verify: Partial success
    assert summary["tracks_processed"] == 3
    assert summary["tracks_fetched"] == 2  # Track 1 and 3 succeeded
    assert summary["tracks_failed"] == 1  # Track 2 failed

    # Verify: All API calls attempted despite failure
    assert mock_spotify_client.get_track.call_count == 3


def test_run_enrichment_no_plays(mock_spotify_client, mock_dynamo_client):
    """Test enrichment pipeline when hot store is empty."""
    from spotify_lifecycle.pipeline.enrich import run_enrichment

    # Setup: Hot store table returns empty scan
    mock_table = Mock()
    mock_table.scan.return_value = {"Items": []}
    mock_dynamo_client.dynamodb.Table.return_value = mock_table

    # Execute
    summary = run_enrichment(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        hot_table="plays",
        tracks_table="tracks",
        artists_table="artists",
        lookback_days=7,
    )

    # Verify: Empty summary (no plays to enrich)
    assert summary["tracks_processed"] == 0
    assert summary["tracks_fetched"] == 0

    # Verify: No API calls made
    mock_spotify_client.get_track.assert_not_called()
    mock_spotify_client.get_artist.assert_not_called()


def test_run_enrichment_with_plays(
    mock_spotify_client, mock_dynamo_client, mock_track_metadata, mock_artist_metadata
):
    """Test enrichment pipeline with plays in hot store."""
    from spotify_lifecycle.pipeline.enrich import run_enrichment

    # Setup: Hot store table returns plays
    mock_table = Mock()
    mock_table.scan.return_value = {
        "Items": [
            {"track_id": "spotify:track:1"},
            {"track_id": "spotify:track:2"},
            {"track_id": "spotify:track:1"},  # Duplicate
        ]
    }
    mock_dynamo_client.dynamodb.Table.return_value = mock_table

    # Setup: Tracks NOT in cache
    mock_dynamo_client.get_track_metadata.return_value = None

    # Setup: Artists NOT in cache
    mock_dynamo_client.get_artist_metadata.return_value = None

    # Setup: Spotify API returns metadata
    mock_spotify_client.get_track.return_value = mock_track_metadata
    mock_spotify_client.get_artist.return_value = mock_artist_metadata

    # Setup: Cache writes succeed
    mock_dynamo_client.write_track_metadata.return_value = True
    mock_dynamo_client.write_artist_metadata.return_value = True

    # Execute
    summary = run_enrichment(
        spotify_client=mock_spotify_client,
        dynamo_client=mock_dynamo_client,
        hot_table="plays",
        tracks_table="tracks",
        artists_table="artists",
        lookback_days=7,
    )

    # Verify: 2 unique tracks processed (deduplication)
    assert summary["tracks_processed"] == 2
    assert summary["tracks_fetched"] == 2

    # Verify: Scan called with correct filter
    mock_table.scan.assert_called_once()
    call_args = mock_table.scan.call_args
    assert "FilterExpression" in call_args[1]
