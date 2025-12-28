"""Tests for utility functions (hashing and time).

Tests focus on:
- Deterministic key generation (same input → same output)
- Edge cases (null values, edge timestamps, etc.)
- Format validation (URI validation, ISO week format)
- Time window calculations (overlap, truncation, etc.)
"""

from datetime import datetime, timezone

import pytest

from spotify_lifecycle.utils.hashing import (
    make_artist_cache_key,
    make_play_id,
    make_playlist_state_key,
    make_track_cache_key,
    make_week_id,
    sha256_hash,
)
from spotify_lifecycle.utils.time import (
    apply_overlap_window,
    datetime_to_iso,
    days_ago,
    get_date_range,
    hours_ago,
    iso_to_datetime,
    make_partition_key,
    minutes_ago,
    truncate_to_day,
    truncate_to_hour,
    utc_now,
)


class TestHashingUtils:
    """Tests for hashing utilities."""

    def test_sha256_hash_deterministic(self):
        """Test SHA256 hash is deterministic."""
        data = "test_string"
        hash1 = sha256_hash(data)
        hash2 = sha256_hash(data)

        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 produces 64-char hex

    def test_sha256_hash_different_inputs(self):
        """Test different inputs produce different hashes."""
        hash1 = sha256_hash("input1")
        hash2 = sha256_hash("input2")

        assert hash1 != hash2

    def test_make_play_id_deterministic(self):
        """Test play_id is deterministic for same inputs."""
        played_at = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        track_id = "spotify:track:6rqhFgbbKwnb9MLmUQDhG6"
        context = "spotify:playlist:37i9dQZF1DXcBWIGoYBM5M"

        play_id1 = make_play_id(played_at, track_id, context)
        play_id2 = make_play_id(played_at, track_id, context)

        assert play_id1 == play_id2
        assert len(play_id1) == 64  # SHA256

    def test_make_play_id_different_context(self):
        """Test different contexts produce different play_ids."""
        played_at = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        track_id = "spotify:track:6rqhFgbbKwnb9MLmUQDhG6"

        play_id1 = make_play_id(played_at, track_id, "spotify:playlist:A")
        play_id2 = make_play_id(played_at, track_id, "spotify:playlist:B")

        assert play_id1 != play_id2

    def test_make_play_id_null_context(self):
        """Test null context is normalized consistently."""
        played_at = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        track_id = "spotify:track:6rqhFgbbKwnb9MLmUQDhG6"

        # All these should produce same play_id
        play_id1 = make_play_id(played_at, track_id, None)
        play_id2 = make_play_id(played_at, track_id, "")
        play_id3 = make_play_id(played_at, track_id, "   ")  # Whitespace only

        assert play_id1 == play_id2 == play_id3

    def test_make_track_cache_key_valid(self):
        """Test track cache key with valid URI."""
        track_id = "spotify:track:6rqhFgbbKwnb9MLmUQDhG6"
        key = make_track_cache_key(track_id)

        assert key == track_id  # No transformation

    def test_make_track_cache_key_invalid(self):
        """Test track cache key fails with invalid URI."""
        with pytest.raises(ValueError) as excinfo:
            make_track_cache_key("invalid:track:12345")

        assert "Invalid track ID" in str(excinfo.value)

    def test_make_artist_cache_key_valid(self):
        """Test artist cache key with valid URI."""
        artist_id = "spotify:artist:0OdUWJ0sBjDrqHygGUXeCF"
        key = make_artist_cache_key(artist_id)

        assert key == artist_id  # No transformation

    def test_make_artist_cache_key_invalid(self):
        """Test artist cache key fails with invalid URI."""
        with pytest.raises(ValueError) as excinfo:
            make_artist_cache_key("invalid:artist:12345")

        assert "Invalid artist ID" in str(excinfo.value)

    def test_make_week_id(self):
        """Test ISO week ID generation."""
        # Test known week
        date = datetime(2025, 12, 27, tzinfo=timezone.utc)  # Week 52
        week_id = make_week_id(date)

        assert week_id == "2025-W52"

    def test_make_week_id_year_transition(self):
        """Test week ID handles year transitions correctly."""
        # December 30, 2024 is Monday of week 1, 2025 (ISO calendar)
        date = datetime(2024, 12, 30, tzinfo=timezone.utc)
        week_id = make_week_id(date)

        assert week_id == "2025-W01"

    def test_make_playlist_state_key(self):
        """Test playlist state key generation."""
        week_id = "2025-W52"
        state_key = make_playlist_state_key(week_id)

        assert state_key == "weekly_playlist_2025_W52"


class TestTimeUtils:
    """Tests for time utilities."""

    def test_utc_now_is_aware(self):
        """Test utc_now returns timezone-aware datetime."""
        now = utc_now()

        assert now.tzinfo is not None
        assert now.tzinfo == timezone.utc

    def test_days_ago(self):
        """Test days_ago calculation."""
        reference = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        result = days_ago(7, from_time=reference)

        expected = datetime(2025, 12, 20, 14, 30, 0, tzinfo=timezone.utc)
        assert result == expected

    def test_hours_ago(self):
        """Test hours_ago calculation."""
        reference = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        result = hours_ago(2, from_time=reference)

        expected = datetime(2025, 12, 27, 12, 30, 0, tzinfo=timezone.utc)
        assert result == expected

    def test_minutes_ago(self):
        """Test minutes_ago calculation."""
        reference = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        result = minutes_ago(5, from_time=reference)

        expected = datetime(2025, 12, 27, 14, 25, 0, tzinfo=timezone.utc)
        assert result == expected

    def test_iso_to_datetime_with_z(self):
        """Test ISO string parsing with Z suffix."""
        iso_string = "2025-12-27T14:30:00Z"
        dt = iso_to_datetime(iso_string)

        assert dt.year == 2025
        assert dt.month == 12
        assert dt.day == 27
        assert dt.hour == 14
        assert dt.minute == 30
        assert dt.tzinfo == timezone.utc

    def test_iso_to_datetime_with_offset(self):
        """Test ISO string parsing with +00:00 offset."""
        iso_string = "2025-12-27T14:30:00+00:00"
        dt = iso_to_datetime(iso_string)

        assert dt.year == 2025
        assert dt.tzinfo == timezone.utc

    def test_iso_to_datetime_invalid(self):
        """Test ISO string parsing fails with invalid format."""
        with pytest.raises(ValueError) as excinfo:
            iso_to_datetime("invalid_timestamp")

        assert "Invalid ISO 8601 timestamp" in str(excinfo.value)

    def test_datetime_to_iso(self):
        """Test datetime to ISO string conversion."""
        dt = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        iso_string = datetime_to_iso(dt)

        assert iso_string == "2025-12-27T14:30:00Z"

    def test_datetime_to_iso_round_trip(self):
        """Test datetime → ISO → datetime round-trip."""
        original = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        iso_string = datetime_to_iso(original)
        restored = iso_to_datetime(iso_string)

        assert original == restored

    def test_make_partition_key(self):
        """Test partition key generation."""
        dt = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        key = make_partition_key(dt)

        assert key == "dt=2025-12-27"

    def test_make_partition_key_custom_prefix(self):
        """Test partition key with custom prefix."""
        dt = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        key = make_partition_key(dt, prefix="date")

        assert key == "date=2025-12-27"

    def test_get_date_range(self):
        """Test date range generation."""
        start = datetime(2025, 12, 25, tzinfo=timezone.utc)
        end = datetime(2025, 12, 27, tzinfo=timezone.utc)
        dates = get_date_range(start, end)

        assert len(dates) == 3  # 25, 26, 27
        assert dates[0].day == 25
        assert dates[1].day == 26
        assert dates[2].day == 27

    def test_get_date_range_single_day(self):
        """Test date range with same start and end."""
        start = datetime(2025, 12, 27, tzinfo=timezone.utc)
        end = datetime(2025, 12, 27, tzinfo=timezone.utc)
        dates = get_date_range(start, end)

        assert len(dates) == 1
        assert dates[0].day == 27

    def test_apply_overlap_window(self):
        """Test overlap window application."""
        cursor_time = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        fetch_time = apply_overlap_window(cursor_time, overlap_minutes=5)

        expected = datetime(2025, 12, 27, 14, 25, 0, tzinfo=timezone.utc)
        assert fetch_time == expected

    def test_truncate_to_hour(self):
        """Test truncation to hour boundary."""
        dt = datetime(2025, 12, 27, 14, 35, 42, tzinfo=timezone.utc)
        truncated = truncate_to_hour(dt)

        expected = datetime(2025, 12, 27, 14, 0, 0, tzinfo=timezone.utc)
        assert truncated == expected

    def test_truncate_to_day(self):
        """Test truncation to day boundary."""
        dt = datetime(2025, 12, 27, 14, 35, 42, tzinfo=timezone.utc)
        truncated = truncate_to_day(dt)

        expected = datetime(2025, 12, 27, 0, 0, 0, tzinfo=timezone.utc)
        assert truncated == expected


class TestIdempotencyProperties:
    """Tests for idempotency properties (consistency guarantees)."""

    def test_play_id_consistency_across_retries(self):
        """Test play_id remains consistent across multiple calls (retry safety)."""
        played_at = datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc)
        track_id = "spotify:track:6rqhFgbbKwnb9MLmUQDhG6"
        context = "spotify:playlist:37i9dQZF1DXcBWIGoYBM5M"

        # Simulate multiple retries
        play_ids = [make_play_id(played_at, track_id, context) for _ in range(10)]

        # All should be identical
        assert len(set(play_ids)) == 1

    def test_different_timestamps_different_play_ids(self):
        """Test different timestamps produce different play_ids."""
        track_id = "spotify:track:6rqhFgbbKwnb9MLmUQDhG6"
        context = "spotify:playlist:37i9dQZF1DXcBWIGoYBM5M"

        play_id1 = make_play_id(
            datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc), track_id, context
        )
        play_id2 = make_play_id(
            datetime(2025, 12, 27, 14, 31, 0, tzinfo=timezone.utc), track_id, context
        )

        assert play_id1 != play_id2

    def test_week_id_consistency(self):
        """Test week_id is consistent for dates in same week."""
        # All these dates are in week 52, 2025
        dates = [
            datetime(2025, 12, 22, tzinfo=timezone.utc),  # Monday
            datetime(2025, 12, 25, tzinfo=timezone.utc),  # Thursday
            datetime(2025, 12, 28, tzinfo=timezone.utc),  # Sunday
        ]

        week_ids = [make_week_id(date) for date in dates]

        # All should be same week
        assert len(set(week_ids)) == 1
        assert week_ids[0] == "2025-W52"
