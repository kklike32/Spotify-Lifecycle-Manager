"""Tests for pipeline utilities."""

from datetime import datetime

from spotify_lifecycle.pipeline.ingest import compute_dedup_key


def test_compute_dedup_key():
    """Test dedup key generation."""
    track_id = "spotify:track:12345"
    played_at = datetime(2025, 1, 1, 12, 0, 0)

    key1 = compute_dedup_key(track_id, played_at)
    key2 = compute_dedup_key(track_id, played_at)

    # Same inputs should produce same key
    assert key1 == key2

    # Different track should produce different key
    key3 = compute_dedup_key("spotify:track:67890", played_at)
    assert key1 != key3

    # Different time should produce different key
    played_at_2 = datetime(2025, 1, 1, 12, 0, 1)
    key4 = compute_dedup_key(track_id, played_at_2)
    assert key1 != key4
