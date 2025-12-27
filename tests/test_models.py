"""Tests for models."""

from datetime import datetime

from spotify_lifecycle.models import ArtistMetadata, PlayEvent, TrackMetadata


def test_play_event():
    """Test PlayEvent model."""
    event = PlayEvent(
        track_id="12345",
        played_at=datetime.now(),
        user_id="user123",
        context="spotify:playlist:abc",
    )

    assert event.track_id == "12345"
    assert event.user_id == "user123"
    assert event.context == "spotify:playlist:abc"


def test_track_metadata():
    """Test TrackMetadata model."""
    metadata = TrackMetadata(
        track_id="12345",
        name="Test Song",
        artist_ids=["artist1", "artist2"],
        album_id="album123",
        album_name="Test Album",
        duration_ms=180000,
        explicit=False,
        popularity=75,
        uri="spotify:track:12345",
    )

    assert metadata.name == "Test Song"
    assert len(metadata.artist_ids) == 2
    assert metadata.duration_ms == 180000


def test_artist_metadata():
    """Test ArtistMetadata model."""
    metadata = ArtistMetadata(
        artist_id="artist123",
        name="Test Artist",
        genres=["pop", "rock"],
        popularity=80,
        uri="spotify:artist:artist123",
        images=[],
    )

    assert metadata.name == "Test Artist"
    assert "pop" in metadata.genres
    assert metadata.popularity == 80
