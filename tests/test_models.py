"""Tests for Pydantic data models.

Tests focus on:
- Schema validation (valid inputs pass, invalid fail)
- Field constraints (ranges, formats, required fields)
- Serialization round-trips (JSON encode/decode)
- Edge cases (null values, empty lists, etc.)
"""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from spotify_lifecycle.models import (
    ArtistMetadata,
    DashboardData,
    IngestionState,
    PlayEvent,
    PlaylistState,
    TrackMetadata,
)


class TestPlayEvent:
    """Tests for PlayEvent model."""

    def test_valid_play_event(self):
        """Test valid PlayEvent creation."""
        event = PlayEvent(
            play_id="abc123",
            track_id="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
            played_at=datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc),
            user_id="me",
            context="spotify:playlist:37i9dQZF1DXcBWIGoYBM5M",
        )

        assert event.version == "1.0.0"
        assert event.play_id == "abc123"
        assert event.track_id == "spotify:track:6rqhFgbbKwnb9MLmUQDhG6"
        assert event.user_id == "me"
        assert event.context == "spotify:playlist:37i9dQZF1DXcBWIGoYBM5M"

    def test_play_event_without_context(self):
        """Test PlayEvent with null context (optional field)."""
        event = PlayEvent(
            play_id="abc123",
            track_id="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
            played_at=datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc),
            user_id="me",
        )

        assert event.context is None

    def test_play_event_invalid_track_uri(self):
        """Test PlayEvent fails with invalid track URI."""
        with pytest.raises(ValidationError) as excinfo:
            PlayEvent(
                play_id="abc123",
                track_id="invalid:track:12345",  # Wrong prefix
                played_at=datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc),
                user_id="me",
            )

        assert "Invalid track URI format" in str(excinfo.value)

    def test_play_event_future_timestamp(self):
        """Test PlayEvent fails with future timestamp."""
        future_time = datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        with pytest.raises(ValidationError) as excinfo:
            PlayEvent(
                play_id="abc123",
                track_id="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
                played_at=future_time,
                user_id="me",
            )

        assert "future" in str(excinfo.value).lower()

    def test_play_event_empty_user_id(self):
        """Test PlayEvent fails with empty user_id."""
        with pytest.raises(ValidationError) as excinfo:
            PlayEvent(
                play_id="abc123",
                track_id="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
                played_at=datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc),
                user_id="",  # Empty
            )

        assert "empty" in str(excinfo.value).lower()

    def test_play_event_json_serialization(self):
        """Test PlayEvent JSON round-trip."""
        event = PlayEvent(
            play_id="abc123",
            track_id="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
            played_at=datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc),
            user_id="me",
            context=None,
        )

        # Serialize
        json_data = event.model_dump_json()

        # Deserialize
        event_restored = PlayEvent.model_validate_json(json_data)

        assert event_restored.play_id == event.play_id
        assert event_restored.track_id == event.track_id
        assert event_restored.played_at == event.played_at


class TestTrackMetadata:
    """Tests for TrackMetadata model."""

    def test_valid_track_metadata(self):
        """Test valid TrackMetadata creation."""
        metadata = TrackMetadata(
            track_id="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
            name="Test Song",
            artist_ids=["spotify:artist:0OdUWJ0sBjDrqHygGUXeCF"],
            artist_names=["Test Artist"],
            album_id="spotify:album:1DFixLWuPkv3KT3TnV35m3",
            album_name="Test Album",
            duration_ms=180000,
            explicit=False,
            popularity=75,
            release_date="2023-05-15",
            uri="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
        )

        assert metadata.name == "Test Song"
        assert len(metadata.artist_ids) == 1
        assert metadata.duration_ms == 180000
        assert metadata.popularity == 75

    def test_track_metadata_invalid_duration(self):
        """Test TrackMetadata fails with invalid duration."""
        with pytest.raises(ValidationError):
            TrackMetadata(
                track_id="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
                name="Test Song",
                artist_ids=["spotify:artist:0OdUWJ0sBjDrqHygGUXeCF"],
                artist_names=["Test Artist"],
                album_id="spotify:album:1DFixLWuPkv3KT3TnV35m3",
                album_name="Test Album",
                duration_ms=0,  # Invalid (must be > 0)
                explicit=False,
                popularity=75,
                release_date="2023-05-15",
                uri="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
            )

    def test_track_metadata_invalid_popularity(self):
        """Test TrackMetadata fails with out-of-range popularity."""
        with pytest.raises(ValidationError):
            TrackMetadata(
                track_id="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
                name="Test Song",
                artist_ids=["spotify:artist:0OdUWJ0sBjDrqHygGUXeCF"],
                artist_names=["Test Artist"],
                album_id="spotify:album:1DFixLWuPkv3KT3TnV35m3",
                album_name="Test Album",
                duration_ms=180000,
                explicit=False,
                popularity=150,  # Invalid (must be 0-100)
                release_date="2023-05-15",
                uri="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
            )

    def test_track_metadata_artist_names_mismatch(self):
        """Test TrackMetadata fails when artist_names length doesn't match artist_ids."""
        with pytest.raises(ValidationError) as excinfo:
            TrackMetadata(
                track_id="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
                name="Test Song",
                artist_ids=["spotify:artist:artist1", "spotify:artist:artist2"],
                artist_names=["Artist 1"],  # Only one name (should be two)
                album_id="spotify:album:1DFixLWuPkv3KT3TnV35m3",
                album_name="Test Album",
                duration_ms=180000,
                explicit=False,
                popularity=75,
                release_date="2023-05-15",
                uri="spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
            )

        assert "match" in str(excinfo.value).lower()


class TestArtistMetadata:
    """Tests for ArtistMetadata model."""

    def test_valid_artist_metadata(self):
        """Test valid ArtistMetadata creation."""
        metadata = ArtistMetadata(
            artist_id="spotify:artist:0OdUWJ0sBjDrqHygGUXeCF",
            name="Test Artist",
            genres=["pop", "rock"],
            popularity=80,
            followers=1000000,
            uri="spotify:artist:0OdUWJ0sBjDrqHygGUXeCF",
            images=[{"url": "https://example.com/image.jpg", "height": 640, "width": 640}],
        )

        assert metadata.name == "Test Artist"
        assert "pop" in metadata.genres
        assert metadata.popularity == 80
        assert metadata.followers == 1000000

    def test_artist_metadata_empty_genres(self):
        """Test ArtistMetadata with empty genres (valid case)."""
        metadata = ArtistMetadata(
            artist_id="spotify:artist:0OdUWJ0sBjDrqHygGUXeCF",
            name="Test Artist",
            genres=[],  # Empty list is valid
            popularity=80,
            followers=1000000,
            uri="spotify:artist:0OdUWJ0sBjDrqHygGUXeCF",
        )

        assert len(metadata.genres) == 0

    def test_artist_metadata_invalid_uri(self):
        """Test ArtistMetadata fails with invalid URI."""
        with pytest.raises(ValidationError):
            ArtistMetadata(
                artist_id="invalid:artist:12345",  # Wrong prefix
                name="Test Artist",
                genres=["pop"],
                popularity=80,
                followers=1000000,
                uri="spotify:artist:0OdUWJ0sBjDrqHygGUXeCF",
            )


class TestIngestionState:
    """Tests for IngestionState model."""

    def test_valid_ingestion_state(self):
        """Test valid IngestionState creation."""
        state = IngestionState(
            last_played_at=datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc),
            last_run_at=datetime(2025, 12, 27, 14, 35, 0, tzinfo=timezone.utc),
            last_event_count=50,
            status="success",
        )

        assert state.state_key == "ingest_cursor"
        assert state.status == "success"
        assert state.last_event_count == 50

    def test_ingestion_state_invalid_status(self):
        """Test IngestionState fails with invalid status."""
        with pytest.raises(ValidationError) as excinfo:
            IngestionState(
                last_played_at=datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc),
                last_run_at=datetime(2025, 12, 27, 14, 35, 0, tzinfo=timezone.utc),
                last_event_count=50,
                status="invalid_status",  # Not in valid set
            )

        assert "success" in str(excinfo.value).lower()


class TestPlaylistState:
    """Tests for PlaylistState model."""

    def test_valid_playlist_state(self):
        """Test valid PlaylistState creation."""
        state = PlaylistState(
            state_key="weekly_playlist_2025_W52",
            week_id="2025-W52",
            playlist_id="spotify:playlist:37i9dQZF1DXcBWIGoYBM5M",
            created_at=datetime(2025, 12, 27, 8, 0, 0, tzinfo=timezone.utc),
            track_count=30,
            source_playlist_id="spotify:playlist:source123",
        )

        assert state.week_id == "2025-W52"
        assert state.track_count == 30

    def test_playlist_state_invalid_week_id(self):
        """Test PlaylistState fails with invalid week_id format."""
        with pytest.raises(ValidationError) as excinfo:
            PlaylistState(
                state_key="weekly_playlist_2025_W52",
                week_id="2025-52",  # Wrong format (should be 2025-W52)
                playlist_id="spotify:playlist:37i9dQZF1DXcBWIGoYBM5M",
                created_at=datetime(2025, 12, 27, 8, 0, 0, tzinfo=timezone.utc),
                track_count=30,
                source_playlist_id="spotify:playlist:source123",
            )

        assert "YYYY-WXX" in str(excinfo.value)

    def test_playlist_state_invalid_playlist_uri(self):
        """Test PlaylistState fails with invalid playlist URI."""
        with pytest.raises(ValidationError):
            PlaylistState(
                state_key="weekly_playlist_2025_W52",
                week_id="2025-W52",
                playlist_id="invalid:playlist:123",  # Wrong prefix
                created_at=datetime(2025, 12, 27, 8, 0, 0, tzinfo=timezone.utc),
                track_count=30,
                source_playlist_id="spotify:playlist:source123",
            )


class TestDashboardData:
    """Tests for DashboardData model."""

    def test_valid_dashboard_data(self):
        """Test valid DashboardData creation."""
        data = DashboardData(
            generated_at=datetime(2025, 12, 27, 2, 0, 0, tzinfo=timezone.utc),
            time_range={
                "start": datetime(2025, 1, 1, tzinfo=timezone.utc),
                "end": datetime(2025, 12, 27, tzinfo=timezone.utc),
            },
            metadata={"total_play_count": 5000, "unique_track_count": 1200},
            top_tracks=[{"track_id": "spotify:track:123", "play_count": 50}],
            top_artists=[{"artist_id": "spotify:artist:456", "play_count": 100}],
            daily_plays=[{"date": "2025-12-27", "play_count": 20}],
            hourly_distribution=[{"hour": 0, "play_count": 5}],
            top_genres=[{"genre": "pop", "play_count": 500}],
            time_periods={
                "all_time": {
                    "top_tracks": [],
                    "top_artists": [],
                    "top_genres": [],
                    "total_plays": 5000,
                }
            },
        )

        assert data.version == "1.0.0"
        assert data.metadata["total_play_count"] == 5000
        assert len(data.top_tracks) == 1

    def test_dashboard_data_json_serialization(self):
        """Test DashboardData JSON round-trip."""
        data = DashboardData(
            generated_at=datetime(2025, 12, 27, 2, 0, 0, tzinfo=timezone.utc),
            time_range={
                "start": datetime(2025, 1, 1, tzinfo=timezone.utc),
                "end": datetime(2025, 12, 27, tzinfo=timezone.utc),
            },
            metadata={"total_play_count": 5000},
            top_tracks=[],
            top_artists=[],
            daily_plays=[],
            hourly_distribution=[],
            top_genres=[],
            time_periods={
                "all_time": {
                    "top_tracks": [],
                    "top_artists": [],
                    "top_genres": [],
                    "total_plays": 5000,
                }
            },
        )

        # Serialize
        json_data = data.model_dump_json()

        # Deserialize
        data_restored = DashboardData.model_validate_json(json_data)

        assert data_restored.metadata["total_play_count"] == 5000
