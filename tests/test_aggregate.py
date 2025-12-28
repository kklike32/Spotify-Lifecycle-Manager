"""Tests for aggregation pipeline (analyst stage).

These tests verify that dashboard analytics are computed correctly using
deterministic fixtures. All tests use mocked storage to avoid cloud dependencies.

Test Coverage:
- Top-N sorting (tracks, artists, genres)
- Time-window filtering (90-day lookback)
- Daily trends (ensure all days present, including zero counts)
- Hourly distribution (0-23 hours, including zeros)
- Metadata enrichment (track → artist → genre)
- Schema validation (Pydantic DashboardData)
- Edge cases (empty data, missing metadata)
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from spotify_lifecycle.models import DashboardData
from spotify_lifecycle.pipeline.aggregate import build_dashboard_data


class TestAggregationLogic:
    """Test core aggregation logic with fixture-based data."""

    def test_top_tracks_sorted_by_play_count(self):
        """Verify top tracks are sorted correctly by play count."""
        # Setup: Mock storage with controlled play data
        dynamo_client = MagicMock()
        s3_client = MagicMock()

        # Create plays: track1=5, track2=10, track3=3
        base_time = datetime.now(timezone.utc)
        plays = []
        for i in range(5):
            plays.append(
                {
                    "track_id": "spotify:track:track1",
                    "played_at": (base_time - timedelta(hours=i)).isoformat(),
                }
            )
        for i in range(10):
            plays.append(
                {
                    "track_id": "spotify:track:track2",
                    "played_at": (base_time - timedelta(hours=i)).isoformat(),
                }
            )
        for i in range(3):
            plays.append(
                {
                    "track_id": "spotify:track:track3",
                    "played_at": (base_time - timedelta(hours=i)).isoformat(),
                }
            )

        dynamo_client.query_plays_by_date_range.return_value = plays

        # Mock metadata
        def mock_get_track(table, track_id):
            return {
                "name": f"Track {track_id[-1]}",
                "artist_ids": [f"spotify:artist:artist{track_id[-1]}"],
                "album_name": "Test Album",
            }

        dynamo_client.get_track_metadata.side_effect = mock_get_track
        dynamo_client.get_artist_metadata.return_value = {"name": "Test Artist", "genres": []}

        # Execute
        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            hot_table_name="test_hot_table",
            tracks_table_name="test_tracks_table",
            artists_table_name="test_artists_table",
            dashboard_bucket_name="test_bucket",
            lookback_days=7,
        )

        # Verify: Top tracks sorted by count (track2=10, track1=5, track3=3)
        assert result.top_tracks[0]["track_id"] == "spotify:track:track2"
        assert result.top_tracks[0]["play_count"] == 10
        assert result.top_tracks[1]["track_id"] == "spotify:track:track1"
        assert result.top_tracks[1]["play_count"] == 5
        assert result.top_tracks[2]["track_id"] == "spotify:track:track3"
        assert result.top_tracks[2]["play_count"] == 3

    def test_daily_trends_include_all_days(self):
        """Verify daily trends include all days in range, even with zero plays."""
        dynamo_client = MagicMock()
        s3_client = MagicMock()

        # Create plays only on day 1 and day 3 (skip day 2)
        base_time = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
        plays = [
            {
                "track_id": "spotify:track:track1",
                "played_at": (base_time - timedelta(days=0)).isoformat(),
            },
            {
                "track_id": "spotify:track:track1",
                "played_at": (base_time - timedelta(days=2)).isoformat(),
            },
        ]

        dynamo_client.query_plays_by_date_range.return_value = plays
        dynamo_client.get_track_metadata.return_value = {
            "name": "Test Track",
            "artist_ids": [],
            "album_name": "Test Album",
        }
        dynamo_client.get_artist_metadata.return_value = {"name": "Test Artist", "genres": []}

        # Execute with 3-day lookback
        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            hot_table_name="test_hot_table",
            tracks_table_name="test_tracks_table",
            artists_table_name="test_artists_table",
            dashboard_bucket_name="test_bucket",
            lookback_days=3,
        )

        # Verify: All 4 days present (today + 3 days back = 4 days)
        assert len(result.daily_trends) == 4

        # Verify: Day 2 has zero plays
        day_counts = {trend["date"]: trend["play_count"] for trend in result.daily_trends}
        day_2 = (base_time - timedelta(days=1)).date().isoformat()
        assert day_counts[day_2] == 0

    def test_hourly_distribution_includes_all_hours(self):
        """Verify hourly distribution includes all 24 hours, even with zero plays."""
        dynamo_client = MagicMock()
        s3_client = MagicMock()

        # Create plays only at hour 9 and hour 17 (skip others)
        base_date = datetime.now(timezone.utc).date()
        plays = [
            {
                "track_id": "spotify:track:track1",
                "played_at": datetime.combine(
                    base_date, datetime.min.time().replace(hour=9, tzinfo=timezone.utc)
                ).isoformat(),
            },
            {
                "track_id": "spotify:track:track1",
                "played_at": datetime.combine(
                    base_date, datetime.min.time().replace(hour=17, tzinfo=timezone.utc)
                ).isoformat(),
            },
        ]

        dynamo_client.query_plays_by_date_range.return_value = plays
        dynamo_client.get_track_metadata.return_value = {
            "name": "Test Track",
            "artist_ids": [],
            "album_name": "Test Album",
        }
        dynamo_client.get_artist_metadata.return_value = {"name": "Test Artist", "genres": []}

        # Execute
        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            hot_table_name="test_hot_table",
            tracks_table_name="test_tracks_table",
            artists_table_name="test_artists_table",
            dashboard_bucket_name="test_bucket",
            lookback_days=1,
        )

        # Verify: All 24 hours present
        assert len(result.hourly_distribution) == 24

        # Verify: Hours 9 and 17 have plays, others are zero
        hour_counts = {dist["hour"]: dist["play_count"] for dist in result.hourly_distribution}
        assert hour_counts[9] == 1
        assert hour_counts[17] == 1
        assert hour_counts[0] == 0
        assert hour_counts[23] == 0

    def test_genre_extraction_from_artists(self):
        """Verify genres are correctly extracted from artist metadata."""
        dynamo_client = MagicMock()
        s3_client = MagicMock()

        # Create plays from tracks by different artists
        base_time = datetime.now(timezone.utc)
        plays = [
            {"track_id": "spotify:track:track1", "played_at": base_time.isoformat()},
            {"track_id": "spotify:track:track2", "played_at": base_time.isoformat()},
        ]

        dynamo_client.query_plays_by_date_range.return_value = plays

        # Mock track metadata with different artists
        def mock_get_track(table, track_id):
            if track_id == "spotify:track:track1":
                return {
                    "name": "Track 1",
                    "artist_ids": ["spotify:artist:artist1"],
                    "album_name": "Album 1",
                }
            else:
                return {
                    "name": "Track 2",
                    "artist_ids": ["spotify:artist:artist2"],
                    "album_name": "Album 2",
                }

        dynamo_client.get_track_metadata.side_effect = mock_get_track

        # Mock artist metadata with genres
        def mock_get_artist(table, artist_id):
            if artist_id == "spotify:artist:artist1":
                return {"name": "Artist 1", "genres": ["rock", "indie"]}
            else:
                return {"name": "Artist 2", "genres": ["jazz", "blues"]}

        dynamo_client.get_artist_metadata.side_effect = mock_get_artist

        # Execute
        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            hot_table_name="test_hot_table",
            tracks_table_name="test_tracks_table",
            artists_table_name="test_artists_table",
            dashboard_bucket_name="test_bucket",
            lookback_days=1,
        )

        # Verify: Genres extracted correctly
        genre_names = [g["genre"] for g in result.genre_breakdown]
        assert "rock" in genre_names
        assert "indie" in genre_names
        assert "jazz" in genre_names
        assert "blues" in genre_names

    def test_top_n_limits_enforced(self):
        """Verify top-N limits are enforced (50 tracks, 50 artists, 20 genres)."""
        dynamo_client = MagicMock()
        s3_client = MagicMock()

        # Create 100 unique tracks
        base_time = datetime.now(timezone.utc)
        plays = []
        for i in range(100):
            plays.append(
                {"track_id": f"spotify:track:track{i:03d}", "played_at": base_time.isoformat()}
            )

        dynamo_client.query_plays_by_date_range.return_value = plays

        # Mock metadata for all tracks
        def mock_get_track(table, track_id):
            track_num = int(track_id.split("track")[-1])
            return {
                "name": f"Track {track_num}",
                "artist_ids": [f"spotify:artist:artist{track_num}"],
                "album_name": f"Album {track_num}",
            }

        dynamo_client.get_track_metadata.side_effect = mock_get_track

        # Mock artist metadata with multiple genres
        def mock_get_artist(table, artist_id):
            artist_num = int(artist_id.split("artist")[-1])
            return {
                "name": f"Artist {artist_num}",
                "genres": [f"genre{artist_num % 30}"],
            }  # 30 unique genres

        dynamo_client.get_artist_metadata.side_effect = mock_get_artist

        # Execute
        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            hot_table_name="test_hot_table",
            tracks_table_name="test_tracks_table",
            artists_table_name="test_artists_table",
            dashboard_bucket_name="test_bucket",
            lookback_days=1,
        )

        # Verify: Limits enforced
        assert len(result.top_tracks) == 50  # Limit to 50
        assert len(result.top_artists) == 50  # Limit to 50
        assert len(result.genre_breakdown) <= 20  # Limit to 20

    def test_schema_validation_with_pydantic(self):
        """Verify output conforms to DashboardData schema."""
        dynamo_client = MagicMock()
        s3_client = MagicMock()

        # Minimal valid data
        base_time = datetime.now(timezone.utc)
        plays = [{"track_id": "spotify:track:track1", "played_at": base_time.isoformat()}]

        dynamo_client.query_plays_by_date_range.return_value = plays
        dynamo_client.get_track_metadata.return_value = {
            "name": "Test Track",
            "artist_ids": [],
            "album_name": "Test Album",
        }
        dynamo_client.get_artist_metadata.return_value = {"name": "Test Artist", "genres": []}

        # Execute
        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            hot_table_name="test_hot_table",
            tracks_table_name="test_tracks_table",
            artists_table_name="test_artists_table",
            dashboard_bucket_name="test_bucket",
            lookback_days=1,
        )

        # Verify: Result is valid DashboardData instance
        assert isinstance(result, DashboardData)
        assert result.version == "1.0.0"
        assert isinstance(result.generated_at, datetime)
        assert "start" in result.time_range
        assert "end" in result.time_range

    def test_empty_plays_produces_valid_output(self):
        """Verify system handles zero plays gracefully."""
        dynamo_client = MagicMock()
        s3_client = MagicMock()

        # No plays
        dynamo_client.query_plays_by_date_range.return_value = []

        # Execute
        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            hot_table_name="test_hot_table",
            tracks_table_name="test_tracks_table",
            artists_table_name="test_artists_table",
            dashboard_bucket_name="test_bucket",
            lookback_days=7,
        )

        # Verify: Valid output with zero counts
        assert result.summary["total_plays"] == 0
        assert result.summary["unique_tracks"] == 0
        assert len(result.top_tracks) == 0
        assert len(result.top_artists) == 0
        assert len(result.genre_breakdown) == 0
        # Daily trends still present (all zeros)
        assert len(result.daily_trends) == 8  # 7 days + today
        # Hourly distribution still present (all zeros)
        assert len(result.hourly_distribution) == 24

    def test_missing_metadata_handled_gracefully(self):
        """Verify system handles missing track/artist metadata."""
        dynamo_client = MagicMock()
        s3_client = MagicMock()

        # Create plays
        base_time = datetime.now(timezone.utc)
        plays = [{"track_id": "spotify:track:track1", "played_at": base_time.isoformat()}]

        dynamo_client.query_plays_by_date_range.return_value = plays

        # Mock missing metadata
        dynamo_client.get_track_metadata.return_value = None  # Track not cached
        dynamo_client.get_artist_metadata.return_value = None  # Artist not cached

        # Execute (should not crash)
        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            hot_table_name="test_hot_table",
            tracks_table_name="test_tracks_table",
            artists_table_name="test_artists_table",
            dashboard_bucket_name="test_bucket",
            lookback_days=1,
        )

        # Verify: Output includes placeholder values
        assert len(result.top_tracks) == 1
        assert result.top_tracks[0]["name"] == "Unknown"
        assert result.top_tracks[0]["album_name"] == "Unknown"

    def test_idempotency_same_input_same_output(self):
        """Verify same input data produces same output (deterministic)."""
        dynamo_client = MagicMock()
        s3_client = MagicMock()

        # Fixed play data
        fixed_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        plays = [{"track_id": "spotify:track:track1", "played_at": fixed_time.isoformat()}]

        dynamo_client.query_plays_by_date_range.return_value = plays
        dynamo_client.get_track_metadata.return_value = {
            "name": "Test Track",
            "artist_ids": [],
            "album_name": "Test Album",
        }
        dynamo_client.get_artist_metadata.return_value = {"name": "Test Artist", "genres": []}

        # Execute twice
        result1 = build_dashboard_data(
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            hot_table_name="test_hot_table",
            tracks_table_name="test_tracks_table",
            artists_table_name="test_artists_table",
            dashboard_bucket_name="test_bucket",
            lookback_days=1,
        )

        result2 = build_dashboard_data(
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            hot_table_name="test_hot_table",
            tracks_table_name="test_tracks_table",
            artists_table_name="test_artists_table",
            dashboard_bucket_name="test_bucket",
            lookback_days=1,
        )

        # Verify: Same data (excluding timestamps)
        assert result1.summary == result2.summary
        assert result1.top_tracks == result2.top_tracks
        assert result1.top_artists == result2.top_artists
        assert result1.daily_trends == result2.daily_trends
        assert result1.hourly_distribution == result2.hourly_distribution
