"""Tests for aggregation pipeline (analyst stage)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from spotify_lifecycle.models import DashboardData
from spotify_lifecycle.pipeline.aggregate import build_dashboard_data


def make_summary(date: datetime, track_counts: dict[str, int]) -> dict:
    return {
        "version": "1.0.0",
        "date": date.date().isoformat(),
        "generated_at": date.isoformat(),
        "total_plays": sum(track_counts.values()),
        "track_counts": track_counts,
    }


def stub_cold_store(summaries: list[dict]) -> MagicMock:
    cold_store = MagicMock()
    dates = [datetime.fromisoformat(summary["date"]) for summary in summaries] or [datetime.now()]
    cold_store.list_daily_summary_dates.return_value = dates
    cold_store.read_daily_summaries.return_value = summaries
    return cold_store


def stub_dashboard_store() -> MagicMock:
    store = MagicMock()
    store.write_dashboard_data = MagicMock()
    return store


class TestAggregationLogic:
    """Test core aggregation logic with summary-based data."""

    def test_top_tracks_sorted_by_play_count(self):
        base_time = datetime.now(ZoneInfo("America/Los_Angeles"))
        summaries = [
            make_summary(
                base_time,
                {
                    "spotify:track:track1": 5,
                    "spotify:track:track2": 10,
                    "spotify:track:track3": 3,
                },
            )
        ]

        dynamo_client = MagicMock()
        dynamo_client.query_plays_by_date_range.return_value = []
        dynamo_client.get_track_metadata.side_effect = lambda table, tid: {
            "name": f"Track {tid[-1]}",
            "artist_ids": [f"spotify:artist:artist{tid[-1]}"],
            "album_name": "Test Album",
        }
        dynamo_client.get_artist_metadata.return_value = {"name": "Test Artist", "genres": []}

        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            dashboard_store=stub_dashboard_store(),
            cold_store=stub_cold_store(summaries),
            hot_table_name="hot",
            tracks_table_name="tracks",
            artists_table_name="artists",
            raw_bucket_name="raw",
            dashboard_bucket_name="dash",
            lookback_days=7,
        )

        top_tracks = result.windows["all_time"]["top_tracks"]
        assert top_tracks[0]["track_id"] == "spotify:track:track2"
        assert top_tracks[1]["track_id"] == "spotify:track:track1"
        assert top_tracks[2]["track_id"] == "spotify:track:track3"

    def test_daily_trends_include_all_days(self):
        base_time = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
        summaries = [
            make_summary(base_time, {"spotify:track:track1": 1}),
            make_summary(base_time - timedelta(days=2), {"spotify:track:track1": 1}),
        ]

        dynamo_client = MagicMock()
        dynamo_client.query_plays_by_date_range.return_value = []
        dynamo_client.get_track_metadata.return_value = {
            "name": "Test Track",
            "artist_ids": [],
            "album_name": "Test Album",
        }
        dynamo_client.get_artist_metadata.return_value = {"name": "Test Artist", "genres": []}

        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            dashboard_store=stub_dashboard_store(),
            cold_store=stub_cold_store(summaries),
            hot_table_name="hot",
            tracks_table_name="tracks",
            artists_table_name="artists",
            raw_bucket_name="raw",
            dashboard_bucket_name="dash",
            lookback_days=3,
        )

        assert len(result.daily_plays) == 4  # lookback + today
        day_counts = {trend["date"]: trend["play_count"] for trend in result.daily_plays}
        missing_day = (base_time - timedelta(days=1)).date().isoformat()
        assert day_counts[missing_day] == 0

    def test_hourly_distribution_includes_all_hours(self):
        pacific = ZoneInfo("America/Los_Angeles")
        base_date = datetime.now(pacific).date()
        summaries = [
            make_summary(datetime.combine(base_date, datetime.min.time(), tzinfo=timezone.utc), {})
        ]

        dynamo_client = MagicMock()
        dynamo_client.query_plays_by_date_range.return_value = [
            {
                "played_at": datetime.combine(base_date, datetime.min.time(), tzinfo=pacific)
                .replace(hour=9)
                .isoformat()
            },
            {
                "played_at": datetime.combine(base_date, datetime.min.time(), tzinfo=pacific)
                .replace(hour=17)
                .isoformat()
            },
        ]
        dynamo_client.get_track_metadata.return_value = {
            "name": "Track",
            "artist_ids": [],
            "album_name": "Album",
        }
        dynamo_client.get_artist_metadata.return_value = {"name": "Artist", "genres": []}

        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            dashboard_store=stub_dashboard_store(),
            cold_store=stub_cold_store(summaries),
            hot_table_name="hot",
            tracks_table_name="tracks",
            artists_table_name="artists",
            raw_bucket_name="raw",
            dashboard_bucket_name="dash",
            lookback_days=1,
        )

        hour_counts = {dist["hour"]: dist["play_count"] for dist in result.hourly_distribution}
        assert hour_counts[9] == 1
        assert hour_counts[17] == 1
        assert hour_counts[0] == 0
        assert hour_counts[23] == 0

    def test_genre_extraction_from_artists(self):
        base_time = datetime.now(ZoneInfo("America/Los_Angeles"))
        summaries = [
            make_summary(
                base_time,
                {"spotify:track:track1": 1, "spotify:track:track2": 1},
            )
        ]

        dynamo_client = MagicMock()
        dynamo_client.query_plays_by_date_range.return_value = []

        def mock_get_track(table, track_id):
            if track_id == "spotify:track:track1":
                return {
                    "name": "Track 1",
                    "artist_ids": ["spotify:artist:artist1"],
                    "album_name": "Album 1",
                }
            return {
                "name": "Track 2",
                "artist_ids": ["spotify:artist:artist2"],
                "album_name": "Album 2",
            }

        dynamo_client.get_track_metadata.side_effect = mock_get_track

        def mock_get_artist(table, artist_id):
            if artist_id == "spotify:artist:artist1":
                return {"name": "Artist 1", "genres": ["rock", "indie"]}
            return {"name": "Artist 2", "genres": ["jazz", "blues"]}

        dynamo_client.get_artist_metadata.side_effect = mock_get_artist

        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            dashboard_store=stub_dashboard_store(),
            cold_store=stub_cold_store(summaries),
            hot_table_name="hot",
            tracks_table_name="tracks",
            artists_table_name="artists",
            raw_bucket_name="raw",
            dashboard_bucket_name="dash",
            lookback_days=1,
        )

        genre_names = [g["genre"] for g in result.windows["all_time"]["top_genres"]]
        assert "rock" in genre_names
        assert "indie" in genre_names
        assert "jazz" in genre_names
        assert "blues" in genre_names

    def test_top_n_limits_enforced(self):
        base_time = datetime.now(ZoneInfo("America/Los_Angeles"))
        track_counts = {f"spotify:track:track{i:03d}": 1 for i in range(100)}
        summaries = [make_summary(base_time, track_counts)]

        dynamo_client = MagicMock()
        dynamo_client.query_plays_by_date_range.return_value = []
        dynamo_client.get_track_metadata.side_effect = lambda table, track_id: {
            "name": f"Track {track_id.split('track')[-1]}",
            "artist_ids": [f"spotify:artist:artist{track_id.split('track')[-1]}"],
            "album_name": "Album",
        }
        dynamo_client.get_artist_metadata.side_effect = lambda table, artist_id: {
            "name": artist_id,
            "genres": [f"genre{int(artist_id.split('artist')[-1]) % 30}"],
        }

        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            dashboard_store=stub_dashboard_store(),
            cold_store=stub_cold_store(summaries),
            hot_table_name="hot",
            tracks_table_name="tracks",
            artists_table_name="artists",
            raw_bucket_name="raw",
            dashboard_bucket_name="dash",
            lookback_days=1,
        )

        assert len(result.windows["all_time"]["top_tracks"]) == 50
        assert len(result.windows["all_time"]["top_artists"]) == 50
        assert len(result.windows["all_time"]["top_genres"]) <= 20

    def test_schema_validation_with_pydantic(self):
        base_time = datetime.now(timezone.utc)
        summaries = [make_summary(base_time, {"spotify:track:track1": 1})]

        dynamo_client = MagicMock()
        dynamo_client.query_plays_by_date_range.return_value = []
        dynamo_client.get_track_metadata.return_value = {
            "name": "Test Track",
            "artist_ids": [],
            "album_name": "Test Album",
        }
        dynamo_client.get_artist_metadata.return_value = {"name": "Test Artist", "genres": []}

        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            dashboard_store=stub_dashboard_store(),
            cold_store=stub_cold_store(summaries),
            hot_table_name="hot",
            tracks_table_name="tracks",
            artists_table_name="artists",
            raw_bucket_name="raw",
            dashboard_bucket_name="dash",
            lookback_days=1,
        )

        assert isinstance(result, DashboardData)
        assert result.version == "1.0.0"
        assert "start" in result.time_range
        assert "end" in result.time_range
        assert "windows" in result.model_dump()

    def test_empty_summaries_produces_valid_output(self):
        dynamo_client = MagicMock()
        dynamo_client.query_plays_by_date_range.return_value = []
        dynamo_client.get_track_metadata.return_value = None
        dynamo_client.get_artist_metadata.return_value = None

        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            dashboard_store=stub_dashboard_store(),
            cold_store=stub_cold_store([]),
            hot_table_name="hot",
            tracks_table_name="tracks",
            artists_table_name="artists",
            raw_bucket_name="raw",
            dashboard_bucket_name="dash",
            lookback_days=3,
        )

        assert result.metadata["total_play_count"] == 0
        assert len(result.top_tracks) == 0
        assert len(result.windows["all_time"]["top_tracks"]) == 0
        assert len(result.daily_plays) == 4  # lookback + today
        assert len(result.hourly_distribution) == 24

    def test_missing_metadata_handled_gracefully(self):
        base_time = datetime.now(ZoneInfo("America/Los_Angeles"))
        summaries = [make_summary(base_time, {"spotify:track:track1": 1})]

        dynamo_client = MagicMock()
        dynamo_client.query_plays_by_date_range.return_value = []
        dynamo_client.get_track_metadata.return_value = None
        dynamo_client.get_artist_metadata.return_value = None

        result = build_dashboard_data(
            dynamo_client=dynamo_client,
            dashboard_store=stub_dashboard_store(),
            cold_store=stub_cold_store(summaries),
            hot_table_name="hot",
            tracks_table_name="tracks",
            artists_table_name="artists",
            raw_bucket_name="raw",
            dashboard_bucket_name="dash",
            lookback_days=1,
        )

        assert result.top_tracks[0]["track_name"] == "Unknown"
        assert result.windows["all_time"]["top_tracks"][0]["track_name"] == "Unknown"

    def test_idempotency_same_input_same_output(self):
        fixed_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        summaries = [make_summary(fixed_time, {"spotify:track:track1": 2})]

        dynamo_client = MagicMock()
        dynamo_client.query_plays_by_date_range.return_value = []
        dynamo_client.get_track_metadata.return_value = {
            "name": "Test Track",
            "artist_ids": [],
            "album_name": "Test Album",
        }
        dynamo_client.get_artist_metadata.return_value = {"name": "Test Artist", "genres": []}

        result1 = build_dashboard_data(
            dynamo_client=dynamo_client,
            dashboard_store=stub_dashboard_store(),
            cold_store=stub_cold_store(summaries),
            hot_table_name="hot",
            tracks_table_name="tracks",
            artists_table_name="artists",
            raw_bucket_name="raw",
            dashboard_bucket_name="dash",
            lookback_days=1,
        )

        result2 = build_dashboard_data(
            dynamo_client=dynamo_client,
            dashboard_store=stub_dashboard_store(),
            cold_store=stub_cold_store(summaries),
            hot_table_name="hot",
            tracks_table_name="tracks",
            artists_table_name="artists",
            raw_bucket_name="raw",
            dashboard_bucket_name="dash",
            lookback_days=1,
        )

        assert (
            result1.windows["all_time"]["top_tracks"] == result2.windows["all_time"]["top_tracks"]
        )
        assert (
            result1.windows["all_time"]["top_artists"] == result2.windows["all_time"]["top_artists"]
        )
