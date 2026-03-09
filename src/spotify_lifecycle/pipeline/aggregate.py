"""Analyst stage: Builds analytics and aggregates.

This module computes comprehensive analytics from play history and generates
a single dashboard_data.json artifact. All analytics are precomputed to avoid
live queries in the browser (zero query cost).

Design Principles:
- Compute once, read many times (S3 static serving)
- No unbounded queries (explicit limits on Top-N)
- Schema-validated output (Pydantic DashboardData model)
- Idempotent: safe to retry (same input → same output)

Cost Model:
- Compute: 1-2 seconds Lambda execution (nightly)
- Storage: ~100KB JSON file in S3 (negligible)
- Reads: Free via S3 static website or CloudFront
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from spotify_lifecycle.models import DashboardData
from spotify_lifecycle.pipeline.enrich import enrich_artist, enrich_track
from spotify_lifecycle.spotify.client import SpotifyClient
from spotify_lifecycle.storage.dynamo import DynamoDBClient
from spotify_lifecycle.storage.s3 import S3ColdStore, S3DashboardStore

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
logger = logging.getLogger(__name__)


def build_dashboard_data(
    dynamo_client: DynamoDBClient,
    dashboard_store: S3DashboardStore,
    cold_store: S3ColdStore,
    hot_table_name: str,
    tracks_table_name: str,
    artists_table_name: str,
    raw_bucket_name: str,
    dashboard_bucket_name: str,
    lookback_days: int = 90,
    daily_trend_days: int | None = None,
    hourly_lookback_days: int = 7,
    spotify_client: SpotifyClient | None = None,
) -> DashboardData:
    """Build precomputed dashboard data with multi-window top lists (cost-optimized)."""
    now = datetime.now(PACIFIC_TZ)
    if daily_trend_days is None:
        daily_trend_days = lookback_days

    def _is_summary_plausible(
        summary: dict, max_total_per_day: int = 2000, max_single_track: int = 600
    ) -> bool:
        """Validate a summary is within reasonable bounds to avoid multiplied totals."""
        total = int(summary.get("total_plays", 0))
        counts: dict[str, int] = summary.get("track_counts", {}) or {}
        track_count = len(counts)

        if total < 0:
            logger.warning(
                "summary rejected: negative total",
                extra={"summary_date": summary.get("date"), "total": total},
            )
            return False

        if total > max_total_per_day:
            logger.warning(
                "summary rejected: total exceeds max",
                extra={
                    "summary_date": summary.get("date"),
                    "total": total,
                    "max_total_per_day": max_total_per_day,
                },
            )
            return False

        if track_count == 0 and total > 0:
            logger.warning(
                "summary rejected: total present with zero tracks",
                extra={"summary_date": summary.get("date"), "total": total},
            )
            return False

        # Allow heavy repeat listening of a single track (e.g., 50+ plays).
        if track_count == 1:
            max_track_total = int(next(iter(counts.values()), 0))
            if max_track_total > max_single_track:
                logger.warning(
                    "summary rejected: single-track total exceeds max",
                    extra={
                        "summary_date": summary.get("date"),
                        "total": total,
                        "max_single_track": max_single_track,
                    },
                )
                return False
            return True

        # For multi-track days, flag extreme averages that hint at duplicated merges.
        avg_per_track = total / track_count if track_count else 0
        if avg_per_track > max_single_track:
            logger.warning(
                "summary rejected: average per track exceeds max_single_track",
                extra={
                    "summary_date": summary.get("date"),
                    "average": avg_per_track,
                    "track_count": track_count,
                    "max_single_track": max_single_track,
                },
            )
            return False

        return True

    summary_dates = sorted(cold_store.list_daily_summary_dates(raw_bucket_name))
    summary_start_date = summary_dates[0].date() if summary_dates else now.date()
    summary_end_date = now.date()

    # Read all available summaries once; cheap (one GET per day with data)
    all_summaries = cold_store.read_daily_summaries(
        raw_bucket_name,
        datetime.combine(summary_start_date, datetime.min.time()).astimezone(timezone.utc),
        datetime.combine(summary_end_date, datetime.min.time()).astimezone(timezone.utc),
    )
    validated_summaries: list[dict] = []
    for summary in all_summaries:
        summary_date = summary.get("date")
        if not summary_date:
            logger.warning("summary rejected: missing date", extra={"summary": summary})
            continue
        if _is_summary_plausible(summary):
            validated_summaries.append(summary)
        else:
            logger.error(
                "dropping implausible summary",
                extra={"summary_date": summary_date, "total_plays": summary.get("total_plays")},
            )

    summary_lookup = {
        datetime.fromisoformat(summary["date"]).date(): summary for summary in validated_summaries
    }

    window_specs = {
        "all_time": {"start": summary_start_date, "end": summary_end_date},
        "year_to_date": {
            "start": summary_end_date.replace(month=1, day=1),
            "end": summary_end_date,
        },
        "last_30_days": {"start": summary_end_date - timedelta(days=30), "end": summary_end_date},
        "last_7_days": {"start": summary_end_date - timedelta(days=7), "end": summary_end_date},
    }

    window_counts: dict[str, dict] = {}
    all_track_ids: set[str] = set()

    for key, spec in window_specs.items():
        start_date = spec["start"]
        end_date = spec["end"]
        if start_date > end_date:
            start_date = end_date

        track_counts: dict[str, int] = defaultdict(int)
        total_plays = 0

        for summary in validated_summaries:
            summary_date = datetime.fromisoformat(summary["date"]).date()
            if start_date <= summary_date <= end_date:
                for track_id, count in summary.get("track_counts", {}).items():
                    track_counts[track_id] += int(count)
                total_plays += int(summary.get("total_plays", 0))

        all_track_ids.update(track_counts.keys())
        window_counts[key] = {
            "start": start_date,
            "end": end_date,
            "track_counts": track_counts,
            "total_plays": total_plays,
        }

    track_metadata_cache: dict[str, dict] = {}
    artist_metadata_cache: dict[str, dict] = {}
    artist_ids: set[str] = set()
    enriched_count = 0

    for track_id in all_track_ids:
        metadata = dynamo_client.get_track_metadata(tracks_table_name, track_id) or {}

        # If metadata is missing and we have a Spotify client, enrich it on-the-fly
        if (not metadata or not metadata.get("name")) and spotify_client:
            logger.info(
                f"Track metadata missing for {track_id}, enriching on-the-fly",
                extra={"track_id": track_id},
            )
            enriched_metadata = enrich_track(
                track_id=track_id,
                spotify_client=spotify_client,
                dynamo_client=dynamo_client,
                tracks_table=tracks_table_name,
            )
            if enriched_metadata:
                # Convert Pydantic model to dict for cache
                metadata = enriched_metadata.model_dump()
                enriched_count += 1
                logger.info(
                    f"Successfully enriched track: {metadata.get('name')}",
                    extra={"track_id": track_id, "track_name": metadata.get("name")},
                )
            else:
                logger.error(f"Failed to enrich track {track_id}", extra={"track_id": track_id})

        track_metadata_cache[track_id] = metadata
        for artist_id in metadata.get("artist_ids", []):
            artist_ids.add(artist_id)

    for artist_id in artist_ids:
        artist_meta = dynamo_client.get_artist_metadata(artists_table_name, artist_id) or {}

        # If artist metadata is missing and we have a Spotify client, enrich it
        if (not artist_meta or not artist_meta.get("name")) and spotify_client:
            logger.info(
                f"Artist metadata missing for {artist_id}, enriching on-the-fly",
                extra={"artist_id": artist_id},
            )
            enriched_artist = enrich_artist(
                artist_id=artist_id,
                spotify_client=spotify_client,
                dynamo_client=dynamo_client,
                artists_table=artists_table_name,
            )
            if enriched_artist:
                artist_meta = enriched_artist.model_dump()
                enriched_count += 1
                logger.info(
                    f"Successfully enriched artist: {artist_meta.get('name')}",
                    extra={"artist_id": artist_id, "artist_name": artist_meta.get("name")},
                )
            else:
                logger.error(f"Failed to enrich artist {artist_id}", extra={"artist_id": artist_id})

        artist_metadata_cache[artist_id] = artist_meta

    if enriched_count > 0:
        logger.info(
            f"Auto-enriched {enriched_count} missing metadata entries",
            extra={"enriched_count": enriched_count},
        )

    def build_window_payload(window_key: str) -> dict:
        data = window_counts[window_key]
        track_counts = data["track_counts"]
        total_plays = data["total_plays"]
        artist_counts: dict[str, int] = defaultdict(int)
        genre_counts: dict[str, int] = defaultdict(int)
        top_tracks: list[dict] = []

        sorted_tracks = sorted(track_counts.items(), key=lambda x: x[1], reverse=True)
        for track_id, count in sorted_tracks:
            metadata = track_metadata_cache.get(track_id) or {}

            # Skip tracks with no metadata (not enriched yet)
            if not metadata or not metadata.get("name"):
                logger.warning(
                    f"Skipping track {track_id} - no metadata in cache",
                    extra={"track_id": track_id, "play_count": count},
                )
                continue

            artist_ids_for_track = metadata.get("artist_ids", [])
            artist_names = []
            for artist_id in artist_ids_for_track:
                artist_meta = artist_metadata_cache.get(artist_id) or {}
                artist_counts[artist_id] += count
                artist_name = artist_meta.get("name", "")
                if artist_name:  # Only add if we have a real name
                    artist_names.append(artist_name)
                for genre in artist_meta.get("genres", []):
                    genre_counts[genre] += count

            # Skip if we don't have artist names (corrupted data)
            if not artist_names:
                logger.warning(
                    f"Skipping track {metadata.get('name')} - no artist names",
                    extra={"track_id": track_id, "track_name": metadata.get("name")},
                )
                continue

            if len(top_tracks) < 50:
                top_tracks.append(
                    {
                        "track_id": track_id,
                        "track_name": metadata.get("name"),
                        "artist_name": ", ".join(artist_names),
                        "album_name": metadata.get("album_name", ""),
                        "play_count": count,
                    }
                )

        top_artists: list[dict] = []
        sorted_artists = sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:50]
        for artist_id, play_count in sorted_artists:
            artist_meta = artist_metadata_cache.get(artist_id) or {}
            artist_name = artist_meta.get("name", "")

            # Skip artists with no name (corrupted/missing data)
            if not artist_name:
                logger.warning(
                    f"Skipping artist {artist_id} - no name in cache",
                    extra={"artist_id": artist_id, "play_count": play_count},
                )
                continue

            top_artists.append(
                {
                    "artist_id": artist_id,
                    "artist_name": artist_name,
                    "genres": artist_meta.get("genres", []),
                    "play_count": play_count,
                }
            )

        top_genres_sorted = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:20]
        top_genres = [{"genre": genre, "play_count": count} for genre, count in top_genres_sorted]

        return {
            "label": window_key,
            "start": datetime.combine(data["start"], datetime.min.time(), tzinfo=PACIFIC_TZ),
            "end": datetime.combine(data["end"], datetime.min.time(), tzinfo=PACIFIC_TZ),
            "top_tracks": top_tracks,
            "top_artists": top_artists,
            "top_genres": top_genres,
            "total_play_count": total_plays,
            "unique_track_count": len(track_counts),
            "unique_artist_count": len(artist_counts),
        }

    windows_payload = {key: build_window_payload(key) for key in window_counts.keys()}

    default_window_order = ["last_30_days", "last_7_days", "year_to_date", "all_time"]
    default_window = next((w for w in default_window_order if w in windows_payload), "all_time")
    selected_window = windows_payload.get(default_window, {})

    # Daily plays from summaries (daily_trend_days inclusive)
    trend_start_date = summary_end_date - timedelta(days=daily_trend_days)
    daily_plays: list[dict] = []
    current_date = trend_start_date
    while current_date <= summary_end_date:
        play_count = summary_lookup.get(current_date, {}).get("total_plays", 0)
        daily_plays.append({"date": current_date.isoformat(), "play_count": play_count})
        current_date += timedelta(days=1)

    # Hourly distribution from hot store (bounded by TTL)
    hourly_start_pacific = now - timedelta(days=min(hourly_lookback_days, lookback_days))
    hourly_start_utc = hourly_start_pacific.astimezone(timezone.utc)
    now_utc = now.astimezone(timezone.utc)
    hourly_counts: dict[int, int] = defaultdict(int)
    plays_for_hours = dynamo_client.query_plays_by_date_range(
        hot_table_name, hourly_start_utc.isoformat(), now_utc.isoformat()
    )
    for play in plays_for_hours:
        try:
            played_at = datetime.fromisoformat(play["played_at"]).astimezone(PACIFIC_TZ)
        except (KeyError, ValueError):
            continue
        hourly_counts[played_at.hour] += 1

    hourly_distribution = [
        {"hour": hour, "play_count": hourly_counts.get(hour, 0)} for hour in range(24)
    ]

    all_time_window = windows_payload.get("all_time", {})
    metadata = {
        "generated_at": now.isoformat(),
        "total_play_count": all_time_window.get("total_play_count", 0),
        "unique_track_count": all_time_window.get("unique_track_count", 0),
        "unique_artist_count": all_time_window.get("unique_artist_count", 0),
        "genre_count": len(all_time_window.get("top_genres", [])),
        "date_range_start": datetime.combine(
            summary_start_date, datetime.min.time(), tzinfo=PACIFIC_TZ
        ).isoformat(),
        "date_range_end": datetime.combine(
            summary_end_date, datetime.min.time(), tzinfo=PACIFIC_TZ
        ).isoformat(),
        "default_window": default_window,
    }
    previous_total_play_count = sum(
        int(summary.get("total_plays", 0))
        for summary in validated_summaries
        if datetime.fromisoformat(summary["date"]).date() < summary_end_date
    )
    aggregate_daily_new_plays = metadata["total_play_count"] - previous_total_play_count
    aggregate_summary_days_processed = len(validated_summaries)

    # Emit CloudWatch Embedded Metric Format (EMF) for metric extraction
    metric_event = {
        "_aws": {
            "Timestamp": int(now.timestamp() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": "spotify-lifecycle",
                    "Dimensions": [[]],
                    "Metrics": [
                        {"Name": "aggregate_total_play_count", "Unit": "None"},
                        {"Name": "aggregate_unique_tracks", "Unit": "None"},
                        {"Name": "aggregate_daily_new_plays", "Unit": "Count"},
                        {"Name": "aggregate_summary_days_processed", "Unit": "Count"},
                        {"Name": "daily_trend_days", "Unit": "Count"},
                        {"Name": "daily_plays_points", "Unit": "Count"},
                    ],
                }
            ],
        },
        "event": "aggregate_completed",
        "aggregate_total_play_count": metadata["total_play_count"],
        "aggregate_unique_tracks": metadata["unique_track_count"],
        "aggregate_daily_new_plays": aggregate_daily_new_plays,
        "aggregate_summary_days_processed": aggregate_summary_days_processed,
        "daily_trend_days": daily_trend_days,
        "daily_plays_points": len(daily_plays),
        "unique_artist_count": metadata["unique_artist_count"],
        "summary_days": aggregate_summary_days_processed,
        "summary_start_date": summary_start_date.isoformat(),
        "summary_end_date": summary_end_date.isoformat(),
        "date_range_start": metadata["date_range_start"],
        "date_range_end": metadata["date_range_end"],
    }
    print(json.dumps(metric_event))  # EMF format for auto metric creation
    logger.info(
        "aggregate_completed",
        extra={
            "total_play_count": metadata["total_play_count"],
            "unique_track_count": metadata["unique_track_count"],
        },
    )

    dashboard_data = DashboardData(
        generated_at=now,
        time_range={
            "start": datetime.combine(summary_start_date, datetime.min.time(), tzinfo=PACIFIC_TZ),
            "end": datetime.combine(summary_end_date, datetime.min.time(), tzinfo=PACIFIC_TZ),
        },
        metadata=metadata,
        top_tracks=selected_window.get("top_tracks", []),
        top_artists=selected_window.get("top_artists", []),
        daily_plays=daily_plays,
        hourly_distribution=hourly_distribution,
        top_genres=selected_window.get("top_genres", []),
        windows=windows_payload,
    )

    dashboard_store.write_dashboard_data(
        dashboard_bucket_name, dashboard_data.model_dump(mode="json")
    )

    return dashboard_data
