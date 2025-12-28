"""Data models for Spotify Lifecycle Manager.

This module defines all data models using Pydantic for validation and serialization.
All models include version fields for schema evolution and support JSON serialization.

See: copilot/docs/architecture/DATA_MODELS.md for complete schema specifications.
"""

from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PlayEvent(BaseModel):
    """A single play event from Spotify.

    Represents one track play from Spotify's recently played API.
    Used for tracking listening history and computing analytics.

    Attributes:
        version: Schema version (semantic versioning)
        play_id: Unique deterministic identifier (generated via make_play_id)
        track_id: Spotify track URI
        played_at: When track was played (UTC)
        user_id: Spotify user ID (MVP: always 'me')
        context: Optional playback context (playlist, album, etc.)
        ingested_at: When we captured this event (UTC)
    """

    version: str = Field(default="1.0.0", description="Schema version")
    play_id: str = Field(..., description="Deterministic event ID (SHA256)")
    track_id: str = Field(..., description="Spotify track URI")
    played_at: datetime = Field(..., description="When track was played (UTC)")
    user_id: str = Field(default="me", description="Spotify user ID")
    context: Optional[str] = Field(default=None, description="Playback context (playlist/album)")
    ingested_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="System capture timestamp"
    )

    @field_validator("track_id")
    @classmethod
    def validate_track_id(cls, v: str) -> str:
        """Validate Spotify track URI format."""
        if not v.startswith("spotify:track:"):
            raise ValueError(f"Invalid track URI format: {v}")
        return v

    @field_validator("played_at")
    @classmethod
    def validate_played_at(cls, v: datetime) -> datetime:
        """Ensure played_at is not in the future."""
        now = datetime.now(timezone.utc)
        if v > now:
            raise ValueError("played_at cannot be in the future")
        return v

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v: str) -> str:
        """Ensure user_id is non-empty."""
        if not v or not v.strip():
            raise ValueError("user_id cannot be empty")
        return v

    model_config = ConfigDict(
        validate_assignment=True,
    )


class TrackMetadata(BaseModel):
    """Metadata for a Spotify track.

    Cached metadata fetched once per track_id and stored forever.
    Used for enriching play events with track details.

    Attributes:
        version: Schema version (semantic versioning)
        track_id: Spotify track URI (primary key)
        name: Track title
        artist_ids: Artist URIs (multiple for collaborations)
        artist_names: Artist display names (same order as artist_ids)
        album_id: Spotify album URI
        album_name: Album title
        duration_ms: Track length in milliseconds
        explicit: Explicit content flag
        popularity: Spotify popularity score (0-100)
        release_date: Album release date (YYYY-MM-DD)
        uri: Canonical Spotify URI
        cached_at: When we fetched this metadata
    """

    version: str = Field(default="1.0.0", description="Schema version")
    track_id: str = Field(..., description="Spotify track URI")
    name: str = Field(..., min_length=1, description="Track title")
    artist_ids: list[str] = Field(..., min_length=1, description="Artist URIs")
    artist_names: list[str] = Field(..., min_length=1, description="Artist names")
    album_id: str = Field(..., description="Spotify album URI")
    album_name: str = Field(..., min_length=1, description="Album title")
    duration_ms: int = Field(..., gt=0, description="Track length (ms)")
    explicit: bool = Field(..., description="Explicit content flag")
    popularity: int = Field(..., ge=0, le=100, description="Popularity score")
    release_date: str = Field(..., description="Release date (YYYY-MM-DD)")
    uri: str = Field(..., description="Canonical Spotify URI")
    cached_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="Cache timestamp"
    )

    @field_validator("track_id", "uri")
    @classmethod
    def validate_track_uri(cls, v: str) -> str:
        """Validate Spotify track URI format."""
        if not v.startswith("spotify:track:"):
            raise ValueError(f"Invalid track URI format: {v}")
        return v

    @field_validator("album_id")
    @classmethod
    def validate_album_uri(cls, v: str) -> str:
        """Validate Spotify album URI format."""
        if not v.startswith("spotify:album:"):
            raise ValueError(f"Invalid album URI format: {v}")
        return v

    @field_validator("artist_ids")
    @classmethod
    def validate_artist_uris(cls, v: list[str]) -> list[str]:
        """Validate all artist URIs."""
        for uri in v:
            if not uri.startswith("spotify:artist:"):
                raise ValueError(f"Invalid artist URI format: {uri}")
        return v

    @field_validator("artist_names")
    @classmethod
    def validate_artist_names_length(cls, v: list[str], info) -> list[str]:
        """Ensure artist_names matches artist_ids length."""
        # Note: info.data contains other validated fields
        artist_ids = info.data.get("artist_ids", [])
        if len(v) != len(artist_ids):
            raise ValueError("artist_names must match artist_ids length")
        return v

    model_config = ConfigDict(
        validate_assignment=True,
    )


class ArtistMetadata(BaseModel):
    """Metadata for a Spotify artist.

    Cached metadata fetched once per artist_id and stored forever.
    Used for enriching play events with artist details.

    Attributes:
        version: Schema version (semantic versioning)
        artist_id: Spotify artist URI (primary key)
        name: Artist display name
        genres: Genre tags (can be empty)
        popularity: Spotify popularity score (0-100)
        followers: Total follower count
        uri: Canonical Spotify URI
        images: Artist images (multiple sizes)
        cached_at: When we fetched this metadata
    """

    version: str = Field(default="1.0.0", description="Schema version")
    artist_id: str = Field(..., description="Spotify artist URI")
    name: str = Field(..., min_length=1, description="Artist display name")
    genres: list[str] = Field(default_factory=list, description="Genre tags")
    popularity: int = Field(..., ge=0, le=100, description="Popularity score")
    followers: int = Field(..., ge=0, description="Follower count")
    uri: str = Field(..., description="Canonical Spotify URI")
    images: list[dict[str, Any]] = Field(default_factory=list, description="Artist images")
    cached_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="Cache timestamp"
    )

    @field_validator("artist_id", "uri")
    @classmethod
    def validate_artist_uri(cls, v: str) -> str:
        """Validate Spotify artist URI format."""
        if not v.startswith("spotify:artist:"):
            raise ValueError(f"Invalid artist URI format: {v}")
        return v

    model_config = ConfigDict(
        validate_assignment=True,
    )


class IngestionState(BaseModel):
    """State for ingestion pipeline (cursor tracking).

    Tracks the last successful ingest to avoid gaps and enable overlap fetching.

    Attributes:
        version: Schema version (semantic versioning)
        state_key: Always "ingest_cursor" (partition key)
        last_played_at: Most recent played_at from last run
        last_run_at: When ingestion last ran
        last_event_count: Events fetched in last run
        status: Run status (success/failed)
    """

    version: str = Field(default="1.0.0", description="Schema version")
    state_key: str = Field(default="ingest_cursor", description="State identifier")
    last_played_at: datetime = Field(..., description="Last processed timestamp")
    last_run_at: datetime = Field(..., description="Last run timestamp")
    last_event_count: int = Field(..., ge=0, description="Events fetched")
    status: str = Field(..., description="Run status (success/failed)")

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate status values."""
        valid_statuses = {"success", "failed"}
        if v not in valid_statuses:
            raise ValueError(f"Status must be one of {valid_statuses}")
        return v

    model_config = ConfigDict(
        validate_assignment=True,
    )


class PlaylistState(BaseModel):
    """State for weekly playlist creation.

    Tracks created playlists to prevent duplicates on retry.

    Attributes:
        version: Schema version (semantic versioning)
        state_key: Format: weekly_playlist_YYYY_WXX
        week_id: ISO week identifier (YYYY-WXX)
        playlist_id: Spotify playlist URI
        created_at: When playlist was created
        track_count: Tracks added to playlist
        source_playlist_id: Source playlist (for debugging)
    """

    version: str = Field(default="1.0.0", description="Schema version")
    state_key: str = Field(..., description="State identifier")
    week_id: str = Field(..., description="ISO week (YYYY-WXX)")
    playlist_id: str = Field(..., description="Spotify playlist URI")
    created_at: datetime = Field(..., description="Creation timestamp")
    track_count: int = Field(..., ge=0, description="Tracks added")
    source_playlist_id: str = Field(..., description="Source playlist")

    @field_validator("week_id")
    @classmethod
    def validate_week_id(cls, v: str) -> str:
        """Validate ISO week format (YYYY-WXX)."""
        import re

        if not re.match(r"^\d{4}-W\d{2}$", v):
            raise ValueError(f"Invalid week ID format: {v} (expected YYYY-WXX)")
        return v

    @field_validator("playlist_id", "source_playlist_id")
    @classmethod
    def validate_playlist_uri(cls, v: str) -> str:
        """Validate Spotify playlist URI format."""
        if not v.startswith("spotify:playlist:"):
            raise ValueError(f"Invalid playlist URI format: {v}")
        return v

    model_config = ConfigDict(
        validate_assignment=True,
    )


class DashboardData(BaseModel):
    """Precomputed dashboard analytics (single JSON file).

    Generated nightly with all dashboard data. No queries needed in browser.

    Attributes:
        version: Schema version (semantic versioning)
        generated_at: When this JSON was created
        time_range: Data coverage period
        metadata: High-level statistics (matches dashboard expectations)
        top_tracks: Top 50 tracks by play count
        top_artists: Top 50 artists by play count
        daily_plays: Last 90 days of daily stats
        hourly_distribution: Plays by hour (0-23)
        top_genres: Top 20 genres by play count
    """

    version: str = Field(default="1.0.0", description="Schema version")
    generated_at: datetime = Field(..., description="Generation timestamp")
    time_range: dict[str, datetime] = Field(..., description="Data coverage period")
    metadata: dict[str, Any] = Field(..., description="High-level stats for dashboard")
    top_tracks: list[dict[str, Any]] = Field(..., description="Top tracks")
    top_artists: list[dict[str, Any]] = Field(..., description="Top artists")
    daily_plays: list[dict[str, Any]] = Field(..., description="Daily play counts")
    hourly_distribution: list[dict[str, int]] = Field(..., description="Hourly distribution")
    top_genres: list[dict[str, Any]] = Field(..., description="Top genres")

    model_config = ConfigDict(
        validate_assignment=True,
    )
