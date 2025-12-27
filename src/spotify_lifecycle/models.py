"""Data models for Spotify Lifecycle Manager."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class PlayEvent:
    """A single play event from Spotify."""

    track_id: str
    played_at: datetime
    user_id: str
    context: Optional[str] = None


@dataclass
class TrackMetadata:
    """Metadata for a Spotify track."""

    track_id: str
    name: str
    artist_ids: list[str]
    album_id: str
    album_name: str
    duration_ms: int
    explicit: bool
    popularity: int
    uri: str


@dataclass
class ArtistMetadata:
    """Metadata for a Spotify artist."""

    artist_id: str
    name: str
    genres: list[str]
    popularity: int
    uri: str
    images: list[dict]


@dataclass
class AudioFeatures:
    """Audio features for a track."""

    track_id: str
    acousticness: float
    danceability: float
    energy: float
    instrumentalness: float
    key: int
    liveness: float
    loudness: float
    mode: int
    speechiness: float
    tempo: float
    time_signature: int
    valence: float
