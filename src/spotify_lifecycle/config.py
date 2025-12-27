"""Configuration management for Spotify Lifecycle Manager."""

import os
from dataclasses import dataclass


@dataclass
class SpotifyConfig:
    """Spotify API configuration."""

    client_id: str
    client_secret: str
    refresh_token: str
    user_id: str = "me"


@dataclass
class StorageConfig:
    """AWS storage configuration."""

    hot_table_name: str
    tracks_table_name: str
    artists_table_name: str
    state_table_name: str
    raw_bucket_name: str
    dashboard_bucket_name: str


@dataclass
class AppConfig:
    """Application configuration."""

    source_playlist_id: str
    lookback_days: int = 7
    environment: str = "development"
    spotify: SpotifyConfig = None
    storage: StorageConfig = None

    def __post_init__(self):
        """Initialize nested configs from environment variables."""
        if self.spotify is None:
            self.spotify = SpotifyConfig(
                client_id=os.getenv("SPOTIFY_CLIENT_ID", ""),
                client_secret=os.getenv("SPOTIFY_CLIENT_SECRET", ""),
                refresh_token=os.getenv("SPOTIFY_REFRESH_TOKEN", ""),
                user_id=os.getenv("USER_ID", "me"),
            )

        if self.storage is None:
            self.storage = StorageConfig(
                hot_table_name=os.getenv("HOT_TABLE_NAME", "spotify-play-events"),
                tracks_table_name=os.getenv("TRACKS_TABLE_NAME", "spotify-tracks"),
                artists_table_name=os.getenv("ARTISTS_TABLE_NAME", "spotify-artists"),
                state_table_name=os.getenv("STATE_TABLE_NAME", "spotify-state"),
                raw_bucket_name=os.getenv("RAW_BUCKET_NAME", "spotify-raw-events"),
                dashboard_bucket_name=os.getenv("DASHBOARD_BUCKET_NAME", "spotify-dashboard"),
            )


def load_config() -> AppConfig:
    """Load application configuration from environment variables."""
    from dotenv import load_dotenv

    load_dotenv()

    return AppConfig(
        source_playlist_id=os.getenv("SOURCE_PLAYLIST_ID", ""),
        lookback_days=int(os.getenv("LOOKBACK_DAYS", "7")),
        environment=os.getenv("ENVIRONMENT", "development"),
    )
