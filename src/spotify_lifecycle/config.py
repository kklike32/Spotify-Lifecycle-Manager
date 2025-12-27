"""Configuration management for Spotify Lifecycle Manager.

This module defines all environment variables required by the application.
No business logic should exist here—only configuration loading.

Environment Variable Categories:
    - SPOTIFY_*: Spotify API credentials and OAuth tokens
    - *_TABLE_NAME: DynamoDB table names for hot storage
    - *_BUCKET_NAME: S3 bucket names for cold storage and dashboard
    - AWS_REGION: AWS region for all resources (default: us-east-1)
    - LOOKBACK_DAYS: Time window for filtering recent plays
    - ENVIRONMENT: Deployment environment (development/production)

For setup instructions, see:
    - copilot/docs/spotify/OAUTH_SETUP.md (Spotify credentials)
    - copilot/docs/cloud/ACCOUNT_SETUP.md (AWS account)
    - copilot/docs/cloud/SECURITY_MODEL.md (Secret management)
"""

import os
from dataclasses import dataclass


@dataclass
class SpotifyConfig:
    """Spotify API configuration.

    Attributes:
        client_id: Spotify application client ID (from developer dashboard)
        client_secret: Spotify application client secret (from developer dashboard)
        refresh_token: OAuth refresh token (generated during setup)
        user_id: Spotify user ID (default: 'me' for current authenticated user)
    """

    client_id: str
    client_secret: str
    refresh_token: str
    user_id: str = "me"


@dataclass
class StorageConfig:
    """AWS storage configuration.

    Attributes:
        hot_table_name: DynamoDB table for recent play events (with TTL)
        tracks_table_name: DynamoDB table for track metadata cache
        artists_table_name: DynamoDB table for artist metadata cache
        state_table_name: DynamoDB table for pipeline state (cursors, run IDs)
        raw_bucket_name: S3 bucket for cold storage (partitioned JSONL)
        dashboard_bucket_name: S3 bucket for static dashboard files
        region: AWS region for all resources (cost-conscious choice)
    """

    hot_table_name: str
    tracks_table_name: str
    artists_table_name: str
    state_table_name: str
    raw_bucket_name: str
    dashboard_bucket_name: str
    region: str = "us-east-1"


@dataclass
class AppConfig:
    """Application configuration.

    Main configuration object that aggregates all sub-configs.

    Attributes:
        source_playlist_id: Spotify playlist ID to source tracks from (for weekly playlists)
        lookback_days: Number of days to look back for recent plays (default: 7)
        environment: Deployment environment (development/production)
        spotify: Spotify API configuration (auto-loaded from env vars)
        storage: AWS storage configuration (auto-loaded from env vars)
    """

    source_playlist_id: str
    lookback_days: int = 7
    environment: str = "development"
    spotify: SpotifyConfig = None
    storage: StorageConfig = None

    def __post_init__(self):
        """Initialize nested configs from environment variables.

        This method is called automatically after dataclass initialization.
        It loads all nested configuration from environment variables if not
        explicitly provided during construction.
        """
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
                region=os.getenv("AWS_REGION", "us-east-1"),
            )


def load_config() -> AppConfig:
    """Load application configuration from environment variables.

    This is the primary entry point for configuration loading. It:
    1. Loads .env file if present (development)
    2. Reads all environment variables
    3. Returns a fully-initialized AppConfig

    Returns:
        AppConfig: Fully-loaded configuration object

    Raises:
        ValueError: If required environment variables are missing (future)

    Example:
        >>> cfg = load_config()
        >>> print(cfg.spotify.client_id)
        >>> print(cfg.storage.region)
    """
    from dotenv import load_dotenv

    load_dotenv()

    return AppConfig(
        source_playlist_id=os.getenv("SOURCE_PLAYLIST_ID", ""),
        lookback_days=int(os.getenv("LOOKBACK_DAYS", "7")),
        environment=os.getenv("ENVIRONMENT", "development"),
    )
