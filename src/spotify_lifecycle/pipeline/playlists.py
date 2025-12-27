"""DJ stage: Creates weekly curated playlists."""

from datetime import datetime
from typing import Optional

from spotify_lifecycle.spotify.client import SpotifyClient
from spotify_lifecycle.storage.dynamo import DynamoDBClient


def create_weekly_playlist(
    spotify_client: SpotifyClient,
    dynamo_client: DynamoDBClient,
    source_playlist_id: str,
    lookback_days: int = 7,
    state_table_name: Optional[str] = None,
) -> dict:
    """Create a new weekly playlist with tracks not played recently.

    Args:
        spotify_client: Authenticated Spotify client
        dynamo_client: DynamoDB client
        source_playlist_id: Playlist ID to source tracks from
        lookback_days: Only include tracks not played in last N days
        state_table_name: Optional DynamoDB table for idempotency

    Returns:
        Summary of playlist creation
    """
    # TODO: Implement weekly playlist logic
    # 1. Get source playlist tracks
    # 2. Query tracks not played in last N days
    # 3. Create new playlist (idempotent)
    # 4. Add tracks to playlist

    return {
        "playlist_id": None,
        "tracks_added": 0,
        "created_at": datetime.now().isoformat(),
    }
