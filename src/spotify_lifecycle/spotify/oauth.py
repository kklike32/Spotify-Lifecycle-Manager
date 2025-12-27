"""OAuth utilities for Spotify authentication."""

from spotipy.oauth2 import SpotifyOAuth


def get_refresh_token(
    client_id: str, client_secret: str, redirect_uri: str = "http://localhost:8888/callback"
) -> str:
    """Get a new refresh token from Spotify OAuth flow.

    Args:
        client_id: Spotify app client ID
        client_secret: Spotify app client secret
        redirect_uri: OAuth redirect URI

    Returns:
        Refresh token for use with SpotifyClient
    """
    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope="user-read-recently-played playlist-modify-private" " playlist-modify-public",
    )

    token_info = auth_manager.get_cached_token()
    if token_info is None:
        token_info = auth_manager.get_access_token(code=None, as_dict=True, check_cache=False)

    return token_info["refresh_token"]
