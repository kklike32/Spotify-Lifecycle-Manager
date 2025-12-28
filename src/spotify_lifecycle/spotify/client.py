"""Spotify API client for fetching play history and metadata."""

from typing import Optional

import requests
import spotipy
from requests.auth import HTTPBasicAuth
from spotipy.oauth2 import SpotifyOAuth


class SpotifyClient:
    """Wrapper around Spotipy for Spotify API interactions."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str = "http://localhost:8888/callback",
    ):
        """Initialize Spotify client.

        Args:
            client_id: Spotify app client ID
            client_secret: Spotify app client secret
            redirect_uri: OAuth redirect URI
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.sp: Optional[spotipy.Spotify] = None

    def authenticate(self, refresh_token: Optional[str] = None) -> spotipy.Spotify:
        """Authenticate with Spotify API.

        Args:
            refresh_token: Existing refresh token to reuse

        Returns:
            Authenticated Spotipy client
        """
        if refresh_token:
            # Non-interactive refresh: exchange refresh token for access token
            token_url = "https://accounts.spotify.com/api/token"
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            }

            response = requests.post(
                token_url,
                data=payload,
                auth=HTTPBasicAuth(self.client_id, self.client_secret),
                timeout=10,
            )
            response.raise_for_status()
            access_token = response.json()["access_token"]
            self.sp = spotipy.Spotify(auth=access_token)
        else:
            # Fallback to interactive code flow if no refresh token was provided
            auth_manager = SpotifyOAuth(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                scope="user-read-recently-played playlist-modify-private" " playlist-modify-public",
                open_browser=False,
            )
            self.sp = spotipy.Spotify(auth_manager=auth_manager)

        return self.sp

    def get_recently_played(self, limit: int = 50, before: Optional[int] = None) -> dict:
        """Get recently played tracks.

        Args:
            limit: Number of tracks to return (max 50)
            before: Timestamp in milliseconds

        Returns:
            API response with play history
        """
        if not self.sp:
            raise RuntimeError("Not authenticated. Call authenticate() first.")

        kwargs = {"limit": limit}
        if before:
            kwargs["before"] = before

        return self.sp.current_user_recently_played(**kwargs)

    def get_track(self, track_id: str) -> dict:
        """Get track metadata.

        Args:
            track_id: Spotify track ID

        Returns:
            Track metadata
        """
        if not self.sp:
            raise RuntimeError("Not authenticated.")

        return self.sp.track(track_id)

    def get_artist(self, artist_id: str) -> dict:
        """Get artist metadata.

        Args:
            artist_id: Spotify artist ID

        Returns:
            Artist metadata
        """
        if not self.sp:
            raise RuntimeError("Not authenticated.")

        return self.sp.artist(artist_id)

    def get_audio_features(self, track_id: str) -> dict:
        """Get audio features for a track.

        Args:
            track_id: Spotify track ID

        Returns:
            Audio features
        """
        if not self.sp:
            raise RuntimeError("Not authenticated.")

        return self.sp.audio_features(track_id)[0]

    def create_playlist(
        self, user_id: str, name: str, description: str = "", public: bool = False
    ) -> dict:
        """Create a new playlist.

        Args:
            user_id: Spotify user ID
            name: Playlist name
            description: Playlist description
            public: Whether playlist is public

        Returns:
            Created playlist metadata
        """
        if not self.sp:
            raise RuntimeError("Not authenticated.")

        return self.sp.user_playlist_create(
            user=user_id,
            name=name,
            public=public,
            description=description,
        )

    def add_tracks_to_playlist(self, playlist_id: str, track_ids: list[str]) -> None:
        """Add tracks to a playlist.

        Args:
            playlist_id: Spotify playlist ID
            track_ids: List of track IDs to add
        """
        if not self.sp:
            raise RuntimeError("Not authenticated.")

        # Spotify API has a limit of 100 tracks per request
        for i in range(0, len(track_ids), 100):
            batch = track_ids[i : i + 100]
            self.sp.playlist_add_items(playlist_id, batch)

    def get_playlist_tracks(self, playlist_id: str) -> list[str]:
        """Get all track IDs from a playlist (with pagination).

        Fetches all tracks from a Spotify playlist, handling pagination
        automatically. Useful for fetching source playlists for filtering.

        Args:
            playlist_id: Spotify playlist ID (or URI)

        Returns:
            List of Spotify track URIs

        Raises:
            RuntimeError: If not authenticated

        Notes:
            - Handles pagination automatically (Spotify returns max 100 per page)
            - Returns track URIs in format: spotify:track:{id}
            - Skips local tracks (not available via Spotify API)
            - Empty playlists return empty list
        """
        if not self.sp:
            raise RuntimeError("Not authenticated.")

        track_ids = []
        offset = 0
        limit = 100  # Spotify API max

        while True:
            # Fetch one page of tracks
            response = self.sp.playlist_tracks(
                playlist_id,
                fields="items(track(uri,is_local)),next",
                limit=limit,
                offset=offset,
            )

            # Extract track URIs (skip local tracks)
            for item in response.get("items", []):
                track = item.get("track")
                if track and not track.get("is_local", False):
                    track_ids.append(track["uri"])

            # Check if more pages exist
            if response.get("next"):
                offset += limit
            else:
                break

        return track_ids
