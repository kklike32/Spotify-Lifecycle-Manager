"""Tests for Spotify OAuth helper behavior."""

from unittest.mock import Mock

from spotify_lifecycle.spotify.oauth import get_refresh_token


def test_get_refresh_token_uses_read_only_recently_played_scope(monkeypatch):
    """Generated refresh tokens should not request playlist write scopes."""
    auth_manager = Mock()
    auth_manager.get_cached_token.return_value = {"refresh_token": "refresh-token"}
    oauth = Mock(return_value=auth_manager)

    monkeypatch.setattr("spotify_lifecycle.spotify.oauth.SpotifyOAuth", oauth)

    assert get_refresh_token("client-id", "client-secret") == "refresh-token"
    assert oauth.call_args.kwargs["scope"] == "user-read-recently-played"
