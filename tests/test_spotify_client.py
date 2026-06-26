"""Tests for Spotify API client authentication behavior."""

from unittest.mock import Mock

import pytest
import requests

from spotify_lifecycle.spotify.client import SpotifyClient, SpotifyRefreshTokenExpiredError


class FakeTokenResponse:
    """Minimal requests.Response stand-in for token endpoint tests."""

    def __init__(self, status_code: int, payload: dict[str, object]):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, object]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} error")


def test_authenticate_with_refresh_token_creates_spotify_client(monkeypatch):
    """A valid refresh token is exchanged for an access token."""
    response = FakeTokenResponse(200, {"access_token": "access-token"})
    post = Mock(return_value=response)
    spotify = Mock()

    monkeypatch.setattr("spotify_lifecycle.spotify.client.requests.post", post)
    monkeypatch.setattr("spotify_lifecycle.spotify.client.spotipy.Spotify", spotify)

    client = SpotifyClient(client_id="client-id", client_secret="client-secret")
    result = client.authenticate(refresh_token="refresh-token")

    assert result == spotify.return_value
    post.assert_called_once()
    assert post.call_args.kwargs["data"] == {
        "grant_type": "refresh_token",
        "refresh_token": "refresh-token",
    }
    spotify.assert_called_once_with(auth="access-token")


def test_authenticate_invalid_grant_raises_reauthorization_error(monkeypatch):
    """An expired refresh token produces a reauthorization-specific error."""
    response = FakeTokenResponse(400, {"error": "invalid_grant"})
    monkeypatch.setattr(
        "spotify_lifecycle.spotify.client.requests.post", Mock(return_value=response)
    )

    client = SpotifyClient(client_id="client-id", client_secret="client-secret")

    with pytest.raises(SpotifyRefreshTokenExpiredError, match="reauthorize"):
        client.authenticate(refresh_token="expired-refresh-token")


def test_authenticate_other_http_errors_raise_requests_error(monkeypatch):
    """Non-expired-token HTTP failures keep the existing requests behavior."""
    response = FakeTokenResponse(500, {"error": "server_error"})
    monkeypatch.setattr(
        "spotify_lifecycle.spotify.client.requests.post", Mock(return_value=response)
    )

    client = SpotifyClient(client_id="client-id", client_secret="client-secret")

    with pytest.raises(requests.HTTPError):
        client.authenticate(refresh_token="refresh-token")


def test_interactive_auth_uses_read_only_recently_played_scope(monkeypatch):
    """New interactive auth sessions do not request playlist write scopes."""
    oauth = Mock()
    spotify = Mock()

    monkeypatch.setattr("spotify_lifecycle.spotify.client.SpotifyOAuth", oauth)
    monkeypatch.setattr("spotify_lifecycle.spotify.client.spotipy.Spotify", spotify)

    client = SpotifyClient(client_id="client-id", client_secret="client-secret")
    client.authenticate()

    assert oauth.call_args.kwargs["scope"] == "user-read-recently-played"
    spotify.assert_called_once_with(auth_manager=oauth.return_value)
