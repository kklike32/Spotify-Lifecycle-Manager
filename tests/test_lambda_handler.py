"""Tests for Lambda handler client initialization."""

from unittest.mock import Mock

import pytest

from spotify_lifecycle import lambda_handler
from spotify_lifecycle.spotify.client import SpotifyRefreshTokenExpiredError


def test_get_spotify_client_does_not_cache_failed_reauthorization(monkeypatch, caplog):
    """Failed Spotify reauthorization does not poison the warm Lambda cache."""
    lambda_handler._spotify_client = None

    monkeypatch.setattr(
        lambda_handler,
        "get_secret",
        lambda key: {
            "SPOTIFY_CLIENT_ID": "client-id",
            "SPOTIFY_CLIENT_SECRET": "client-secret",
            "SPOTIFY_REFRESH_TOKEN": "expired-refresh-token",
        }[key],
    )

    instance = Mock()
    instance.authenticate.side_effect = SpotifyRefreshTokenExpiredError("reauthorize")
    spotify_client = Mock(return_value=instance)
    monkeypatch.setattr(lambda_handler, "SpotifyClient", spotify_client)

    with caplog.at_level("ERROR"), pytest.raises(SpotifyRefreshTokenExpiredError):
        lambda_handler.get_spotify_client()

    assert lambda_handler._spotify_client is None
    assert "spotify_reauthorization_required" in caplog.text
