"""Librarian stage: Enriches play events with metadata."""

from spotify_lifecycle.models import ArtistMetadata, AudioFeatures, TrackMetadata
from spotify_lifecycle.spotify.client import SpotifyClient
from spotify_lifecycle.storage.dynamo import DynamoDBClient


def enrich_track_metadata(
    spotify_client: SpotifyClient,
    dynamo_client: DynamoDBClient,
    tracks_table_name: str,
    track_id: str,
) -> TrackMetadata:
    """Enrich a track with metadata from Spotify API and cache it.

    Args:
        spotify_client: Authenticated Spotify client
        dynamo_client: DynamoDB client
        tracks_table_name: DynamoDB table for track metadata
        track_id: Spotify track ID

    Returns:
        TrackMetadata object
    """
    # Check cache first
    cached = dynamo_client.get_track_metadata(tracks_table_name, track_id)
    if cached:
        return TrackMetadata(
            track_id=cached["track_id"],
            name=cached["name"],
            artist_ids=cached["artist_ids"],
            album_id=cached["album_id"],
            album_name=cached["album_name"],
            duration_ms=cached["duration_ms"],
            explicit=cached["explicit"],
            popularity=cached["popularity"],
            uri=cached["uri"],
        )

    # Fetch from Spotify
    track_data = spotify_client.get_track(track_id)

    metadata = TrackMetadata(
        track_id=track_id,
        name=track_data["name"],
        artist_ids=[artist["id"] for artist in track_data["artists"]],
        album_id=track_data["album"]["id"],
        album_name=track_data["album"]["name"],
        duration_ms=track_data["duration_ms"],
        explicit=track_data["explicit"],
        popularity=track_data["popularity"],
        uri=track_data["uri"],
    )

    # Cache it
    dynamo_client.write_track_metadata(tracks_table_name, metadata)

    return metadata


def enrich_artist_metadata(
    spotify_client: SpotifyClient,
    dynamo_client: DynamoDBClient,
    artists_table_name: str,
    artist_id: str,
) -> ArtistMetadata:
    """Enrich an artist with metadata from Spotify API and cache it.

    Args:
        spotify_client: Authenticated Spotify client
        dynamo_client: DynamoDB client
        artists_table_name: DynamoDB table for artist metadata
        artist_id: Spotify artist ID

    Returns:
        ArtistMetadata object
    """
    # Check cache first
    cached = dynamo_client.get_artist_metadata(artists_table_name, artist_id)
    if cached:
        return ArtistMetadata(
            artist_id=cached["artist_id"],
            name=cached["name"],
            genres=cached["genres"],
            popularity=cached["popularity"],
            uri=cached["uri"],
            images=cached.get("images", []),
        )

    # Fetch from Spotify
    artist_data = spotify_client.get_artist(artist_id)

    metadata = ArtistMetadata(
        artist_id=artist_id,
        name=artist_data["name"],
        genres=artist_data["genres"],
        popularity=artist_data["popularity"],
        uri=artist_data["uri"],
        images=artist_data.get("images", []),
    )

    # Cache it
    dynamo_client.write_artist_metadata(artists_table_name, metadata)

    return metadata


def enrich_audio_features(spotify_client: SpotifyClient, track_id: str) -> AudioFeatures:
    """Fetch audio features for a track.

    Args:
        spotify_client: Authenticated Spotify client
        track_id: Spotify track ID

    Returns:
        AudioFeatures object
    """
    features_data = spotify_client.get_audio_features(track_id)

    return AudioFeatures(
        track_id=track_id,
        acousticness=features_data["acousticness"],
        danceability=features_data["danceability"],
        energy=features_data["energy"],
        instrumentalness=features_data["instrumentalness"],
        key=features_data["key"],
        liveness=features_data["liveness"],
        loudness=features_data["loudness"],
        mode=features_data["mode"],
        speechiness=features_data["speechiness"],
        tempo=features_data["tempo"],
        time_signature=features_data["time_signature"],
        valence=features_data["valence"],
    )
