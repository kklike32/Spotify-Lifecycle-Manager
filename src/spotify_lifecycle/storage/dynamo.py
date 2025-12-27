"""DynamoDB interactions for storing play events and metadata."""

import json
from typing import Optional

import boto3

from spotify_lifecycle.models import ArtistMetadata, PlayEvent, TrackMetadata


class DynamoDBClient:
    """DynamoDB client for storing and querying Spotify data."""

    def __init__(self, region_name: str = "us-east-1"):
        """Initialize DynamoDB client.

        Args:
            region_name: AWS region
        """
        self.dynamodb = boto3.resource("dynamodb", region_name=region_name)

    def write_play_event(self, table_name: str, event: PlayEvent, dedup_key: str) -> None:
        """Write a play event to DynamoDB with idempotency.

        Args:
            table_name: DynamoDB table name
            event: PlayEvent to write
            dedup_key: Unique key for deduplication
        """
        table = self.dynamodb.Table(table_name)
        table.put_item(
            Item={
                "dedup_key": dedup_key,
                "track_id": event.track_id,
                "played_at": event.played_at.isoformat(),
                "user_id": event.user_id,
                "context": event.context or "",
            }
        )

    def write_track_metadata(self, table_name: str, metadata: TrackMetadata) -> None:
        """Cache track metadata.

        Args:
            table_name: DynamoDB table name
            metadata: TrackMetadata to cache
        """
        table = self.dynamodb.Table(table_name)
        table.put_item(
            Item={
                "track_id": metadata.track_id,
                "name": metadata.name,
                "artist_ids": metadata.artist_ids,
                "album_id": metadata.album_id,
                "album_name": metadata.album_name,
                "duration_ms": metadata.duration_ms,
                "explicit": metadata.explicit,
                "popularity": metadata.popularity,
                "uri": metadata.uri,
            }
        )

    def write_artist_metadata(self, table_name: str, metadata: ArtistMetadata) -> None:
        """Cache artist metadata.

        Args:
            table_name: DynamoDB table name
            metadata: ArtistMetadata to cache
        """
        table = self.dynamodb.Table(table_name)
        table.put_item(
            Item={
                "artist_id": metadata.artist_id,
                "name": metadata.name,
                "genres": metadata.genres,
                "popularity": metadata.popularity,
                "uri": metadata.uri,
                "images": json.dumps(metadata.images),
            }
        )

    def get_track_metadata(self, table_name: str, track_id: str) -> Optional[dict]:
        """Get cached track metadata.

        Args:
            table_name: DynamoDB table name
            track_id: Spotify track ID

        Returns:
            Track metadata or None if not found
        """
        table = self.dynamodb.Table(table_name)
        response = table.get_item(Key={"track_id": track_id})
        return response.get("Item")

    def get_artist_metadata(self, table_name: str, artist_id: str) -> Optional[dict]:
        """Get cached artist metadata.

        Args:
            table_name: DynamoDB table name
            artist_id: Spotify artist ID

        Returns:
            Artist metadata or None if not found
        """
        table = self.dynamodb.Table(table_name)
        response = table.get_item(Key={"artist_id": artist_id})
        return response.get("Item")

    def query_plays_by_date_range(
        self, table_name: str, start_date: str, end_date: str
    ) -> list[dict]:
        """Query play events within a date range.

        Args:
            table_name: DynamoDB table name
            start_date: ISO format start date
            end_date: ISO format end date

        Returns:
            List of play events
        """
        table = self.dynamodb.Table(table_name)
        response = table.scan(
            FilterExpression="played_at BETWEEN :start AND :end",
            ExpressionAttributeValues={":start": start_date, ":end": end_date},
        )
        return response.get("Items", [])
