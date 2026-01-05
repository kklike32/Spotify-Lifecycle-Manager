"""DynamoDB interactions for storing play events and metadata.

This module provides cost-aware, idempotent storage operations for:
- Hot storage: Recent play events with TTL-based expiry
- Metadata cache: Tracks and artists (cache-once strategy)
- State storage: Pipeline state with race-safe updates

Design Principles:
- All writes are conditional (idempotent, safe to retry)
- Hot data expires automatically (TTL prevents unbounded growth)
- State updates are race-safe (conditional expressions)
- No unbounded queries (all operations bounded by design)

Cost Considerations:
- TTL deletions are free (DynamoDB handles automatically)
- Conditional writes prevent duplicate data (storage cost savings)
- Cache-once strategy prevents redundant API calls
"""

import json
import time
from datetime import datetime
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from spotify_lifecycle.models import (
    ArtistMetadata,
    IngestionState,
    PlayEvent,
    PlaylistState,
    TrackMetadata,
)


class DynamoDBClient:
    """DynamoDB client for storing and querying Spotify data.

    This client implements idempotent, cost-aware storage operations.
    All writes use conditional expressions to prevent overwrites.
    TTL is automatically applied to hot data to bound storage costs.
    """

    def __init__(self, region_name: str = "us-east-1"):
        """Initialize DynamoDB client.

        Args:
            region_name: AWS region for all DynamoDB operations
        """
        self.dynamodb = boto3.resource("dynamodb", region_name=region_name)
        self.client = boto3.client("dynamodb", region_name=region_name)

    def write_play_event(
        self, table_name: str, event: PlayEvent, play_id: str, ttl_days: int = 7
    ) -> bool:
        """Write a play event to DynamoDB with TTL and idempotency.

        Uses conditional write to prevent duplicate events. If the play_id
        already exists, the write is skipped (idempotent behavior).

        TTL (Time To Live) is automatically applied to bound storage costs.
        DynamoDB will delete expired items at no cost.

        Args:
            table_name: DynamoDB table name for hot storage
            event: PlayEvent to write
            play_id: Unique key for deduplication (from make_play_id)
            ttl_days: Number of days before automatic deletion (default: 7)

        Returns:
            bool: True if event was written, False if already exists (skip)

        Cost Impact:
            - 1 WCU per write (idempotent, no duplicate storage)
            - TTL deletion is free (DynamoDB handles automatically)
        """
        table = self.dynamodb.Table(table_name)

        # Calculate TTL timestamp (Unix epoch)
        ttl_timestamp = int(time.time() + (ttl_days * 24 * 60 * 60))

        try:
            table.put_item(
                Item={
                    "dedup_key": play_id,  # Use dedup_key as table hash key
                    "track_id": event.track_id,
                    "played_at": event.played_at.isoformat(),
                    "user_id": event.user_id,
                    "context": event.context or "",
                    "ttl": ttl_timestamp,  # TTL attribute (DynamoDB will auto-delete)
                },
                ConditionExpression="attribute_not_exists(dedup_key)",  # Only write if new
            )
            return True  # Event was written
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                # Dedup key already exists, skip (idempotent)
                return False
            raise  # Unexpected error, propagate

    def exists(self, table_name: str, key_name: str, key_value: str) -> bool:
        """Check if an item exists in DynamoDB.

        Used for explicit idempotency checks before writes.

        Args:
            table_name: DynamoDB table name
            key_name: Primary key attribute name
            key_value: Primary key value to check

        Returns:
            bool: True if item exists, False otherwise

        Cost Impact:
            - 0.5 RCU per check (eventually consistent)
        """
        table = self.dynamodb.Table(table_name)
        response = table.get_item(Key={key_name: key_value}, ConsistentRead=False)
        return "Item" in response

    def write_track_metadata(
        self, table_name: str, metadata: TrackMetadata, overwrite_existing: bool = False
    ) -> bool:
        """Cache track metadata with conditional write (cache-once strategy).

        Args:
            table_name: DynamoDB table name
            metadata: TrackMetadata to cache
            overwrite_existing: If True, overwrite existing record (used for repairs)

        Returns:
            bool: True if metadata was written, False if already cached

        Cost Impact:
            - 1 WCU per write (idempotent, no duplicate storage)
            - No TTL (metadata never expires, cache-once)
        """
        table = self.dynamodb.Table(table_name)
        try:
            item = {
                "track_id": metadata.track_id,
                "name": metadata.name,
                "artist_ids": metadata.artist_ids,
                "artist_names": metadata.artist_names,
                "album_id": metadata.album_id,
                "album_name": metadata.album_name,
                "duration_ms": metadata.duration_ms,
                "explicit": metadata.explicit,
                "popularity": metadata.popularity,
                "release_date": metadata.release_date,
                "uri": metadata.uri,
                "cached_at": metadata.cached_at.isoformat(),
                "version": metadata.version,
            }
            kwargs = {"Item": item}
            if not overwrite_existing:
                kwargs["ConditionExpression"] = "attribute_not_exists(track_id)"  # Cache-once

            table.put_item(**kwargs)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False  # Already cached
            raise

    def write_artist_metadata(self, table_name: str, metadata: ArtistMetadata) -> bool:
        """Cache artist metadata with conditional write (cache-once strategy).

        Args:
            table_name: DynamoDB table name
            metadata: ArtistMetadata to cache

        Returns:
            bool: True if metadata was written, False if already cached

        Cost Impact:
            - 1 WCU per write (idempotent, no duplicate storage)
            - No TTL (metadata never expires, cache-once)
        """
        table = self.dynamodb.Table(table_name)
        try:
            table.put_item(
                Item={
                    "artist_id": metadata.artist_id,
                    "name": metadata.name,
                    "genres": metadata.genres,
                    "popularity": metadata.popularity,
                    "uri": metadata.uri,
                    "images": json.dumps(metadata.images),
                },
                ConditionExpression="attribute_not_exists(artist_id)",  # Cache-once
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False  # Already cached
            raise

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

    # ==========================================
    # STATE STORE OPERATIONS (Race-Safe)
    # ==========================================

    def get_ingestion_cursor(self, table_name: str, user_id: str) -> Optional[str]:
        """Get the last ingestion cursor (last_played_at timestamp).

        Used by ingestion pipeline to track progress and implement overlap windows.

        Args:
            table_name: DynamoDB state table name
            user_id: Spotify user ID (partition key)

        Returns:
            ISO format timestamp of last play fetched, or None if first run

        Cost Impact:
            - 0.5 RCU per read (eventually consistent)
        """
        table = self.dynamodb.Table(table_name)
        response = table.get_item(
            Key={"state_key": f"ingestion_cursor#{user_id}"}, ConsistentRead=False
        )
        item = response.get("Item")
        return item.get("cursor_value") if item else None

    def set_ingestion_cursor(
        self, table_name: str, user_id: str, cursor: str, prev_cursor: Optional[str] = None
    ) -> bool:
        """Set the ingestion cursor with race-safe conditional update.

        Args:
            table_name: DynamoDB state table name
            user_id: Spotify user ID (partition key)
            cursor: New cursor value (ISO timestamp)
            prev_cursor: Expected previous cursor (for race detection)

        Returns:
            bool: True if cursor was updated, False if race condition detected

        Cost Impact:
            - 1 WCU per write (conditional)

        Race Safety:
            If prev_cursor is provided, update only succeeds if current cursor
            matches prev_cursor. This prevents concurrent pipeline runs from
            clobbering each other's progress.
        """
        table = self.dynamodb.Table(table_name)
        state_key = f"ingestion_cursor#{user_id}"

        try:
            if prev_cursor is not None:
                # Conditional update: only if cursor matches expected value
                table.put_item(
                    Item={
                        "state_key": state_key,
                        "cursor_value": cursor,
                        "updated_at": datetime.utcnow().isoformat(),
                    },
                    ConditionExpression="cursor_value = :prev",
                    ExpressionAttributeValues={":prev": prev_cursor},
                )
            else:
                # Unconditional update (first run or manual reset)
                table.put_item(
                    Item={
                        "state_key": state_key,
                        "cursor_value": cursor,
                        "updated_at": datetime.utcnow().isoformat(),
                    }
                )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                # Race condition detected: another pipeline run updated cursor
                return False
            raise

    def check_weekly_run_exists(self, table_name: str, week_id: str) -> bool:
        """Check if a weekly playlist run has already completed.

        Used by weekly playlist pipeline to implement idempotency.

        Args:
            table_name: DynamoDB state table name
            week_id: Week identifier (e.g., "2025-W52" from make_week_id)

        Returns:
            bool: True if weekly run already completed, False otherwise

        Cost Impact:
            - 0.5 RCU per check (eventually consistent)
        """
        table = self.dynamodb.Table(table_name)
        response = table.get_item(Key={"key": f"weekly_run#{week_id}"}, ConsistentRead=False)
        return "Item" in response

    def record_weekly_run(
        self, table_name: str, week_id: str, playlist_id: str, track_count: int
    ) -> bool:
        """Record a weekly playlist run with conditional write.

        Args:
            table_name: DynamoDB state table name
            week_id: Week identifier (e.g., "2025-W52")
            playlist_id: Spotify playlist ID created
            track_count: Number of tracks added to playlist

        Returns:
            bool: True if run was recorded, False if already exists

        Cost Impact:
            - 1 WCU per write (conditional)

        Idempotency:
            Conditional write prevents duplicate weekly runs if pipeline
            is retried or accidentally run multiple times in same week.
        """
        table = self.dynamodb.Table(table_name)
        state_key = f"weekly_run#{week_id}"

        try:
            table.put_item(
                Item={
                    "key": state_key,
                    "playlist_id": playlist_id,
                    "track_count": track_count,
                    "created_at": datetime.utcnow().isoformat(),
                },
                ConditionExpression="attribute_not_exists(key)",
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                # Weekly run already exists (idempotent retry)
                return False
            raise

    def get_ingestion_state(self, table_name: str, state_key: str) -> Optional["IngestionState"]:
        """Get current ingestion state (cursor tracking).

        Args:
            table_name: DynamoDB state table name
            state_key: State identifier (e.g., "ingestion_state")

        Returns:
            IngestionState if exists, None otherwise
        """
        from spotify_lifecycle.models import IngestionState

        table = self.dynamodb.Table(table_name)
        response = table.get_item(Key={"key": state_key}, ConsistentRead=True)

        if "Item" not in response:
            return None

        item = response["Item"]
        return IngestionState(
            state_key=item["key"],
            last_played_at=datetime.fromisoformat(item["last_played_at"]),
            last_run_at=datetime.fromisoformat(item["last_run_at"]),
            last_event_count=item["last_event_count"],
            status=item["status"],
        )

    def update_ingestion_state(
        self,
        table_name: str,
        state: "IngestionState",
        prev_cursor: Optional[datetime] = None,
    ) -> bool:
        """Update ingestion state with conditional write.

        Args:
            table_name: DynamoDB state table name
            state: New ingestion state
            prev_cursor: Previous cursor for conditional update (optional)

        Returns:
            bool: True if state was updated, False if condition failed
        """
        table = self.dynamodb.Table(table_name)

        item = {
            "key": state.state_key,
            "last_played_at": state.last_played_at.isoformat(),
            "last_run_at": state.last_run_at.isoformat(),
            "last_event_count": state.last_event_count,
            "status": state.status,
            "version": state.version,
        }

        # Conditional write to prevent race conditions
        if prev_cursor:
            try:
                table.put_item(
                    Item=item,
                    ConditionExpression="last_played_at = :prev_cursor",
                    ExpressionAttributeValues={
                        ":prev_cursor": prev_cursor.isoformat(),
                    },
                )
                return True
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return False
                raise
        else:
            # First run, no condition
            table.put_item(Item=item)
            return True

    # ==========================================
    # PLAYLIST OPERATIONS
    # ==========================================

    def get_recently_played_track_ids(self, table_name: str, lookback_days: int) -> set[str]:
        """Get set of track IDs played in last N days (from hot store).

        Scans hot store for recent plays and returns unique track IDs.
        Used by weekly playlist pipeline to filter out recently played tracks.

        Args:
            table_name: DynamoDB hot store table name
            lookback_days: Number of days to look back

        Returns:
            Set of Spotify track URIs played in lookback window

        Cost Impact:
            - Scan operation (bounded by TTL, max 7 days of data)
            - For typical usage (30 plays/day × 7 days = 210 items):
              ~1 RCU (eventually consistent)
            - Worst case (100 plays/day × 7 days = 700 items):
              ~3 RCU (eventually consistent)

        Performance:
            - Hot store is small (<1000 items due to TTL)
            - Scan completes in <100ms typically
            - No secondary index needed (bounded data size)

        Notes:
            - Returns track_id only (not full play events)
            - Deduplicates automatically via set
            - Time filtering done by checking played_at timestamps
        """
        from spotify_lifecycle.utils.time import days_ago

        table = self.dynamodb.Table(table_name)
        cutoff_time = days_ago(lookback_days)
        cutoff_str = cutoff_time.isoformat()

        # Scan hot store (bounded by TTL, safe for small datasets)
        track_ids = set()
        response = table.scan(
            FilterExpression="played_at >= :cutoff",
            ExpressionAttributeValues={":cutoff": cutoff_str},
            ProjectionExpression="track_id",  # Only fetch track_id (save bandwidth)
            ConsistentRead=False,  # Eventually consistent (cheaper)
        )

        # Collect track IDs
        for item in response.get("Items", []):
            track_ids.add(item["track_id"])

        # Handle pagination (if hot store grows beyond 1MB)
        while "LastEvaluatedKey" in response:
            response = table.scan(
                FilterExpression="played_at >= :cutoff",
                ExpressionAttributeValues={":cutoff": cutoff_str},
                ProjectionExpression="track_id",
                ConsistentRead=False,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            for item in response.get("Items", []):
                track_ids.add(item["track_id"])

        return track_ids

    def get_playlist_state(self, table_name: str, state_key: str) -> Optional["PlaylistState"]:
        """Get playlist state (check if weekly playlist already created).

        Args:
            table_name: DynamoDB state table name
            state_key: Playlist state key (from make_playlist_state_key)

        Returns:
            PlaylistState if exists, None otherwise

        Cost Impact:
            - 0.5 RCU per read (eventually consistent)
        """
        from spotify_lifecycle.models import PlaylistState

        table = self.dynamodb.Table(table_name)
        response = table.get_item(Key={"key": state_key}, ConsistentRead=False)

        if "Item" not in response:
            return None

        item = response["Item"]
        return PlaylistState(
            state_key=item["key"],
            week_id=item["week_id"],
            playlist_id=item["playlist_id"],
            created_at=datetime.fromisoformat(item["created_at"]),
            track_count=item["track_count"],
            source_playlist_id=item["source_playlist_id"],
        )

    def write_playlist_state(self, table_name: str, state: "PlaylistState") -> bool:
        """Write playlist state with conditional write (idempotency).

        Prevents duplicate playlists from being created on retry.

        Args:
            table_name: DynamoDB state table name
            state: PlaylistState to write

        Returns:
            bool: True if state was written, False if already exists

        Cost Impact:
            - 1 WCU per write (conditional)

        Idempotency:
            Conditional write ensures exactly-once playlist creation per week.
            If pipeline retries, this write fails gracefully and returns False.
        """
        table = self.dynamodb.Table(table_name)

        item = {
            "key": state.state_key,
            "week_id": state.week_id,
            "playlist_id": state.playlist_id,
            "created_at": state.created_at.isoformat(),
            "track_count": state.track_count,
            "source_playlist_id": state.source_playlist_id,
            "version": state.version,
        }

        try:
            table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(#k)",  # Only write if new
                ExpressionAttributeNames={"#k": "key"},  # 'key' is a reserved word
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                # State already exists (idempotent retry)
                return False
            raise
