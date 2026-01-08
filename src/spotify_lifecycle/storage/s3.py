"""S3 interactions for cold storage (data lake) and dashboard data.

This module implements append-only, date-partitioned cold storage for long-term
play history. Design principles:

1. **Append-only**: Never overwrite existing data, only add new partitions
2. **Date partitioning**: Organize by dt=YYYY-MM-DD for query efficiency
3. **JSONL format**: One JSON object per line for streaming and cost efficiency
4. **Bounded costs**: Storage scales linearly, queries are time-scoped

For architecture details, see: copilot/docs/architecture/DATA_LAKE.md
For cost analysis, see: copilot/docs/cost/ANALYTICS_COSTS.md
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Iterator, Optional
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError

from spotify_lifecycle.models import PlayEvent

logger = logging.getLogger(__name__)
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")


class S3ColdStore:
    """Cold storage (data lake) for long-term play history.

    This class implements append-only, date-partitioned storage in S3.
    All writes are idempotent and safe to retry.

    Partition Strategy:
        - Path format: s3://bucket/dt=YYYY-MM-DD/events_HHMMSS.jsonl
        - One partition per day
        - Multiple files per partition (append-only)
        - JSONL format (one JSON per line)

    Cost Characteristics:
        - Storage: ~$0.023/GB/month (S3 Standard)
        - PUT: $0.005 per 1000 requests
        - GET: $0.0004 per 1000 requests
        - See: copilot/docs/cost/ANALYTICS_COSTS.md for projections
    """

    def __init__(self, region_name: str = "us-east-1"):
        """Initialize S3 cold store client.

        Args:
            region_name: AWS region for S3 bucket
        """
        self.s3 = boto3.client("s3", region_name=region_name)
        self.region_name = region_name

    def write_play_events(
        self,
        bucket_name: str,
        partition_date: datetime,
        events: list[PlayEvent],
    ) -> str:
        """Write play events to cold storage (append-only).

        This method is idempotent and safe to retry. Each call writes a new file
        with a unique timestamp suffix. Never overwrites existing data.

        Args:
            bucket_name: S3 bucket name for cold storage
            partition_date: Date for partition (typically event played_at date)
            events: List of PlayEvent objects to store

        Returns:
            S3 object key where events were written

        Raises:
            ValueError: If events list is empty
            ClientError: If S3 write fails

        Example:
            >>> store = S3ColdStore()
            >>> events = [PlayEvent(...), PlayEvent(...)]
            >>> key = store.write_play_events("my-bucket", datetime(2025, 1, 1), events)
            >>> print(key)  # "dt=2025-01-01/events_120530.jsonl"
        """
        if not events:
            raise ValueError("Cannot write empty events list")

        # Generate unique file key (append-only, never overwrite)
        timestamp = datetime.utcnow().strftime("%H%M%S")
        partition_key = partition_date.strftime("%Y-%m-%d")
        object_key = f"dt={partition_key}/events_{timestamp}.jsonl"

        # Serialize to JSONL (one JSON per line)
        jsonl_lines = [event.model_dump_json() for event in events]
        content = "\n".join(jsonl_lines) + "\n"  # Trailing newline for consistency

        # Write to S3 (idempotent: new file each time)
        self.s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=content.encode("utf-8"),
            ContentType="application/x-jsonlines",
        )

        logger.info(
            "wrote play events to cold storage",
            extra={
                "bucket": bucket_name,
                "key": object_key,
                "count": len(events),
                "bytes": len(content.encode("utf-8")),
            },
        )

        return object_key

    def write_raw_events(
        self,
        bucket_name: str,
        partition_date: datetime,
        events: list[dict],
    ) -> str:
        """Write raw play events (already dict-serialized) to cold storage."""
        if not events:
            raise ValueError("Cannot write empty events list")

        timestamp = datetime.utcnow().strftime("%H%M%S")
        partition_key = partition_date.strftime("%Y-%m-%d")
        object_key = f"dt={partition_key}/events_{timestamp}.jsonl"

        jsonl_lines = [json.dumps(event) for event in events]
        content = "\n".join(jsonl_lines) + "\n"

        self.s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=content.encode("utf-8"),
            ContentType="application/x-jsonlines",
        )

        logger.info(
            "wrote raw events to cold storage",
            extra={
                "bucket": bucket_name,
                "key": object_key,
                "count": len(events),
                "bytes": len(content.encode("utf-8")),
            },
        )

        return object_key

    def read_play_events(
        self,
        bucket_name: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Iterator[PlayEvent]:
        """Read play events from cold storage within date range.

        This method streams events from all partitions in the date range.
        Does not load entire dataset into memory (generator pattern).

        Args:
            bucket_name: S3 bucket name for cold storage
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Yields:
            PlayEvent: Individual play events from storage

        Example:
            >>> store = S3ColdStore()
            >>> start = datetime(2025, 1, 1)
            >>> end = datetime(2025, 1, 7)
            >>> for event in store.read_play_events("my-bucket", start, end):
            ...     print(event.track_id)
        """
        # List all object keys in date range
        object_keys = self._list_partition_keys(bucket_name, start_date, end_date)

        # Stream events from each file
        for key in object_keys:
            yield from self._read_jsonl_file(bucket_name, key)

    def _list_partition_keys(
        self,
        bucket_name: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[str]:
        """List all object keys within date range.

        Args:
            bucket_name: S3 bucket name
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of S3 object keys in date range
        """
        object_keys = []
        current_date = start_date

        while current_date <= end_date:
            partition_key = current_date.strftime("%Y-%m-%d")
            prefix = f"dt={partition_key}/"

            # List all files in this partition
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if "Contents" in page:
                    object_keys.extend([obj["Key"] for obj in page["Contents"]])

            current_date += timedelta(days=1)

        return object_keys

    def _read_jsonl_file(self, bucket_name: str, object_key: str) -> Iterator[PlayEvent]:
        """Read and parse a single JSONL file from S3.

        Args:
            bucket_name: S3 bucket name
            object_key: S3 object key

        Yields:
            PlayEvent: Parsed play events from file
        """
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=object_key)
            content = response["Body"].read().decode("utf-8")

            # Parse JSONL (one JSON per line)
            for line in content.strip().split("\n"):
                if line:  # Skip empty lines
                    event_dict = json.loads(line)
                    yield PlayEvent(**event_dict)

        except ClientError as e:
            logger.error(
                "failed to read jsonl file",
                extra={
                    "bucket": bucket_name,
                    "key": object_key,
                    "error": str(e),
                },
            )
            raise

    def get_partition_stats(
        self,
        bucket_name: str,
        start_date: datetime,
        end_date: datetime,
    ) -> dict:
        """Get storage statistics for partitions in date range.

        Useful for monitoring storage costs and data volume.

        Args:
            bucket_name: S3 bucket name
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            dict: Statistics including:
                - partition_count: Number of partitions
                - file_count: Total files across partitions
                - total_bytes: Total storage used
                - avg_bytes_per_partition: Average partition size

        Example:
            >>> store = S3ColdStore()
            >>> stats = store.get_partition_stats("my-bucket", start, end)
            >>> print(f"Total storage: {stats['total_bytes'] / (1024**2):.2f} MB")
        """
        object_keys = self._list_partition_keys(bucket_name, start_date, end_date)

        if not object_keys:
            return {
                "partition_count": 0,
                "file_count": 0,
                "total_bytes": 0,
                "avg_bytes_per_partition": 0,
            }

        # Get object metadata
        total_bytes = 0
        partitions = set()

        for key in object_keys:
            # Extract partition from key (dt=YYYY-MM-DD/...)
            partition = key.split("/")[0]
            partitions.add(partition)

            # Get object size
            try:
                response = self.s3.head_object(Bucket=bucket_name, Key=key)
                total_bytes += response["ContentLength"]
            except ClientError:
                pass  # Skip missing objects

        partition_count = len(partitions)
        avg_bytes = total_bytes / partition_count if partition_count > 0 else 0

        return {
            "partition_count": partition_count,
            "file_count": len(object_keys),
            "total_bytes": total_bytes,
            "avg_bytes_per_partition": avg_bytes,
        }

    # ==========================================
    # DAILY SUMMARY OPERATIONS
    # ==========================================

    def _daily_summary_key(self, partition_date: datetime) -> str:
        """Return deterministic summary key for a given date."""
        date_str = partition_date.strftime("%Y-%m-%d")
        return f"summaries/dt={date_str}/summary.json"

    def _counts_match(self, current: dict[str, int], existing: dict[str, int]) -> bool:
        """Check whether two count maps are identical (idempotency helper)."""
        if len(current) != len(existing):
            return False
        for track_id, count in current.items():
            if int(existing.get(track_id, -1)) != int(count):
                return False
        return True

    def _calculate_daily_track_counts(
        self, bucket_name: str, partition_date: datetime
    ) -> dict[str, int]:
        """Aggregate track counts for a day by reading raw event files.

        Deduplicates by play_id to avoid double-counting overlapping ingest runs.
        """
        object_keys = self._list_partition_keys(bucket_name, partition_date, partition_date)
        counts: dict[str, int] = defaultdict(int)
        seen_play_ids: set[str] = set()

        for key in object_keys:
            try:
                for event in self._read_jsonl_file(bucket_name, key):
                    play_id = getattr(event, "play_id", None)
                    if play_id and play_id in seen_play_ids:
                        continue  # skip duplicate events across files
                    if play_id:
                        seen_play_ids.add(play_id)
                    counts[getattr(event, "track_id", "")] += 1
            except Exception as e:
                logger.error(
                    "failed to read events while calculating summary",
                    extra={"bucket": bucket_name, "key": key, "error": str(e)},
                )

        return counts

    def write_daily_summary(
        self,
        bucket_name: str,
        partition_date: datetime,
        track_counts: Optional[dict[str, int]] = None,
    ) -> str:
        """Write daily play summary (idempotent: replace, never merge).

        This function can be called many times per day (hourly ingest retries).
        It must not accumulate prior counts, otherwise totals will multiply.

        Raises:
            Exception: If summary calculation or write fails (propagated for logging)
        """
        key = self._daily_summary_key(partition_date)
        date_str = partition_date.strftime("%Y-%m-%d")

        if track_counts is None:
            try:
                track_counts = self._calculate_daily_track_counts(bucket_name, partition_date)
                event_files = len(
                    self._list_partition_keys(bucket_name, partition_date, partition_date)
                )
                logger.info(
                    f"calculated daily summary from {event_files} event files for {date_str}",
                    extra={
                        "bucket": bucket_name,
                        "key": key,
                        "partition_date": date_str,
                        "event_files": event_files,
                        "unique_tracks": len(track_counts),
                    },
                )
            except Exception as e:
                logger.error(
                    f"failed to calculate track counts for {date_str}: "
                    f"{type(e).__name__}: {str(e)}",
                    exc_info=True,
                )
                raise  # Re-raise to trigger caller's error handling

        normalized_counts = {track_id: int(count) for track_id, count in track_counts.items()}
        incoming_total = int(sum(normalized_counts.values()))

        # Read existing summary (if any) to check if update needed
        try:
            existing = self.read_daily_summary(bucket_name, partition_date)
        except Exception as e:
            logger.warning(
                f"failed to read existing summary for {date_str}, will create new: "
                f"{type(e).__name__}: {str(e)}"
            )
            existing = None

        if existing and "track_counts" in existing:
            if self._counts_match(normalized_counts, existing["track_counts"]):
                logger.info(
                    "daily summary already up to date",
                    extra={
                        "bucket": bucket_name,
                        "key": key,
                        "total_plays": existing.get("total_plays", 0),
                        "status": "unchanged",
                    },
                )
                return key

            # Mismatch detected - distinguish expected from unexpected changes
            existing_total = existing.get("total_plays", 0)
            delta = incoming_total - existing_total

            if delta > 0:
                # Expected: new events arrived between runs (normal for hourly ingest)
                logger.info(
                    f"daily summary updated: {delta} new plays added",
                    extra={
                        "bucket": bucket_name,
                        "key": key,
                        "existing_total": existing_total,
                        "incoming_total": incoming_total,
                        "delta": delta,
                        "reason": "new_events_arrived",
                    },
                )
            elif delta < 0:
                # Unexpected: count decreased (possible bug - data loss, deduplication issue, etc.)
                logger.error(
                    f"daily summary mismatch: count DECREASED by {abs(delta)} plays (unexpected)",
                    extra={
                        "bucket": bucket_name,
                        "key": key,
                        "existing_total": existing_total,
                        "incoming_total": incoming_total,
                        "delta": delta,
                        "reason": "count_decreased",
                        "severity": "high",
                    },
                )
            else:
                # delta == 0 but counts still differ (different tracks with same total)
                logger.warning(
                    "daily summary mismatch: same total but different track distribution",
                    extra={
                        "bucket": bucket_name,
                        "key": key,
                        "existing_total": existing_total,
                        "incoming_total": incoming_total,
                        "delta": 0,
                        "reason": "track_distribution_changed",
                    },
                )

        payload = {
            "version": "2.0.0",
            "date": partition_date.strftime("%Y-%m-%d"),
            "generated_at": datetime.now(PACIFIC_TZ).isoformat(),
            "total_plays": incoming_total,
            "track_counts": normalized_counts,
        }

        # Write summary to S3
        try:
            self.s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=json.dumps(payload).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as e:
            logger.error(
                f"failed to write summary to S3 for {date_str}: {type(e).__name__}: {str(e)}",
                exc_info=True,
            )
            raise  # Re-raise to trigger caller's error handling

        logger.info(
            f"wrote daily summary for {date_str}: {incoming_total} plays "
            f"across {len(normalized_counts)} tracks",
            extra={
                "bucket": bucket_name,
                "key": key,
                "total_plays": payload["total_plays"],
                "unique_tracks": len(normalized_counts),
                "status": "replaced" if existing else "created",
            },
        )

        return key

    def read_daily_summary(self, bucket_name: str, partition_date: datetime) -> Optional[dict]:
        """Read a single daily summary if it exists."""
        key = self._daily_summary_key(partition_date)
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=key)
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise

    def list_daily_summary_dates(self, bucket_name: str) -> list[datetime]:
        """List available summary dates from S3."""
        paginator = self.s3.get_paginator("list_objects_v2")
        dates: list[datetime] = []
        prefix = "summaries/"
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                try:
                    date_part = key.split("/")[1].replace("dt=", "")
                    dates.append(datetime.strptime(date_part, "%Y-%m-%d"))
                except (IndexError, ValueError):
                    continue
        return dates

    def read_daily_summaries(
        self, bucket_name: str, start_date: datetime, end_date: datetime
    ) -> list[dict]:
        """Read summaries in a date range (inclusive)."""
        summaries: list[dict] = []
        current = start_date
        while current <= end_date:
            summary = self.read_daily_summary(bucket_name, current)
            if summary:
                summaries.append(summary)
            current += timedelta(days=1)
        return summaries


class S3DashboardStore:
    """Dashboard artifact storage (static JSON).

    Separate class from cold store for clarity and different access patterns.
    Dashboard data is small, frequently read, infrequently written.
    """

    def __init__(self, region_name: str = "us-east-1"):
        """Initialize S3 dashboard store client.

        Args:
            region_name: AWS region for S3 bucket
        """
        self.s3 = boto3.client("s3", region_name=region_name)
        self.region_name = region_name

    def write_dashboard_data(self, bucket_name: str, dashboard_json: dict) -> None:
        """Write dashboard data JSON artifact with short cache.

        This file is read by the static dashboard website. It contains
        precomputed analytics (no live querying required).

        Cache Strategy:
        - Cache-Control: max-age=300, must-revalidate (5 minutes)
        - Forces CloudFront to revalidate with origin after 5 minutes
        - Prevents stale data served from edge locations

        Args:
            bucket_name: S3 bucket name for dashboard
            dashboard_json: Precomputed dashboard data

        Example:
            >>> store = S3DashboardStore()
            >>> data = {"top_tracks": [...], "listening_trends": [...]}
            >>> store.write_dashboard_data("my-dashboard-bucket", data)
        """
        self.s3.put_object(
            Bucket=bucket_name,
            Key="dashboard_data.json",
            Body=json.dumps(dashboard_json, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="max-age=300, must-revalidate",  # 5 minutes, must check origin
        )

        logger.info(
            "wrote dashboard data",
            extra={
                "bucket": bucket_name,
                "key": "dashboard_data.json",
            },
        )

    def read_dashboard_data(self, bucket_name: str) -> Optional[dict]:
        """Read dashboard data JSON artifact.

        Args:
            bucket_name: S3 bucket name

        Returns:
            Dashboard data dict or None if not found

        Example:
            >>> store = S3DashboardStore()
            >>> data = store.read_dashboard_data("my-dashboard-bucket")
            >>> if data:
            ...     print(data["top_tracks"])
        """
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key="dashboard_data.json")
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning("dashboard data not found", extra={"bucket": bucket_name})
                return None
            raise
