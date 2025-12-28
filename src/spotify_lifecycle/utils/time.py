"""Time utilities for windowing, partitioning, and cursor management.

This module provides pure functions for time-based operations used throughout the pipeline.
All functions are deterministic and safe to call multiple times.

See: copilot/docs/architecture/IDEMPOTENCY.md for cursor strategy details.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional


def utc_now() -> datetime:
    """Get current UTC time (timezone-aware).

    Returns:
        Current datetime in UTC

    Examples:
        >>> utc_now()
        datetime.datetime(2025, 12, 27, 14, 30, 0, tzinfo=datetime.timezone.utc)

    Notes:
        - Always timezone-aware (UTC)
        - Use this instead of datetime.utcnow() (which is naive)
        - Consistent with Spotify API timestamps
    """
    return datetime.now(timezone.utc)


def days_ago(days: int, from_time: Optional[datetime] = None) -> datetime:
    """Get a datetime from N days ago.

    Args:
        days: Number of days ago
        from_time: Reference time (default: now UTC)

    Returns:
        Datetime object (days before reference time)

    Examples:
        >>> days_ago(7)
        datetime.datetime(2025, 12, 20, 14, 30, 0, tzinfo=datetime.timezone.utc)

        >>> days_ago(1, from_time=datetime(2025, 12, 27, 12, 0, 0, tzinfo=timezone.utc))
        datetime.datetime(2025, 12, 26, 12, 0, 0, tzinfo=datetime.timezone.utc)

    Notes:
        - Always returns timezone-aware datetime (UTC)
        - Useful for time-window queries (e.g., last 7 days)
    """
    if from_time is None:
        from_time = utc_now()
    return from_time - timedelta(days=days)


def hours_ago(hours: int, from_time: Optional[datetime] = None) -> datetime:
    """Get a datetime from N hours ago.

    Args:
        hours: Number of hours ago
        from_time: Reference time (default: now UTC)

    Returns:
        Datetime object (hours before reference time)

    Examples:
        >>> hours_ago(2)
        datetime.datetime(2025, 12, 27, 12, 30, 0, tzinfo=datetime.timezone.utc)

    Notes:
        - Always returns timezone-aware datetime (UTC)
        - Useful for enrichment queries (e.g., last hour)
    """
    if from_time is None:
        from_time = utc_now()
    return from_time - timedelta(hours=hours)


def minutes_ago(minutes: int, from_time: Optional[datetime] = None) -> datetime:
    """Get a datetime from N minutes ago.

    Args:
        minutes: Number of minutes ago
        from_time: Reference time (default: now UTC)

    Returns:
        Datetime object (minutes before reference time)

    Examples:
        >>> minutes_ago(5)
        datetime.datetime(2025, 12, 27, 14, 25, 0, tzinfo=datetime.timezone.utc)

    Notes:
        - Always returns timezone-aware datetime (UTC)
        - Useful for overlap window in ingestion
    """
    if from_time is None:
        from_time = utc_now()
    return from_time - timedelta(minutes=minutes)


def iso_to_datetime(iso_string: str) -> datetime:
    """Convert ISO format string to datetime (timezone-aware UTC).

    Handles both formats:
    - With Z suffix: "2025-12-27T14:30:00Z"
    - With +00:00: "2025-12-27T14:30:00+00:00"

    Args:
        iso_string: ISO 8601 datetime string

    Returns:
        Timezone-aware datetime object (UTC)

    Examples:
        >>> iso_to_datetime("2025-12-27T14:30:00Z")
        datetime.datetime(2025, 12, 27, 14, 30, 0, tzinfo=datetime.timezone.utc)

        >>> iso_to_datetime("2025-12-27T14:30:00+00:00")
        datetime.datetime(2025, 12, 27, 14, 30, 0, tzinfo=datetime.timezone.utc)

    Raises:
        ValueError: If iso_string is not a valid ISO 8601 format

    Notes:
        - Always returns UTC timezone
        - Handles Spotify API timestamp format
        - Use for deserializing stored timestamps
    """
    # Replace Z with +00:00 for consistent parsing
    normalized = iso_string.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError as e:
        raise ValueError(f"Invalid ISO 8601 timestamp: {iso_string}") from e


def datetime_to_iso(dt: datetime) -> str:
    """Convert datetime to ISO format string (with Z suffix for UTC).

    Args:
        dt: Datetime object (preferably timezone-aware)

    Returns:
        ISO 8601 string with Z suffix

    Examples:
        >>> datetime_to_iso(datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc))
        '2025-12-27T14:30:00Z'

    Notes:
        - Outputs Z suffix for UTC (standard format)
        - Use for serializing to storage/API
        - If dt is naive (no timezone), assumes UTC
    """
    # Ensure UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    elif dt.tzinfo != timezone.utc:
        dt = dt.astimezone(timezone.utc)

    # Format with Z suffix
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_partition_key(dt: datetime, prefix: str = "dt") -> str:
    """Generate S3 partition key from datetime (Hive-style partitioning).

    Args:
        dt: Datetime to partition by
        prefix: Partition column name (default: "dt")

    Returns:
        Partition key in format: {prefix}=YYYY-MM-DD

    Examples:
        >>> make_partition_key(datetime(2025, 12, 27, 14, 30, 0))
        'dt=2025-12-27'

        >>> make_partition_key(datetime(2025, 12, 27), prefix="date")
        'date=2025-12-27'

    Notes:
        - Hive-style partitioning for data lake
        - Date only (no time component)
        - Use for organizing S3 cold storage
    """
    date_str = dt.strftime("%Y-%m-%d")
    return f"{prefix}={date_str}"


def get_date_range(start: datetime, end: datetime) -> list[datetime]:
    """Generate list of dates between start and end (inclusive).

    Args:
        start: Start datetime
        end: End datetime

    Returns:
        List of datetime objects (one per day)

    Examples:
        >>> get_date_range(
        ...     datetime(2025, 12, 25, tzinfo=timezone.utc),
        ...     datetime(2025, 12, 27, tzinfo=timezone.utc)
        ... )
        [datetime.datetime(2025, 12, 25, 0, 0, 0, tzinfo=datetime.timezone.utc),
         datetime.datetime(2025, 12, 26, 0, 0, 0, tzinfo=datetime.timezone.utc),
         datetime.datetime(2025, 12, 27, 0, 0, 0, tzinfo=datetime.timezone.utc)]

    Notes:
        - Inclusive range (includes both start and end dates)
        - Returns midnight (00:00:00) for each date
        - Useful for iterating partitions
    """
    dates = []
    current = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end.replace(hour=0, minute=0, second=0, microsecond=0)

    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    return dates


def apply_overlap_window(cursor_time: datetime, overlap_minutes: int = 5) -> datetime:
    """Apply overlap window to cursor for safe incremental fetching.

    This is the core of our gap-prevention strategy. By fetching events before
    the cursor time, we ensure no events are missed even if the cursor is stale.

    Args:
        cursor_time: Last processed timestamp (from state)
        overlap_minutes: Minutes to overlap (default: 5)

    Returns:
        Adjusted fetch time (cursor_time - overlap_minutes)

    Examples:
        >>> apply_overlap_window(
        ...     datetime(2025, 12, 27, 14, 30, 0, tzinfo=timezone.utc),
        ...     overlap_minutes=5
        ... )
        datetime.datetime(2025, 12, 27, 14, 25, 0, tzinfo=datetime.timezone.utc)

    Notes:
        - Default 5-minute overlap (configurable)
        - Dedup at write time prevents duplicates
        - See: copilot/docs/architecture/IDEMPOTENCY.md (Cursor Semantics)
    """
    return cursor_time - timedelta(minutes=overlap_minutes)


def is_recent(dt: datetime, threshold_hours: int = 24) -> bool:
    """Check if datetime is within threshold hours from now.

    Args:
        dt: Datetime to check
        threshold_hours: Hours threshold (default: 24)

    Returns:
        True if within threshold, False otherwise

    Examples:
        >>> is_recent(utc_now(), threshold_hours=1)
        True

        >>> is_recent(days_ago(2), threshold_hours=24)
        False

    Notes:
        - Useful for determining if data is stale
        - Always uses UTC for comparison
    """
    threshold = hours_ago(threshold_hours)
    return dt >= threshold


def truncate_to_hour(dt: datetime) -> datetime:
    """Truncate datetime to hour boundary (zero minutes/seconds).

    Args:
        dt: Datetime to truncate

    Returns:
        Datetime with minutes, seconds, microseconds set to 0

    Examples:
        >>> truncate_to_hour(datetime(2025, 12, 27, 14, 35, 42, tzinfo=timezone.utc))
        datetime.datetime(2025, 12, 27, 14, 0, 0, tzinfo=datetime.timezone.utc)

    Notes:
        - Preserves timezone
        - Useful for hourly aggregations
    """
    return dt.replace(minute=0, second=0, microsecond=0)


def truncate_to_day(dt: datetime) -> datetime:
    """Truncate datetime to day boundary (midnight).

    Args:
        dt: Datetime to truncate

    Returns:
        Datetime with time set to 00:00:00

    Examples:
        >>> truncate_to_day(datetime(2025, 12, 27, 14, 35, 42, tzinfo=timezone.utc))
        datetime.datetime(2025, 12, 27, 0, 0, 0, tzinfo=datetime.timezone.utc)

    Notes:
        - Preserves timezone
        - Useful for daily aggregations and partitioning
    """
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)
