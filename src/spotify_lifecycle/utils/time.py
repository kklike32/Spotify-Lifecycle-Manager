"""Time utilities."""

from datetime import datetime, timedelta


def days_ago(days: int) -> datetime:
    """Get a datetime from N days ago.

    Args:
        days: Number of days ago

    Returns:
        datetime object
    """
    return datetime.now() - timedelta(days=days)


def iso_to_datetime(iso_string: str) -> datetime:
    """Convert ISO format string to datetime.

    Args:
        iso_string: ISO format datetime string

    Returns:
        datetime object
    """
    return datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
