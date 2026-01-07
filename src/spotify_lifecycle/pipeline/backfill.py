"""Automated backfill pipeline for missing daily summaries.

This module checks for missing daily summaries and regenerates them from raw
S3 events. It's designed to run automatically before the aggregation Lambda
to ensure no data gaps in the dashboard.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import boto3

logger = logging.getLogger(__name__)


def check_and_backfill_summaries(
    bucket_name: str,
    days_to_check: int = 7,
    region_name: str = "us-east-1",
) -> dict:
    """Check for missing summaries and backfill if needed.

    Args:
        bucket_name: S3 bucket name for raw events
        days_to_check: Number of days back to check (default 7)
        region_name: AWS region

    Returns:
        dict with backfill results:
            {
                "checked_days": int,
                "missing_days": int,
                "backfilled": int,
                "errors": int,
                "missing_dates": list[str]
            }
    """
    from spotify_lifecycle.storage.s3 import S3ColdStore

    s3_client = S3ColdStore(region_name)
    s3 = boto3.client("s3", region_name=region_name)

    # Check last N days
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=days_to_check)

    logger.info(f"Checking for missing summaries from {start_date.date()} " f"to {end_date.date()}")

    # Find dates with events but no summaries
    dates_with_events = set()
    dates_with_summaries = set()

    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        event_prefix = f"dt={date_str}/"
        summary_key = f"summaries/dt={date_str}/summary.json"

        # Check for events
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=event_prefix, MaxKeys=1)
        if response.get("KeyCount", 0) > 0:
            dates_with_events.add(current)

        # Check for summary
        try:
            s3.head_object(Bucket=bucket_name, Key=summary_key)
            dates_with_summaries.add(current)
        except Exception:
            pass  # Summary doesn't exist

        current += timedelta(days=1)

    missing_dates = sorted(dates_with_events - dates_with_summaries)

    if not missing_dates:
        logger.info(f"No missing summaries found in last {days_to_check} days")
        return {
            "checked_days": days_to_check,
            "missing_days": 0,
            "backfilled": 0,
            "errors": 0,
            "missing_dates": [],
        }

    logger.warning(
        f"Found {len(missing_dates)} missing summaries: "
        f"{[d.strftime('%Y-%m-%d') for d in missing_dates]}"
    )

    # Backfill missing summaries
    backfilled = 0
    errors = 0

    for date in missing_dates:
        date_str = date.strftime("%Y-%m-%d")
        try:
            logger.info(f"Backfilling summary for {date_str}")
            s3_client.write_daily_summary(bucket_name, date)
            backfilled += 1
            logger.info(f"Successfully backfilled {date_str}")
        except Exception as e:
            errors += 1
            logger.error(
                f"Failed to backfill {date_str}: {type(e).__name__}: {str(e)}",
                exc_info=True,
            )

    result = {
        "checked_days": days_to_check,
        "missing_days": len(missing_dates),
        "backfilled": backfilled,
        "errors": errors,
        "missing_dates": [d.strftime("%Y-%m-%d") for d in missing_dates],
    }

    if backfilled > 0:
        logger.info(f"Backfill complete: {backfilled}/{len(missing_dates)} successful")

    if errors > 0:
        logger.error(f"Backfill had {errors} errors - check logs for details")

    return result
