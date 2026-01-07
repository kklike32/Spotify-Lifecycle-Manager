"""Backfill missing daily summaries from raw S3 events.

This script detects missing daily summary files and regenerates them from raw
event files. It's designed to fix gaps caused by write_daily_summary() failures.

Usage:
    # Check and fix last 7 days
    uv run python scripts/backfill_missing_summaries.py

    # Check and fix specific date range
    uv run python scripts/backfill_missing_summaries.py \\
        --start-date 2026-01-01 --end-date 2026-01-07

    # Dry run (check only, don't write)
    uv run python scripts/backfill_missing_summaries.py --dry-run

    # Force regenerate all summaries (even if they exist)
    uv run python scripts/backfill_missing_summaries.py --force
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone

from spotify_lifecycle.config import load_config
from spotify_lifecycle.storage.s3 import S3ColdStore

logger = logging.getLogger(__name__)


def get_date_range_with_events(
    s3_client: S3ColdStore, bucket: str, start_date: datetime, end_date: datetime
) -> set[datetime]:
    """Get all dates that have raw event files in S3.

    Args:
        s3_client: S3 cold store client
        bucket: S3 bucket name
        start_date: Start of date range (inclusive)
        end_date: End of date range (inclusive)

    Returns:
        Set of dates that have event files
    """
    import boto3

    s3 = boto3.client("s3", region_name=s3_client.region_name)
    dates_with_events = set()

    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        prefix = f"dt={date_str}/"

        # Check if any objects exist with this prefix
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if response.get("KeyCount", 0) > 0:
            dates_with_events.add(current)
            logger.info(f"Found events for {date_str}")

        current += timedelta(days=1)

    return dates_with_events


def get_existing_summaries(
    s3_client: S3ColdStore, bucket: str, start_date: datetime, end_date: datetime
) -> set[datetime]:
    """Get all dates that have summary files in S3.

    Args:
        s3_client: S3 cold store client
        bucket: S3 bucket name
        start_date: Start of date range (inclusive)
        end_date: End of date range (inclusive)

    Returns:
        Set of dates that have summary files
    """
    import boto3

    s3 = boto3.client("s3", region_name=s3_client.region_name)
    dates_with_summaries = set()

    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        key = f"summaries/dt={date_str}/summary.json"

        # Check if summary file exists
        try:
            s3.head_object(Bucket=bucket, Key=key)
            dates_with_summaries.add(current)
            logger.debug(f"Found summary for {date_str}")
        except Exception as e:
            error_code = (
                e.response.get("Error", {}).get("Code", "") if hasattr(e, "response") else ""
            )
            if error_code == "404":
                logger.debug(f"Missing summary for {date_str}")
            else:
                logger.warning(f"Error checking summary for {date_str}: {e}")

        current += timedelta(days=1)

    return dates_with_summaries


def backfill_missing_summaries(
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    """Backfill missing daily summaries from raw S3 events.

    Args:
        start_date: Start of date range (defaults to 30 days ago)
        end_date: End of date range (defaults to today)
        dry_run: If True, only check for missing summaries without writing
        force: If True, regenerate all summaries even if they exist
    """
    cfg = load_config()
    storage = cfg.storage
    s3_client = S3ColdStore(storage.region)

    # Default to last 30 days if no range specified
    if end_date is None:
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    if start_date is None:
        start_date = end_date - timedelta(days=30)

    logger.info(f"Checking summaries from {start_date.date()} to {end_date.date()}")

    # Get dates with events and existing summaries
    dates_with_events = get_date_range_with_events(
        s3_client, storage.raw_bucket_name, start_date, end_date
    )
    dates_with_summaries = get_existing_summaries(
        s3_client, storage.raw_bucket_name, start_date, end_date
    )

    logger.info(f"Dates with events: {len(dates_with_events)}")
    logger.info(f"Dates with summaries: {len(dates_with_summaries)}")

    # Determine which dates need summaries
    if force:
        dates_to_backfill = dates_with_events
        logger.info(f"Force mode: regenerating all {len(dates_to_backfill)} summaries")
    else:
        dates_to_backfill = dates_with_events - dates_with_summaries
        logger.info(f"Missing summaries: {len(dates_to_backfill)}")

    if not dates_to_backfill:
        logger.info("No missing summaries found!")
        return

    # Sort dates for consistent processing
    sorted_dates = sorted(dates_to_backfill)

    if dry_run:
        logger.info("DRY RUN MODE - would backfill the following dates:")
        for date in sorted_dates:
            logger.info(f"  - {date.date()}")
        return

    # Backfill each missing date
    success_count = 0
    error_count = 0

    for date in sorted_dates:
        date_str = date.strftime("%Y-%m-%d")
        try:
            logger.info(f"Backfilling summary for {date_str}...")
            s3_client.write_daily_summary(storage.raw_bucket_name, date)
            success_count += 1
            logger.info(f"✓ Successfully created summary for {date_str}")
        except Exception as e:
            error_count += 1
            logger.error(
                f"✗ Failed to create summary for {date_str}: {type(e).__name__}: {str(e)}",
                exc_info=True,
            )

    # Summary
    logger.info("=" * 60)
    logger.info("Backfill complete!")
    logger.info(f"  Success: {success_count}/{len(sorted_dates)}")
    logger.info(f"  Errors:  {error_count}/{len(sorted_dates)}")
    logger.info("=" * 60)

    if error_count > 0:
        logger.warning(f"{error_count} summaries failed to backfill - check logs above")


def main():
    parser = argparse.ArgumentParser(
        description="Backfill missing daily summaries from raw S3 events"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD), defaults to 30 days ago",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check for missing summaries without writing",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate all summaries even if they exist",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose debug logging",
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Parse dates
    start_date = None
    end_date = None

    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error(f"Invalid start date format: {args.start_date} (use YYYY-MM-DD)")
            return

    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error(f"Invalid end date format: {args.end_date} (use YYYY-MM-DD)")
            return

    # Run backfill
    backfill_missing_summaries(
        start_date=start_date,
        end_date=end_date,
        dry_run=args.dry_run,
        force=args.force,
    )


if __name__ == "__main__":
    main()
