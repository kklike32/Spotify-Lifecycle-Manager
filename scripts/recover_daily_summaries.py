"""Recover daily summaries by recomputing from DynamoDB hot store.

This script is idempotent and safe to re-run. It rewrites per-day summaries
in the dashboard/raw bucket using the deduplicated hot store as the source of
truth. Use when inflated totals appear due to previous non-idempotent merges.

Usage:
    uv run python scripts/recover_daily_summaries.py
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone

from spotify_lifecycle.config import load_config
from spotify_lifecycle.storage.dynamo import DynamoDBClient
from spotify_lifecycle.storage.s3 import S3ColdStore

logger = logging.getLogger(__name__)


def _group_plays_by_date(plays: list[dict]) -> dict[datetime.date, dict[str, int]]:
    grouped: dict[datetime.date, dict[str, int]] = {}
    for play in plays:
        played_at_raw = play.get("played_at")
        track_id = play.get("track_id")
        if not played_at_raw or not track_id:
            continue
        try:
            played_at = datetime.fromisoformat(played_at_raw)
        except ValueError:
            continue
        play_date = played_at.date()
        if play_date not in grouped:
            grouped[play_date] = defaultdict(int)
        grouped[play_date][track_id] += 1
    return grouped


def recover_daily_summaries() -> None:
    cfg = load_config()
    storage = cfg.storage
    dynamo = DynamoDBClient(storage.region)
    cold_store = S3ColdStore(storage.region)

    end = datetime.now(timezone.utc)
    start = datetime(1970, 1, 1, tzinfo=timezone.utc)
    plays = dynamo.query_plays_by_date_range(
        storage.hot_table_name, start.isoformat(), end.isoformat()
    )

    grouped = _group_plays_by_date(plays)
    if not grouped:
        logger.info("no plays found to recover", extra={"table": storage.hot_table_name})
        return

    rewritten = 0
    for play_date, track_counts in sorted(grouped.items()):
        cold_store.write_daily_summary(
            storage.raw_bucket_name,
            datetime.combine(play_date, datetime.min.time()),
            track_counts,
        )  # type: ignore[arg-type]
        rewritten += 1
        logger.info(
            "rewrote daily summary",
            extra={"date": play_date.isoformat(), "total_plays": sum(track_counts.values())},
        )

    logger.info(
        "recovery complete",
        extra={
            "days_rewritten": rewritten,
            "bucket": storage.raw_bucket_name,
            "table": storage.hot_table_name,
        },
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    recover_daily_summaries()
