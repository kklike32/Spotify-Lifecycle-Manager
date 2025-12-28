#!/usr/bin/env python3
"""Local runner for ingestion pipeline.

This script runs the ingestion pipeline locally for development and testing.
It:
1. Loads configuration from .env file
2. Initializes all required clients (Spotify, DynamoDB, S3)
3. Runs the ingestion pipeline
4. Prints summary statistics

Usage:
    # Run with defaults (5 pages max)
    uv run python scripts/run_ingest.py

    # Run with custom page limit
    uv run python scripts/run_ingest.py --max-pages 10

    # Dry run (no writes)
    uv run python scripts/run_ingest.py --dry-run

Requirements:
    - .env file with all required credentials
    - AWS credentials configured (via ~/.aws/credentials or env vars)
    - Spotify OAuth refresh token

For setup instructions, see:
    - copilot/docs/runbooks/LOCAL_DEV.md
    - copilot/docs/spotify/OAUTH_SETUP.md
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from spotify_lifecycle.config import load_config
from spotify_lifecycle.pipeline.ingest import run_ingestion
from spotify_lifecycle.spotify.client import SpotifyClient
from spotify_lifecycle.storage.dynamo import DynamoDBClient
from spotify_lifecycle.storage.s3 import S3ColdStore


def setup_logging(verbose: bool = False):
    """Configure logging for local execution.

    Args:
        verbose: Enable debug logging
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    """Run ingestion pipeline locally."""
    parser = argparse.ArgumentParser(description="Run Spotify ingestion pipeline locally")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum pages to fetch (default: 5)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch but don't write to storage",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    logger.info("Loading configuration...")
    config = load_config()

    # Validate required config
    if not config.spotify.client_id or not config.spotify.client_secret:
        logger.error("Missing Spotify credentials. Check .env file.")
        sys.exit(1)

    if not config.spotify.refresh_token:
        logger.error("Missing Spotify refresh token. Run OAuth setup first.")
        sys.exit(1)

    logger.info(
        "Configuration loaded",
        extra={
            "region": config.storage.region,
            "hot_table": config.storage.hot_table_name,
            "raw_bucket": config.storage.raw_bucket_name,
        },
    )

    # Initialize clients
    logger.info("Initializing Spotify client...")
    spotify_client = SpotifyClient(
        client_id=config.spotify.client_id,
        client_secret=config.spotify.client_secret,
    )
    spotify_client.authenticate(refresh_token=config.spotify.refresh_token)
    logger.info("Spotify client authenticated")

    if args.dry_run:
        logger.warning("DRY RUN MODE: No writes will be performed")
        # TODO: Implement dry-run mode with mock storage clients
        logger.error("Dry-run mode not yet implemented")
        sys.exit(1)

    logger.info("Initializing storage clients...")
    dynamo_client = DynamoDBClient(region=config.storage.region)
    s3_client = S3ColdStore(region=config.storage.region)
    logger.info("Storage clients initialized")

    # Run ingestion
    logger.info("Starting ingestion pipeline...")
    try:
        summary = run_ingestion(
            spotify_client=spotify_client,
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            state_table_name=config.storage.state_table_name,
            hot_table_name=config.storage.hot_table_name,
            raw_bucket_name=config.storage.raw_bucket_name,
            max_pages=args.max_pages,
        )

        logger.info("Ingestion complete!")
        print("\n" + "=" * 60)
        print("INGESTION SUMMARY")
        print("=" * 60)
        print(json.dumps(summary, indent=2, default=str))
        print("=" * 60 + "\n")

    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
