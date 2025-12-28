"""AWS Lambda handler for ingestion pipeline.

This module provides the Lambda entry point for scheduled ingestion.
It's designed to be deployed as an AWS Lambda function triggered by
CloudWatch Events (EventBridge) on a schedule.

Architecture:
- Triggered by: CloudWatch Events (cron: 0 * * * * - hourly)
- Runtime: Python 3.11+
- Memory: 256 MB (sufficient for API calls)
- Timeout: 5 minutes (sufficient for 5 pages)
- Concurrency: 1 (state management prevents conflicts)

Environment variables required:
    SPOTIFY_CLIENT_ID: Spotify app client ID
    SPOTIFY_CLIENT_SECRET: Spotify app client secret
    SPOTIFY_REFRESH_TOKEN: OAuth refresh token
    HOT_TABLE_NAME: DynamoDB table for recent plays
    STATE_TABLE_NAME: DynamoDB table for state
    RAW_BUCKET_NAME: S3 bucket for cold storage
    AWS_REGION: AWS region for resources

IAM permissions required:
    - dynamodb:GetItem (state table)
    - dynamodb:PutItem (state table, hot table)
    - dynamodb:UpdateItem (state table)
    - s3:PutObject (raw bucket)

Cost implications:
- Lambda execution: ~$0.0000002 per invocation
- CloudWatch Events: Free (first 1M rules/month)
- Total: ~$0.01/month for hourly runs

For deployment instructions, see:
    - copilot/docs/cloud/DEPLOYMENT.md
    - copilot/docs/runbooks/INGESTION.md
"""

import json
import logging
import os
from typing import Any, Dict

from spotify_lifecycle.pipeline.ingest import run_ingestion
from spotify_lifecycle.spotify.client import SpotifyClient
from spotify_lifecycle.storage.dynamo import DynamoDBClient
from spotify_lifecycle.storage.s3 import S3ColdStore

# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Global clients (reused across warm invocations)
_spotify_client = None
_dynamo_client = None
_s3_client = None


def get_spotify_client() -> SpotifyClient:
    """Get or create Spotify client (cached for warm starts).

    Returns:
        Authenticated Spotify client
    """
    global _spotify_client

    if _spotify_client is None:
        client_id = os.environ["SPOTIFY_CLIENT_ID"]
        client_secret = os.environ["SPOTIFY_CLIENT_SECRET"]
        refresh_token = os.environ["SPOTIFY_REFRESH_TOKEN"]

        _spotify_client = SpotifyClient(
            client_id=client_id,
            client_secret=client_secret,
        )
        _spotify_client.authenticate(refresh_token=refresh_token)
        logger.info("spotify_client_initialized")

    return _spotify_client


def get_dynamo_client() -> DynamoDBClient:
    """Get or create DynamoDB client (cached for warm starts).

    Returns:
        DynamoDB client
    """
    global _dynamo_client

    if _dynamo_client is None:
        region = os.environ.get("AWS_REGION", "us-east-1")
        _dynamo_client = DynamoDBClient(region=region)
        logger.info("dynamo_client_initialized", extra={"region": region})

    return _dynamo_client


def get_s3_client() -> S3ColdStore:
    """Get or create S3 client (cached for warm starts).

    Returns:
        S3 cold store client
    """
    global _s3_client

    if _s3_client is None:
        region = os.environ.get("AWS_REGION", "us-east-1")
        _s3_client = S3ColdStore(region=region)
        logger.info("s3_client_initialized", extra={"region": region})

    return _s3_client


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler for ingestion pipeline.

    This function is called by AWS Lambda on CloudWatch Event trigger.
    It runs the complete ingestion pipeline and returns a summary.

    Args:
        event: CloudWatch Event (ignored)
        context: Lambda context (for logging only)

    Returns:
        Response dict with:
        - statusCode: 200 on success, 500 on error
        - body: JSON summary of ingestion

    Error handling:
        - Spotify API errors: Logged and raised (Lambda retries)
        - Storage errors: Logged and raised (Lambda retries)
        - State update errors: Logged but not raised (safe to continue)
    """
    logger.info(
        "lambda_invoked",
        extra={
            "request_id": context.request_id,
            "function_name": context.function_name,
            "event_source": event.get("source"),
        },
    )

    try:
        # Get configuration from environment
        hot_table_name = os.environ["HOT_TABLE_NAME"]
        state_table_name = os.environ["STATE_TABLE_NAME"]
        raw_bucket_name = os.environ["RAW_BUCKET_NAME"]
        max_pages = int(os.environ.get("MAX_PAGES", "5"))

        logger.info(
            "config_loaded",
            extra={
                "hot_table": hot_table_name,
                "state_table": state_table_name,
                "raw_bucket": raw_bucket_name,
                "max_pages": max_pages,
            },
        )

        # Initialize clients (cached across warm starts)
        spotify_client = get_spotify_client()
        dynamo_client = get_dynamo_client()
        s3_client = get_s3_client()

        # Run ingestion
        summary = run_ingestion(
            spotify_client=spotify_client,
            dynamo_client=dynamo_client,
            s3_client=s3_client,
            state_table_name=state_table_name,
            hot_table_name=hot_table_name,
            raw_bucket_name=raw_bucket_name,
            max_pages=max_pages,
        )

        logger.info("lambda_success", extra=summary)

        return {
            "statusCode": 200,
            "body": json.dumps(summary, default=str),
        }

    except Exception as e:
        logger.error(
            "lambda_failed",
            extra={"error": str(e), "error_type": type(e).__name__},
            exc_info=True,
        )

        # Return error response (Lambda will retry if configured)
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "error": str(e),
                    "error_type": type(e).__name__,
                }
            ),
        }
