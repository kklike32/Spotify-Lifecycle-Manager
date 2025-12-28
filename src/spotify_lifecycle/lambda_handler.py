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

import boto3

from spotify_lifecycle.pipeline.aggregate import build_dashboard_data
from spotify_lifecycle.pipeline.enrich import run_enrichment
from spotify_lifecycle.pipeline.ingest import run_ingestion
from spotify_lifecycle.pipeline.playlists import create_weekly_playlist
from spotify_lifecycle.spotify.client import SpotifyClient
from spotify_lifecycle.storage.dynamo import DynamoDBClient
from spotify_lifecycle.storage.s3 import S3ColdStore, S3DashboardStore

# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Global clients (reused across warm invocations)
_spotify_client = None
_dynamo_client = None
_s3_client = None
_s3_dashboard_client = None
_ssm_client = None


def get_ssm_client():
    """Get or create SSM client (cached for warm starts)."""
    global _ssm_client
    if _ssm_client is None:
        region = os.environ.get("AWS_REGION", "us-east-1")
        _ssm_client = boto3.client("ssm", region_name=region)
    return _ssm_client


def get_secret(key: str) -> str:
    """Get secret from env var or SSM parameter store.

    If KEY exists in env, return it.
    If KEY_PARAM exists in env, fetch value from SSM.
    Otherwise raise KeyError.
    """
    # 1. Check direct env var
    if key in os.environ:
        return os.environ[key]

    # 2. Check for SSM parameter path
    param_key = f"{key}_PARAM"
    if param_key in os.environ:
        param_path = os.environ[param_key]
        ssm = get_ssm_client()
        try:
            response = ssm.get_parameter(Name=param_path, WithDecryption=True)
            return response["Parameter"]["Value"]
        except Exception as e:
            logger.error(f"Failed to fetch SSM parameter {param_path}: {e}")
            raise

    raise KeyError(f"Missing configuration: {key} or {param_key}")


def get_spotify_client() -> SpotifyClient:
    """Get or create Spotify client (cached for warm starts).

    Returns:
        Authenticated Spotify client
    """
    global _spotify_client

    if _spotify_client is None:
        client_id = get_secret("SPOTIFY_CLIENT_ID")
        client_secret = get_secret("SPOTIFY_CLIENT_SECRET")
        refresh_token = get_secret("SPOTIFY_REFRESH_TOKEN")

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
        _dynamo_client = DynamoDBClient(region_name=region)
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
        _s3_client = S3ColdStore(region_name=region)
        logger.info("s3_client_initialized", extra={"region": region})

    return _s3_client


def get_s3_dashboard_client() -> S3DashboardStore:
    """Get or create S3 dashboard store client (cached for warm starts).

    Returns:
        S3 dashboard store client
    """
    global _s3_dashboard_client

    if _s3_dashboard_client is None:
        region = os.environ.get("AWS_REGION", "us-east-1")
        _s3_dashboard_client = S3DashboardStore(region_name=region)
        logger.info("s3_dashboard_client_initialized", extra={"region": region})

    return _s3_dashboard_client


def _handle_error(e: Exception) -> Dict[str, Any]:
    """Standard error handler for all Lambda functions."""
    logger.error(
        "lambda_failed",
        extra={"error": str(e), "error_type": type(e).__name__},
        exc_info=True,
    )
    return {
        "statusCode": 500,
        "body": json.dumps({"error": str(e), "error_type": type(e).__name__}),
    }


def ingest_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler for ingestion pipeline."""
    logger.info(
        "ingest_invoked",
        extra={"request_id": context.aws_request_id, "source": event.get("source")},
    )

    try:
        hot_table_name = os.environ["HOT_TABLE_NAME"]
        state_table_name = os.environ["STATE_TABLE_NAME"]
        raw_bucket_name = os.environ["RAW_BUCKET_NAME"]
        max_pages = int(os.environ.get("MAX_PAGES", "5"))

        summary = run_ingestion(
            spotify_client=get_spotify_client(),
            dynamo_client=get_dynamo_client(),
            s3_client=get_s3_client(),
            state_table_name=state_table_name,
            hot_table_name=hot_table_name,
            raw_bucket_name=raw_bucket_name,
            max_pages=max_pages,
        )

        logger.info("ingest_success", extra=summary)
        return {"statusCode": 200, "body": json.dumps(summary, default=str)}

    except Exception as e:
        return _handle_error(e)


def enrich_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler for enrichment pipeline."""
    logger.info(
        "enrich_invoked",
        extra={"request_id": context.aws_request_id, "source": event.get("source")},
    )

    try:
        hot_table_name = os.environ["HOT_TABLE_NAME"]
        tracks_table_name = os.environ["TRACKS_TABLE_NAME"]
        artists_table_name = os.environ["ARTISTS_TABLE_NAME"]
        lookback_days = int(os.environ.get("LOOKBACK_DAYS", "7"))

        summary = run_enrichment(
            spotify_client=get_spotify_client(),
            dynamo_client=get_dynamo_client(),
            hot_table=hot_table_name,
            tracks_table=tracks_table_name,
            artists_table=artists_table_name,
            lookback_days=lookback_days,
        )

        logger.info("enrich_success", extra=summary)
        return {"statusCode": 200, "body": json.dumps(summary, default=str)}

    except Exception as e:
        return _handle_error(e)


def playlist_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler for weekly playlist generation."""
    logger.info(
        "playlist_invoked",
        extra={"request_id": context.aws_request_id, "source": event.get("source")},
    )

    try:
        hot_table_name = os.environ["HOT_TABLE_NAME"]
        state_table_name = os.environ["STATE_TABLE_NAME"]
        source_playlist_id = os.environ["SOURCE_PLAYLIST_ID"]
        user_id = os.environ.get("USER_ID", "me")
        lookback_days = int(os.environ.get("LOOKBACK_DAYS", "7"))

        summary = create_weekly_playlist(
            spotify_client=get_spotify_client(),
            dynamo_client=get_dynamo_client(),
            source_playlist_id=source_playlist_id,
            lookback_days=lookback_days,
            hot_table_name=hot_table_name,
            state_table_name=state_table_name,
            user_id=user_id,
        )

        logger.info("playlist_success", extra=summary)
        return {"statusCode": 200, "body": json.dumps(summary, default=str)}

    except Exception as e:
        return _handle_error(e)


def aggregate_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda handler for dashboard aggregation."""
    logger.info(
        "aggregate_invoked",
        extra={"request_id": context.aws_request_id, "source": event.get("source")},
    )

    try:
        hot_table_name = os.environ["HOT_TABLE_NAME"]
        tracks_table_name = os.environ["TRACKS_TABLE_NAME"]
        artists_table_name = os.environ["ARTISTS_TABLE_NAME"]
        dashboard_bucket_name = os.environ["DASHBOARD_BUCKET_NAME"]
        lookback_days = int(os.environ.get("LOOKBACK_DAYS", "90"))

        dashboard_data = build_dashboard_data(
            dynamo_client=get_dynamo_client(),
            s3_client=get_s3_dashboard_client(),
            hot_table_name=hot_table_name,
            tracks_table_name=tracks_table_name,
            artists_table_name=artists_table_name,
            dashboard_bucket_name=dashboard_bucket_name,
            lookback_days=lookback_days,
        )

        # Convert Pydantic model to dict for summary
        summary = {
            "top_tracks_count": len(dashboard_data.top_tracks),
            "top_artists_count": len(dashboard_data.top_artists),
            "generated_at": dashboard_data.generated_at.isoformat(),
        }

        logger.info("aggregate_success", extra=summary)
        return {"statusCode": 200, "body": json.dumps(summary, default=str)}

    except Exception as e:
        return _handle_error(e)
