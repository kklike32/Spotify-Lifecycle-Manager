"""S3 interactions for storing raw events and dashboard data."""

import json
from datetime import datetime
from typing import Optional

import boto3


class S3Client:
    """S3 client for storing raw events and dashboard artifacts."""

    def __init__(self, region_name: str = "us-east-1"):
        """Initialize S3 client.

        Args:
            region_name: AWS region
        """
        self.s3 = boto3.client("s3", region_name=region_name)

    def write_raw_events(self, bucket_name: str, date: datetime, events: list[dict]) -> None:
        """Write raw play events to S3 partitioned by date.

        Args:
            bucket_name: S3 bucket name
            date: Date for partition
            events: List of event dictionaries
        """
        key = f"dt={date.strftime('%Y-%m-%d')}/events.jsonl"

        lines = [json.dumps(event) for event in events]
        content = "\n".join(lines)

        self.s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=content.encode("utf-8"),
        )

    def write_dashboard_data(self, bucket_name: str, dashboard_json: dict) -> None:
        """Write dashboard data JSON.

        Args:
            bucket_name: S3 bucket name
            dashboard_json: Dashboard data dictionary
        """
        self.s3.put_object(
            Bucket=bucket_name,
            Key="dashboard_data.json",
            Body=json.dumps(dashboard_json, indent=2),
            ContentType="application/json",
        )

    def read_dashboard_data(self, bucket_name: str) -> Optional[dict]:
        """Read dashboard data JSON.

        Args:
            bucket_name: S3 bucket name

        Returns:
            Dashboard data or None if not found
        """
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key="dashboard_data.json")
            return json.loads(response["Body"].read().decode("utf-8"))
        except self.s3.exceptions.NoSuchKey:
            return None

    def list_raw_events(
        self, bucket_name: str, start_date: datetime, end_date: datetime
    ) -> list[str]:
        """List raw event files within date range.

        Args:
            bucket_name: S3 bucket name
            start_date: Start date
            end_date: End date

        Returns:
            List of S3 object keys
        """
        result = []
        paginator = self.s3.get_paginator("list_objects_v2")

        current = start_date
        while current <= end_date:
            prefix = f"dt={current.strftime('%Y-%m-%d')}/"
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if "Contents" in page:
                    result.extend([obj["Key"] for obj in page["Contents"]])
            current = datetime(current.year, current.month, current.day + 1)

        return result
