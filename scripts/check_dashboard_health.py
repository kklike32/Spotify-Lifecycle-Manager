#!/usr/bin/env python3
"""Dashboard health check script.

Validates that the Spotify Lifecycle dashboard has recent data and no anomalies.
Checks both CloudFront (cached) and S3 (source) versions.

Usage:
    python scripts/check_dashboard_health.py [--days N] [--verbose]

Examples:
    # Check last 7 days (default)
    python scripts/check_dashboard_health.py

    # Check last 30 days with verbose output
    python scripts/check_dashboard_health.py --days 30 --verbose

    # Quick check (last 3 days)
    python scripts/check_dashboard_health.py --days 3
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    import boto3
    import requests
except ImportError:
    print("Error: Required dependencies not installed")
    print("Run: pip install boto3 requests")
    sys.exit(1)


class DashboardHealthChecker:
    """Validates dashboard data health and freshness."""

    def __init__(self, days_to_check: int = 7, verbose: bool = False):
        self.days_to_check = days_to_check
        self.verbose = verbose
        self.s3_bucket = "spotify-dashboard-kk"
        self.cloudfront_url = "https://d2vjk97t08aaas.cloudfront.net"
        self.issues: list[str] = []
        self.warnings: list[str] = []

    def log_verbose(self, message: str) -> None:
        """Print verbose log message."""
        if self.verbose:
            print(f"  {message}")

    def fetch_dashboard_data_from_s3(self) -> dict[str, Any] | None:
        """Fetch dashboard data directly from S3."""
        try:
            self.log_verbose("Fetching data from S3...")
            s3 = boto3.client("s3", region_name="us-east-1")
            response = s3.get_object(Bucket=self.s3_bucket, Key="dashboard_data.json")
            data = json.loads(response["Body"].read())
            self.log_verbose(f"✓ S3 data fetched (size: {len(json.dumps(data))} bytes)")
            return data
        except Exception as e:
            self.issues.append(f"Failed to fetch S3 data: {type(e).__name__}: {str(e)}")
            return None

    def fetch_dashboard_data_from_cloudfront(self) -> dict[str, Any] | None:
        """Fetch dashboard data from CloudFront CDN."""
        try:
            self.log_verbose("Fetching data from CloudFront...")
            url = f"{self.cloudfront_url}/dashboard_data.json"
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            # Check cache status
            cache_status = response.headers.get("x-cache", "Unknown")
            self.log_verbose(f"  Cache status: {cache_status}")

            data = response.json()
            self.log_verbose(f"✓ CloudFront data fetched (size: {len(response.content)} bytes)")
            return data
        except Exception as e:
            self.issues.append(f"Failed to fetch CloudFront data: {type(e).__name__}: {str(e)}")
            return None

    def check_data_freshness(self, data: dict[str, Any]) -> None:
        """Validate that dashboard has recent data."""
        daily_plays = data.get("daily_plays", [])
        if not daily_plays:
            self.issues.append("No daily plays data found")
            return

        # Get most recent date
        latest_play = max(daily_plays, key=lambda x: x.get("date", ""))
        latest_date_str = latest_play.get("date", "")

        try:
            latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            now = datetime.now(timezone.utc)
            age_hours = (now - latest_date).total_seconds() / 3600

            self.log_verbose(f"Latest data: {latest_date_str} ({age_hours:.1f}h ago)")

            # Data should be within 48 hours
            if age_hours > 48:
                self.issues.append(
                    f"Dashboard data is stale: {latest_date_str} " f"({age_hours:.1f} hours old)"
                )
            elif age_hours > 24:
                self.warnings.append(
                    f"Dashboard data is aging: {latest_date_str} " f"({age_hours:.1f} hours old)"
                )
            else:
                self.log_verbose(f"✓ Data is fresh ({age_hours:.1f}h old)")

        except ValueError as e:
            self.issues.append(f"Invalid date format: {latest_date_str}: {str(e)}")

    def check_missing_days(self, data: dict[str, Any]) -> None:
        """Check for missing days in the specified range."""
        daily_plays = data.get("daily_plays", [])
        if not daily_plays:
            return

        # Get dates from data
        dates_in_data = {play.get("date") for play in daily_plays if play.get("date")}

        # Generate expected dates for last N days
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=self.days_to_check - 1)

        expected_dates = set()
        current = start_date
        while current <= end_date:
            expected_dates.add(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

        # Find missing dates
        missing_dates = expected_dates - dates_in_data

        if missing_dates:
            missing_sorted = sorted(missing_dates, reverse=True)
            self.warnings.append(
                f"Missing {len(missing_dates)} day(s) in last "
                f"{self.days_to_check} days: {', '.join(missing_sorted)}"
            )
            self.log_verbose(f"  Missing dates: {missing_sorted}")
        else:
            self.log_verbose(f"✓ No missing days in last {self.days_to_check} days")

    def check_zero_play_days(self, data: dict[str, Any]) -> None:
        """Check for days with zero plays (likely data issues)."""
        daily_plays = data.get("daily_plays", [])
        if not daily_plays:
            return

        # Get recent days with zero plays
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=self.days_to_check - 1)
        start_str = start_date.strftime("%Y-%m-%d")

        zero_play_days = [
            play.get("date")
            for play in daily_plays
            if play.get("play_count", 0) == 0 and play.get("date", "") >= start_str
        ]

        if zero_play_days:
            self.warnings.append(
                f"Found {len(zero_play_days)} day(s) with 0 plays: "
                f"{', '.join(sorted(zero_play_days, reverse=True))}"
            )
            self.log_verbose(f"  Zero-play days: {zero_play_days}")
        else:
            self.log_verbose("✓ No zero-play days found")

    def check_play_count_anomalies(self, data: dict[str, Any]) -> None:
        """Check for unusually high play counts (>2000/day threshold)."""
        daily_plays = data.get("daily_plays", [])
        if not daily_plays:
            return

        # Get recent high play count days
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=self.days_to_check - 1)
        start_str = start_date.strftime("%Y-%m-%d")

        high_play_days = [
            (play.get("date"), play.get("play_count", 0))
            for play in daily_plays
            if play.get("play_count", 0) > 2000 and play.get("date", "") >= start_str
        ]

        if high_play_days:
            self.warnings.append(
                f"Found {len(high_play_days)} day(s) with >2000 plays "
                f"(anomaly threshold): {high_play_days}"
            )
            self.log_verbose(f"  High play counts: {high_play_days}")
        else:
            self.log_verbose("✓ No play count anomalies (>2000/day)")

    def check_top_tracks(self, data: dict[str, Any]) -> None:
        """Validate top tracks data exists and is reasonable."""
        top_tracks = data.get("top_tracks", [])

        if not top_tracks:
            self.warnings.append("No top tracks data found")
            return

        self.log_verbose(f"✓ Top tracks: {len(top_tracks)} tracks")

        # Check for tracks with unreasonable play counts
        for track in top_tracks[:10]:  # Check top 10
            play_count = track.get("play_count", 0)
            if play_count > 10000:
                self.warnings.append(
                    f"Unusually high play count for track "
                    f"'{track.get('track_name', 'Unknown')}': {play_count}"
                )

    def compare_s3_and_cloudfront(
        self, s3_data: dict[str, Any] | None, cf_data: dict[str, Any] | None
    ) -> None:
        """Compare S3 and CloudFront data for consistency."""
        if not s3_data or not cf_data:
            self.log_verbose("⚠ Skipping S3/CloudFront comparison (missing data)")
            return

        # Compare latest dates
        s3_daily = s3_data.get("daily_plays", [])
        cf_daily = cf_data.get("daily_plays", [])

        if s3_daily and cf_daily:
            s3_latest = max(s3_daily, key=lambda x: x.get("date", "")).get("date", "")
            cf_latest = max(cf_daily, key=lambda x: x.get("date", "")).get("date", "")

            if s3_latest != cf_latest:
                self.warnings.append(
                    f"CloudFront cache is outdated: S3={s3_latest}, " f"CloudFront={cf_latest}"
                )
                self.log_verbose(f"  S3 latest: {s3_latest}, CloudFront latest: {cf_latest}")
            else:
                self.log_verbose(f"✓ S3 and CloudFront in sync (latest: {s3_latest})")

        # Compare data sizes
        s3_size = len(json.dumps(s3_data))
        cf_size = len(json.dumps(cf_data))
        size_diff_pct = abs(s3_size - cf_size) / s3_size * 100

        if size_diff_pct > 5:
            self.warnings.append(
                f"S3 and CloudFront data size mismatch: {size_diff_pct:.1f}% difference"
            )
        else:
            self.log_verbose(f"✓ Data sizes match (within {size_diff_pct:.1f}%)")

    def run_checks(self) -> bool:
        """Run all health checks and return overall status.

        Returns:
            bool: True if healthy (no critical issues), False otherwise
        """
        print("=== Dashboard Health Check ===")
        print(f"Checking last {self.days_to_check} days\n")

        # Fetch data
        s3_data = self.fetch_dashboard_data_from_s3()
        cf_data = self.fetch_dashboard_data_from_cloudfront()

        # Use S3 data as primary source for checks
        primary_data = s3_data or cf_data

        if not primary_data:
            print("\n❌ CRITICAL: Could not fetch dashboard data from any source")
            return False

        print("\n--- Running Health Checks ---\n")

        # Run all checks on primary data
        self.check_data_freshness(primary_data)
        self.check_missing_days(primary_data)
        self.check_zero_play_days(primary_data)
        self.check_play_count_anomalies(primary_data)
        self.check_top_tracks(primary_data)

        # Compare sources if both available
        if s3_data and cf_data:
            self.compare_s3_and_cloudfront(s3_data, cf_data)

        # Print results
        print("\n=== Results ===\n")

        if self.issues:
            print(f"❌ Critical Issues ({len(self.issues)}):")
            for issue in self.issues:
                print(f"  • {issue}")
            print()

        if self.warnings:
            print(f"⚠️  Warnings ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  • {warning}")
            print()

        if not self.issues and not self.warnings:
            print("✅ Dashboard is healthy - no issues detected")

        # Summary
        print("\nSummary:")
        print(f"  Checked: Last {self.days_to_check} days")
        print(f"  Issues: {len(self.issues)}")
        print(f"  Warnings: {len(self.warnings)}")

        # Return True if no critical issues
        return len(self.issues) == 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check Spotify Lifecycle dashboard health",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check last 7 days (default)
  python scripts/check_dashboard_health.py

  # Check last 30 days with verbose output
  python scripts/check_dashboard_health.py --days 30 --verbose

  # Quick check (last 3 days)
  python scripts/check_dashboard_health.py --days 3
        """,
    )

    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to check (default: 7)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    # Run health checks
    checker = DashboardHealthChecker(days_to_check=args.days, verbose=args.verbose)
    is_healthy = checker.run_checks()

    # Exit with error code if unhealthy
    return 0 if is_healthy else 1


if __name__ == "__main__":
    sys.exit(main())
