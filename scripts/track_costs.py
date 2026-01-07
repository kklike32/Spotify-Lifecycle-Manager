#!/usr/bin/env python3
"""AWS cost tracker with historical storage.

Tracks AWS costs over time using Cost Explorer API and stores them in a local
SQLite database for trending analysis. Helps ensure the project stays under
the $2/month budget.

Usage:
    python scripts/track_costs.py [--update] [--report] [--days N]

Examples:
    # Update costs for current month
    python scripts/track_costs.py --update

    # Show cost report for last 30 days
    python scripts/track_costs.py --report --days 30

    # Update and show report
    python scripts/track_costs.py --update --report

    # Show month-to-date costs
    python scripts/track_costs.py --report
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import boto3
except ImportError:
    print("Error: boto3 not installed")
    print("Run: pip install boto3")
    sys.exit(1)


class CostTracker:
    """Tracks and analyzes AWS costs over time."""

    BUDGET_TARGET = 2.00  # Monthly budget target in USD
    BUDGET_WARNING = 1.60  # 80% of budget

    def __init__(self, db_path: Path | None = None):
        """Initialize cost tracker.

        Args:
            db_path: Path to SQLite database file (default: tmp/cost_tracking.db)
        """
        if db_path is None:
            db_path = Path("tmp/cost_tracking.db")

        # Ensure tmp directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_database()

        # AWS Cost Explorer client
        self.ce_client = boto3.client("ce", region_name="us-east-1")

    def _init_database(self) -> None:
        """Create database tables if they don't exist."""
        cursor = self.conn.cursor()

        # Daily costs table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_costs (
                date TEXT PRIMARY KEY,
                total_cost REAL NOT NULL,
                lambda_cost REAL DEFAULT 0,
                dynamodb_cost REAL DEFAULT 0,
                s3_cost REAL DEFAULT 0,
                cloudwatch_cost REAL DEFAULT 0,
                other_cost REAL DEFAULT 0,
                recorded_at TEXT NOT NULL
            )
        """
        )

        # Monthly summaries table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_summaries (
                year_month TEXT PRIMARY KEY,
                total_cost REAL NOT NULL,
                lambda_cost REAL DEFAULT 0,
                dynamodb_cost REAL DEFAULT 0,
                s3_cost REAL DEFAULT 0,
                cloudwatch_cost REAL DEFAULT 0,
                other_cost REAL DEFAULT 0,
                days_counted INTEGER NOT NULL,
                recorded_at TEXT NOT NULL
            )
        """
        )

        self.conn.commit()

    def fetch_costs_from_aws(
        self, start_date: datetime, end_date: datetime
    ) -> dict[str, dict[str, float]]:
        """Fetch costs from AWS Cost Explorer.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (exclusive)

        Returns:
            Dictionary mapping dates to service costs
        """
        print(f"Fetching costs from {start_date.date()} to {end_date.date()} from AWS...")

        try:
            response = self.ce_client.get_cost_and_usage(
                TimePeriod={
                    "Start": start_date.strftime("%Y-%m-%d"),
                    "End": end_date.strftime("%Y-%m-%d"),
                },
                Granularity="DAILY",
                Metrics=["BlendedCost"],
                GroupBy=[{"Type": "SERVICE", "Key": "SERVICE"}],
            )

            costs_by_date: dict[str, dict[str, float]] = {}

            for result in response.get("ResultsByTime", []):
                date_str = result["TimePeriod"]["Start"]
                costs_by_date[date_str] = {}

                total = 0.0
                for group in result.get("Groups", []):
                    service = group["Keys"][0]
                    amount = float(group["Metrics"]["BlendedCost"]["Amount"])

                    # Map to our service categories
                    if "Lambda" in service:
                        costs_by_date[date_str]["lambda"] = (
                            costs_by_date[date_str].get("lambda", 0.0) + amount
                        )
                    elif "DynamoDB" in service:
                        costs_by_date[date_str]["dynamodb"] = (
                            costs_by_date[date_str].get("dynamodb", 0.0) + amount
                        )
                    elif "S3" in service or "CloudFront" in service:
                        costs_by_date[date_str]["s3"] = (
                            costs_by_date[date_str].get("s3", 0.0) + amount
                        )
                    elif "CloudWatch" in service:
                        costs_by_date[date_str]["cloudwatch"] = (
                            costs_by_date[date_str].get("cloudwatch", 0.0) + amount
                        )
                    else:
                        costs_by_date[date_str]["other"] = (
                            costs_by_date[date_str].get("other", 0.0) + amount
                        )

                    total += amount

                costs_by_date[date_str]["total"] = total

            print(f"  Fetched costs for {len(costs_by_date)} days")
            return costs_by_date

        except Exception as e:
            print(f"Error fetching costs from AWS: {type(e).__name__}: {str(e)}")
            return {}

    def store_daily_costs(self, costs_by_date: dict[str, dict[str, float]]) -> int:
        """Store daily costs in database.

        Args:
            costs_by_date: Dictionary mapping dates to service costs

        Returns:
            Number of records inserted/updated
        """
        cursor = self.conn.cursor()
        recorded_at = datetime.now(timezone.utc).isoformat()
        count = 0

        for date_str, costs in costs_by_date.items():
            cursor.execute(
                """
                INSERT OR REPLACE INTO daily_costs
                (date, total_cost, lambda_cost, dynamodb_cost, s3_cost,
                 cloudwatch_cost, other_cost, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    date_str,
                    costs.get("total", 0.0),
                    costs.get("lambda", 0.0),
                    costs.get("dynamodb", 0.0),
                    costs.get("s3", 0.0),
                    costs.get("cloudwatch", 0.0),
                    costs.get("other", 0.0),
                    recorded_at,
                ),
            )
            count += 1

        self.conn.commit()
        return count

    def update_monthly_summary(self, year_month: str) -> None:
        """Update monthly summary from daily costs.

        Args:
            year_month: Month in YYYY-MM format
        """
        cursor = self.conn.cursor()

        # Aggregate daily costs for the month
        cursor.execute(
            """
            SELECT
                COUNT(*) as days_counted,
                SUM(total_cost) as total_cost,
                SUM(lambda_cost) as lambda_cost,
                SUM(dynamodb_cost) as dynamodb_cost,
                SUM(s3_cost) as s3_cost,
                SUM(cloudwatch_cost) as cloudwatch_cost,
                SUM(other_cost) as other_cost
            FROM daily_costs
            WHERE date LIKE ?
        """,
            (f"{year_month}%",),
        )

        row = cursor.fetchone()
        if not row or row["days_counted"] == 0:
            return

        recorded_at = datetime.now(timezone.utc).isoformat()

        cursor.execute(
            """
            INSERT OR REPLACE INTO monthly_summaries
            (year_month, total_cost, lambda_cost, dynamodb_cost, s3_cost,
             cloudwatch_cost, other_cost, days_counted, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                year_month,
                row["total_cost"] or 0.0,
                row["lambda_cost"] or 0.0,
                row["dynamodb_cost"] or 0.0,
                row["s3_cost"] or 0.0,
                row["cloudwatch_cost"] or 0.0,
                row["other_cost"] or 0.0,
                row["days_counted"],
                recorded_at,
            ),
        )

        self.conn.commit()

    def update_costs(self, days: int = 30) -> None:
        """Update costs for the last N days.

        Args:
            days: Number of days to update (default: 30)
        """
        end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=days - 1)

        # Fetch and store costs
        costs = self.fetch_costs_from_aws(start_date, end_date + timedelta(days=1))
        if costs:
            count = self.store_daily_costs(costs)
            print(f"✓ Stored {count} daily cost records")

            # Update monthly summaries for affected months
            months = set()
            for date_str in costs.keys():
                year_month = date_str[:7]  # YYYY-MM
                months.add(year_month)

            for month in months:
                self.update_monthly_summary(month)

            print(f"✓ Updated {len(months)} monthly summaries")
        else:
            print("⚠ No costs fetched from AWS")

    def get_daily_costs(self, days: int = 30) -> list[dict[str, Any]]:
        """Get daily costs for the last N days.

        Args:
            days: Number of days to retrieve

        Returns:
            List of daily cost records
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT * FROM daily_costs
            ORDER BY date DESC
            LIMIT ?
        """,
            (days,),
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_monthly_summary(self, year_month: str | None = None) -> dict[str, Any]:
        """Get monthly summary.

        Args:
            year_month: Month in YYYY-MM format (default: current month)

        Returns:
            Monthly summary record
        """
        if year_month is None:
            year_month = datetime.now(timezone.utc).strftime("%Y-%m")

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT * FROM monthly_summaries
            WHERE year_month = ?
        """,
            (year_month,),
        )

        row = cursor.fetchone()
        return dict(row) if row else {}

    def print_report(self, days: int = 30) -> None:
        """Print cost report for the last N days.

        Args:
            days: Number of days to include in report
        """
        print(f"\n=== AWS Cost Report (Last {days} Days) ===\n")

        # Get daily costs
        daily_costs = self.get_daily_costs(days)

        if not daily_costs:
            print("No cost data available. Run with --update to fetch costs.")
            return

        # Calculate totals
        total = sum(d["total_cost"] for d in daily_costs)
        lambda_total = sum(d["lambda_cost"] for d in daily_costs)
        dynamodb_total = sum(d["dynamodb_cost"] for d in daily_costs)
        s3_total = sum(d["s3_cost"] for d in daily_costs)
        cloudwatch_total = sum(d["cloudwatch_cost"] for d in daily_costs)
        other_total = sum(d["other_cost"] for d in daily_costs)

        # Print summary by service
        print("Costs by Service:")
        print(f"  Lambda:      ${lambda_total:>7.4f}")
        print(f"  DynamoDB:    ${dynamodb_total:>7.4f}")
        print(f"  S3/CloudFront: ${s3_total:>7.4f}")
        print(f"  CloudWatch:  ${cloudwatch_total:>7.4f}")
        print(f"  Other:       ${other_total:>7.4f}")
        print(f"  {'─' * 23}")
        print(f"  Total:       ${total:>7.4f}\n")

        # Get current month summary
        current_month = datetime.now(timezone.utc).strftime("%Y-%m")
        monthly = self.get_monthly_summary(current_month)

        if monthly:
            print(f"Month-to-Date ({current_month}):")
            print(f"  Total Cost:  ${monthly['total_cost']:.4f}")
            print(f"  Days Counted: {monthly['days_counted']}")

            # Project monthly cost
            days_in_month = (
                datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                + timedelta(days=32)
            ).replace(day=1) - timedelta(days=1)
            total_days = days_in_month.day
            current_day = datetime.now(timezone.utc).day

            if monthly["days_counted"] > 0 and current_day > 0:
                daily_avg = monthly["total_cost"] / monthly["days_counted"]
                projected = daily_avg * total_days

                print(f"  Projected:   ${projected:.4f}")

                # Budget status
                budget_used_pct = (monthly["total_cost"] / self.BUDGET_TARGET) * 100
                projected_pct = (projected / self.BUDGET_TARGET) * 100

                print("\nBudget Status:")
                print(f"  Target:      ${self.BUDGET_TARGET:.2f}/month")
                print(f"  Current:     ${monthly['total_cost']:.4f} ({budget_used_pct:.1f}%)")
                print(f"  Projected:   ${projected:.4f} ({projected_pct:.1f}%)")

                # Status indicator
                if projected > self.BUDGET_TARGET:
                    print(
                        f"  Status:      ❌ OVER BUDGET (by ${projected - self.BUDGET_TARGET:.4f})"
                    )
                elif projected > self.BUDGET_WARNING:
                    print("  Status:      ⚠️  WARNING (approaching limit)")
                else:
                    remaining = self.BUDGET_TARGET - projected
                    print(f"  Status:      ✅ UNDER BUDGET (${remaining:.4f} remaining)")

        # Show recent daily trends
        print("\nRecent Daily Costs:")
        print(f"  {'Date':<12} {'Total':>8} {'Lambda':>8} {'DynamoDB':>8} {'CloudWatch':>8}")
        print(f"  {'-' * 60}")

        for record in daily_costs[:10]:
            print(
                f"  {record['date']:<12} "
                f"${record['total_cost']:>7.4f} "
                f"${record['lambda_cost']:>7.4f} "
                f"${record['dynamodb_cost']:>7.4f} "
                f"${record['cloudwatch_cost']:>7.4f}"
            )

    def close(self) -> None:
        """Close database connection."""
        self.conn.close()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Track AWS costs over time",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update costs and show report
  python scripts/track_costs.py --update --report

  # Show report for last 60 days
  python scripts/track_costs.py --report --days 60

  # Update costs for current month only
  python scripts/track_costs.py --update --days 10
        """,
    )

    parser.add_argument(
        "--update",
        action="store_true",
        help="Fetch and update costs from AWS",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Show cost report",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to process (default: 30)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Path to SQLite database (default: tmp/cost_tracking.db)",
    )

    args = parser.parse_args()

    # At least one action required
    if not args.update and not args.report:
        parser.error("At least one of --update or --report is required")

    # Initialize tracker
    tracker = CostTracker(db_path=args.db)

    try:
        # Update costs if requested
        if args.update:
            tracker.update_costs(days=args.days)

        # Show report if requested
        if args.report:
            tracker.print_report(days=args.days)

    finally:
        tracker.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
