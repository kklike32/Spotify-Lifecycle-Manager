#!/usr/bin/env python3
"""Real-time Lambda log streamer with color-coded output.

Streams CloudWatch logs for Spotify Lifecycle Lambda functions with color-coding
by log level and filtering capabilities. Similar to 'tail -f' for AWS Lambda.

Usage:
    python scripts/stream_logs.py FUNCTION [--follow] [--filter PATTERN] [--since TIME]

Examples:
    # Stream ingest logs in real-time
    python scripts/stream_logs.py ingest --follow

    # Show last 10 minutes of errors from aggregate
    python scripts/stream_logs.py aggregate --filter ERROR --since 10m

    # Follow all functions (errors only)
    python scripts/stream_logs.py all --follow --filter ERROR

    # Show recent backfill logs
    python scripts/stream_logs.py backfill --since 1h
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    import boto3
except ImportError:
    print("Error: boto3 not installed")
    print("Run: pip install boto3")
    sys.exit(1)


class Colors:
    """ANSI color codes for terminal output."""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    # Foreground colors
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    # Background colors
    BG_RED = "\033[41m"
    BG_YELLOW = "\033[43m"

    @classmethod
    def disable(cls) -> None:
        """Disable colors (for non-TTY output)."""
        cls.RESET = ""
        cls.BOLD = ""
        cls.DIM = ""
        cls.BLACK = ""
        cls.RED = ""
        cls.GREEN = ""
        cls.YELLOW = ""
        cls.BLUE = ""
        cls.MAGENTA = ""
        cls.CYAN = ""
        cls.WHITE = ""
        cls.BG_RED = ""
        cls.BG_YELLOW = ""


class LogStreamer:
    """Streams and formats CloudWatch logs for Lambda functions."""

    LAMBDA_FUNCTIONS = {
        "ingest": "spotify-lifecycle-ingest",
        "enrich": "spotify-lifecycle-enrich",
        "aggregate": "spotify-lifecycle-aggregate",
        "playlist": "spotify-lifecycle-playlist",
        "backfill": "spotify-lifecycle-backfill",
    }

    def __init__(self, function: str, region: str = "us-east-1"):
        """Initialize log streamer.

        Args:
            function: Function name (short name like 'ingest' or full name)
            region: AWS region
        """
        # Map short name to full function name
        if function in self.LAMBDA_FUNCTIONS:
            self.function_name = self.LAMBDA_FUNCTIONS[function]
            self.short_name = function
        else:
            self.function_name = function
            self.short_name = function.replace("spotify-lifecycle-", "")

        self.log_group = f"/aws/lambda/{self.function_name}"
        self.logs_client = boto3.client("logs", region_name=region)

    def parse_time_spec(self, time_spec: str) -> datetime:
        """Parse time specification into datetime.

        Args:
            time_spec: Time specification like '10m', '1h', '24h', or ISO timestamp

        Returns:
            datetime object
        """
        # Try parsing as relative time (e.g., '10m', '1h')
        match = re.match(r"^(\d+)([smhd])$", time_spec)
        if match:
            value = int(match.group(1))
            unit = match.group(2)

            delta_kwargs = {
                "s": {"seconds": value},
                "m": {"minutes": value},
                "h": {"hours": value},
                "d": {"days": value},
            }

            return datetime.now(timezone.utc) - timedelta(**delta_kwargs[unit])

        # Try parsing as ISO timestamp
        try:
            return datetime.fromisoformat(time_spec.replace("Z", "+00:00"))
        except ValueError:
            pass

        # Default: 10 minutes ago
        print(
            f"Warning: Could not parse time '{time_spec}', using 10 minutes ago",
            file=sys.stderr,
        )
        return datetime.now(timezone.utc) - timedelta(minutes=10)

    def colorize_log_level(self, message: str) -> str:
        """Add color coding based on log level.

        Args:
            message: Log message

        Returns:
            Colorized message
        """
        # ERROR - Red
        if "ERROR" in message or "Error" in message or "error:" in message:
            return f"{Colors.RED}{message}{Colors.RESET}"

        # WARNING - Yellow
        if "WARNING" in message or "Warning" in message or "WARN" in message:
            return f"{Colors.YELLOW}{message}{Colors.RESET}"

        # INFO - Green
        if "INFO" in message:
            return f"{Colors.GREEN}{message}{Colors.RESET}"

        # DEBUG - Dim
        if "DEBUG" in message:
            return f"{Colors.DIM}{message}{Colors.RESET}"

        # START/END/REPORT - Cyan
        if message.startswith("START") or message.startswith("END") or message.startswith("REPORT"):
            return f"{Colors.CYAN}{message}{Colors.RESET}"

        # Default - no color
        return message

    def format_log_event(self, event: dict[str, Any], show_timestamp: bool = True) -> str:
        """Format a log event for display.

        Args:
            event: CloudWatch log event
            show_timestamp: Whether to show timestamp

        Returns:
            Formatted log line
        """
        timestamp = event["timestamp"]
        message = event["message"].rstrip()

        # Format timestamp
        dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        time_str = dt.strftime("%H:%M:%S.%f")[:-3]  # HH:MM:SS.mmm

        # Colorize message
        colored_message = self.colorize_log_level(message)

        # Format output
        if show_timestamp:
            return f"{Colors.DIM}[{time_str}]{Colors.RESET} {colored_message}"
        else:
            return colored_message

    def stream_logs(
        self,
        since: str = "10m",
        follow: bool = False,
        filter_pattern: str | None = None,
    ) -> None:
        """Stream logs from CloudWatch.

        Args:
            since: How far back to start streaming (e.g., '10m', '1h')
            follow: Whether to follow logs in real-time
            filter_pattern: Optional filter pattern (substring match)
        """
        start_time = self.parse_time_spec(since)
        start_ms = int(start_time.timestamp() * 1000)

        print(
            f"{Colors.BOLD}Streaming logs for {self.short_name}{Colors.RESET}",
            file=sys.stderr,
        )
        print(
            f"{Colors.DIM}Log group: {self.log_group}{Colors.RESET}",
            file=sys.stderr,
        )
        print(
            f"{Colors.DIM}Since: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}{Colors.RESET}",
            file=sys.stderr,
        )
        if filter_pattern:
            print(
                f"{Colors.DIM}Filter: {filter_pattern}{Colors.RESET}",
                file=sys.stderr,
            )
        print(file=sys.stderr)

        seen_event_ids = set()
        next_token = None

        try:
            while True:
                # Build filter params
                filter_params: dict[str, Any] = {
                    "logGroupName": self.log_group,
                    "startTime": start_ms,
                    "interleaved": True,
                }

                if filter_pattern:
                    filter_params["filterPattern"] = filter_pattern

                if next_token:
                    filter_params["nextToken"] = next_token

                # Fetch events
                try:
                    response = self.logs_client.filter_log_events(**filter_params)
                except self.logs_client.exceptions.ResourceNotFoundException:
                    print(
                        f"{Colors.RED}Error: Log group not found: {self.log_group}{Colors.RESET}",
                        file=sys.stderr,
                    )
                    return
                except Exception as e:
                    error_msg = f"{type(e).__name__}: {str(e)}"
                    print(
                        f"{Colors.RED}Error fetching logs: {error_msg}{Colors.RESET}",
                        file=sys.stderr,
                    )
                    return

                # Process events
                events = response.get("events", [])
                new_events = 0

                for event in events:
                    event_id = event["eventId"]
                    if event_id not in seen_event_ids:
                        seen_event_ids.add(event_id)
                        print(self.format_log_event(event))
                        sys.stdout.flush()
                        new_events += 1

                # Update pagination token
                next_token = response.get("nextToken")

                # If following, continue polling
                if follow:
                    # If we got events, continue immediately
                    # Otherwise, wait a bit before next poll
                    if new_events == 0:
                        time.sleep(1)

                    # Update start time to recent events
                    if events:
                        latest_timestamp = max(e["timestamp"] for e in events)
                        start_ms = latest_timestamp
                else:
                    # Not following - exit if no more events
                    if not next_token:
                        break

        except KeyboardInterrupt:
            print(
                f"\n{Colors.DIM}Stream interrupted{Colors.RESET}",
                file=sys.stderr,
            )


def stream_all_functions(
    since: str = "10m",
    follow: bool = False,
    filter_pattern: str | None = None,
) -> None:
    """Stream logs from all Lambda functions simultaneously.

    Args:
        since: How far back to start streaming
        follow: Whether to follow logs in real-time
        filter_pattern: Optional filter pattern
    """
    import threading

    print(
        f"{Colors.BOLD}Streaming logs from all functions{Colors.RESET}",
        file=sys.stderr,
    )
    print(file=sys.stderr)

    # Create a streamer for each function
    streamers = [LogStreamer(func) for func in LogStreamer.LAMBDA_FUNCTIONS.keys()]

    def stream_function(streamer: LogStreamer) -> None:
        """Stream logs for a single function."""
        streamer.stream_logs(since=since, follow=follow, filter_pattern=filter_pattern)

    # Start threads for each function
    threads = []
    for streamer in streamers:
        thread = threading.Thread(target=stream_function, args=(streamer,))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    try:
        # Wait for all threads
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print(
            f"\n{Colors.DIM}Stream interrupted{Colors.RESET}",
            file=sys.stderr,
        )


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Stream Lambda logs with color-coded output",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Functions:
  ingest      - Hourly ingestion Lambda
  enrich      - Hourly enrichment Lambda
  aggregate   - Daily aggregation Lambda
  playlist    - Weekly playlist Lambda
  backfill    - Daily backfill Lambda
  all         - All functions (parallel streaming)

Examples:
  # Stream recent ingest logs
  python scripts/stream_logs.py ingest

  # Follow ingest logs in real-time
  python scripts/stream_logs.py ingest --follow

  # Show errors from last hour
  python scripts/stream_logs.py aggregate --filter ERROR --since 1h

  # Follow all functions (errors only)
  python scripts/stream_logs.py all --follow --filter ERROR
        """,
    )

    parser.add_argument(
        "function",
        choices=list(LogStreamer.LAMBDA_FUNCTIONS.keys()) + ["all"],
        help="Lambda function to stream logs from",
    )
    parser.add_argument(
        "--follow",
        "-f",
        action="store_true",
        help="Follow logs in real-time (like tail -f)",
    )
    parser.add_argument(
        "--filter",
        type=str,
        default=None,
        help="Filter pattern (substring match)",
    )
    parser.add_argument(
        "--since",
        type=str,
        default="10m",
        help="How far back to stream (e.g., 10m, 1h, 24h) (default: 10m)",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable color output",
    )

    args = parser.parse_args()

    # Disable colors if requested or not a TTY
    if args.no_color or not sys.stdout.isatty():
        Colors.disable()

    # Stream logs
    if args.function == "all":
        stream_all_functions(
            since=args.since,
            follow=args.follow,
            filter_pattern=args.filter,
        )
    else:
        streamer = LogStreamer(args.function)
        streamer.stream_logs(
            since=args.since,
            follow=args.follow,
            filter_pattern=args.filter,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
