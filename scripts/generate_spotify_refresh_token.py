#!/usr/bin/env python3
"""Generate a Spotify OAuth refresh token for non-interactive pipeline runs."""

import argparse
import os
import sys

from dotenv import load_dotenv

from spotify_lifecycle.spotify.oauth import get_refresh_token


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate a Spotify refresh token for SPOTIFY_REFRESH_TOKEN."
    )
    parser.add_argument("--client-id", default=os.getenv("SPOTIFY_CLIENT_ID"))
    parser.add_argument("--client-secret", default=os.getenv("SPOTIFY_CLIENT_SECRET"))
    parser.add_argument(
        "--redirect-uri",
        default=os.getenv("SPOTIFY_REDIRECT_URI", "http://localhost:8888/callback"),
    )
    return parser.parse_args()


def main() -> int:
    """Run the refresh token generation flow."""
    load_dotenv()
    args = parse_args()

    if not args.client_id or not args.client_secret:
        print(
            "Missing Spotify credentials. Set SPOTIFY_CLIENT_ID and "
            "SPOTIFY_CLIENT_SECRET or pass --client-id and --client-secret.",
            file=sys.stderr,
        )
        return 1

    refresh_token = get_refresh_token(
        client_id=args.client_id,
        client_secret=args.client_secret,
        redirect_uri=args.redirect_uri,
    )
    print(refresh_token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
