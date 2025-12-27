# Spotify Lifecycle Manager

A serverless, event-driven Spotify Lifecycle Manager that tracks play history, creates weekly playlists, and provides long-term analytics.

## Goals

- Reliable ingestion of Spotify play history (no gaps)
- Idempotent and retry-safe processing
- Weekly playlist automation based on "not played in last N days"
- Long-term analytics via cold storage and precomputed aggregates
- Minimal operational overhead, minimal cost

## Setup

### Prerequisites

- Python 3.11+
- `uv` for environment and dependency management
- AWS Account (for DynamoDB and S3)
- Spotify API credentials

### Installation

```bash
# Create virtual environment
uv venv

# Activate (if needed for your shell)
source .venv/bin/activate

# Install dependencies
uv add spotipy boto3 pydantic python-dotenv

# Install dev dependencies
uv add --dev pytest ruff black
```

### Configuration

1. Copy `.env.example` to `.env`
2. Fill in your Spotify API credentials and AWS configuration

```bash
cp .env.example .env
```

## Development

### Running Tests

```bash
uv run pytest -q
```

### Linting and Formatting

```bash
# Check for lint issues
uv run ruff check .

# Format code
uv run black .
```

### Project Structure

```
src/spotify_lifecycle/  - Main application code
├── config.py           - Configuration management
├── models.py           - Data models
├── spotify/            - Spotify API client
├── storage/            - DynamoDB and S3 interactions
├── pipeline/           - ETL pipeline stages
└── utils/              - Utility functions

tests/                  - Unit tests
scripts/                - Utility scripts
infra/terraform/        - Infrastructure as Code
dashboard/site/         - Static dashboard
```

## Architecture

### Pipeline Stages

1. **Ingest (Recorder)** - Fetch play history from Spotify API
2. **Enrich (Librarian)** - Add metadata and features to plays
3. **Aggregate (Analyst)** - Build analytics and precomputed views
4. **Playlists (DJ)** - Create weekly curated playlists

## License

TBD
