# Spotify Lifecycle Manager

A serverless, event-driven Spotify Lifecycle Manager that tracks play history, creates weekly playlists, and provides long-term analytics.

## Goals

- Reliable ingestion of Spotify play history (no gaps)
- Idempotent and retry-safe processing
- Weekly playlist automation based on "not played in last N days"
- Long-term analytics via cold storage and precomputed aggregates
- Minimal operational overhead, minimal cost

## Quick Start

### Prerequisites

- Python 3.11+
- `uv` for environment and dependency management
- AWS Account with configured credentials
- Spotify API credentials ([Get them here](https://developer.spotify.com/dashboard))
- Terraform 1.0+

### Local Setup

```bash
# Create virtual environment
uv venv

# Install dependencies
uv add spotipy boto3 pydantic python-dotenv
uv add --dev pytest ruff black

# Run tests
uv run pytest -q

# Lint and format
uv run ruff check .
uv run black .
```

### AWS Deployment

```bash
# 1. Configure AWS credentials
aws configure

# 2. Deploy infrastructure
cd infra/terraform
terraform init
terraform apply

# 3. Set Spotify secrets in SSM Parameter Store
aws ssm put-parameter \
    --name "/spotify-lifecycle/spotify/client_id" \
    --value "YOUR_CLIENT_ID" \
    --type "SecureString"

aws ssm put-parameter \
    --name "/spotify-lifecycle/spotify/client_secret" \
    --value "YOUR_CLIENT_SECRET" \
    --type "SecureString"

aws ssm put-parameter \
    --name "/spotify-lifecycle/spotify/refresh_token" \
    --value "YOUR_REFRESH_TOKEN" \
    --type "SecureString"

# 4. Deploy everything (Lambda + Dashboard)
./scripts/deploy.sh all
```

### Quick Deploy Script

After initial setup, use the deploy script for updates:

```bash
# Deploy everything
./scripts/deploy.sh all

# Deploy only Lambda functions
./scripts/deploy.sh lambda

# Deploy only dashboard
./scripts/deploy.sh dashboard
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
├── lambda_handler.py   - AWS Lambda entry points
├── spotify/            - Spotify API client
│   ├── client.py       - Spotipy wrapper
│   └── oauth.py        - OAuth flow
├── storage/            - DynamoDB and S3 interactions
│   ├── dynamo.py       - DynamoDB operations
│   └── s3.py           - S3 cold storage
├── pipeline/           - ETL pipeline stages
│   ├── ingest.py       - Fetch plays from Spotify
│   ├── enrich.py       - Cache track/artist metadata
│   ├── playlists.py    - Generate weekly playlists
│   └── aggregate.py    - Build dashboard analytics
└── utils/              - Utility functions
    ├── time.py         - Date/time helpers
    └── hashing.py      - Dedup key generation

tests/                  - Unit tests
scripts/
├── deploy.sh           - Main deployment script
└── hooks/              - Git hooks
infra/terraform/        - Infrastructure as Code
├── build_lambda_package.sh  - Lambda build script
├── lambda.tf           - Lambda function definitions
├── dynamodb.tf         - DynamoDB tables
└── s3.tf               - S3 buckets
dashboard/site/         - Static dashboard
├── index.html          - Dashboard UI
├── app.js              - Dashboard logic
└── styles.css          - Dashboard styling
```

## Architecture

### Pipeline Stages

1. **Ingest (Recorder)** - Fetch play history from Spotify API
   - Runs hourly via EventBridge
   - Writes to DynamoDB (hot store, 7-day TTL)
   - Appends to S3 (cold store, date-partitioned)

2. **Enrich (Librarian)** - Cache track/artist metadata
   - Runs 5 minutes after ingest
   - Cache-once strategy (no redundant API calls)
   - Stores in DynamoDB (no TTL)

3. **Aggregate (Analyst)** - Build dashboard analytics
   - Runs nightly at 2 AM
   - Precomputes all charts and stats
   - Writes single JSON to S3

4. **Playlists (DJ)** - Create weekly curated playlists
   - Runs every Monday at 8 AM
   - Set-diff: source playlist - recently played tracks
   - Creates new playlist in Spotify

### Data Flow

```
Spotify API → [Ingest] → DynamoDB (hot) + S3 (cold)
                           ↓
                        [Enrich]
                           ↓
                     DynamoDB (metadata)
                           ↓
                        [Aggregate]
                           ↓
                     S3 (dashboard.json)
                           ↓
                       [Browser]
```

### Key Design Decisions

- **Serverless-first**: No always-on servers, only event-driven Lambda
- **Idempotent**: All operations safe to retry (conditional writes)
- **TTL-based cleanup**: Hot data auto-expires (no manual cleanup)
- **Precomputed analytics**: Dashboard reads static JSON (zero queries)
- **Cross-platform builds**: Lambda packages built with manylinux wheels

## Testing

### Manual Testing

```bash
# Test ingest
aws lambda invoke --function-name spotify-lifecycle-ingest /tmp/test.json
cat /tmp/test.json | jq .

# Test enrich
aws lambda invoke --function-name spotify-lifecycle-enrich /tmp/test.json

# Test aggregate
aws lambda invoke --function-name spotify-lifecycle-aggregate /tmp/test.json

# Check DynamoDB
aws dynamodb scan --table-name spotify-play-events --select COUNT

# Check dashboard data
aws s3 cp s3://spotify-dashboard-{account-id}/dashboard_data.json - | jq .
```

### Automated Tests

```bash
# Run all tests
uv run pytest -q

# Run specific test file
uv run pytest tests/test_pipeline.py -v

# Run with coverage
uv run pytest --cov=spotify_lifecycle
```

## Troubleshooting

See [DEPLOYMENT.md](copilot/docs/runbooks/DEPLOYMENT.md) for:

- Common issues and solutions
- Debugging procedures
- Rollback instructions
- Cost monitoring

## Documentation

- [MVP.md](copilot/MVP.md) - Feature specifications
- [DEPLOYMENT.md](copilot/docs/runbooks/DEPLOYMENT.md) - Deployment guide
- [LOCAL_DEV.md](copilot/docs/runbooks/LOCAL_DEV.md) - Local development
- [OVERVIEW.md](copilot/docs/architecture/OVERVIEW.md) - System architecture

## Contributing

1. Make changes in `src/`
2. Run tests: `uv run pytest -q`
3. Format code: `uv run ruff check . --fix && uv run black .`
4. Deploy: `./scripts/deploy.sh all`

## License

TBD
