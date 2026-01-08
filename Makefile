.PHONY: help fmt lint test check tf-validate-tags run-ingest run-enrich run-playlists run-aggregate dashboard-build dashboard-deploy clean

help:
	@echo "Spotify Lifecycle Manager - Development Commands"
	@echo ""
	@echo "Code Quality:"
	@echo "  make fmt       - Format code with black"
	@echo "  make lint      - Run ruff linter"
	@echo "  make test      - Run pytest"
	@echo "  make check     - Run fmt + lint + test (pre-commit workflow)"
	@echo ""
	@echo "Pipelines (local execution):"
	@echo "  make run-ingest      - Run ingestion pipeline"
	@echo "  make run-enrich      - Run enrichment pipeline"
	@echo "  make run-playlists   - Run playlist creation pipeline"
	@echo "  make run-aggregate   - Run aggregation pipeline"
	@echo ""
	@echo "Dashboard:"
	@echo "  make dashboard-build   - Build dashboard/site into dist/ with hashed assets"
	@echo "  make dashboard-deploy  - Build + deploy hashed dashboard to S3 (uses scripts/deploy.sh dashboard)"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean     - Remove temporary files and caches"

fmt:
	@echo "Formatting code with black..."
	uv run black .

lint:
	@echo "Linting code with ruff..."
	uv run ruff check .

lint-fix:
	@echo "Auto-fixing lint issues with ruff..."
	uv run ruff check . --fix

test:
	@echo "Running tests with pytest..."
	uv run pytest -q

test-verbose:
	@echo "Running tests (verbose)..."
	uv run pytest -v

check: fmt lint test
	@echo "All checks passed!"

tf-validate-tags:
	@echo "Validating Terraform tag values..."
	./scripts/validate_tf_tags.sh

run-ingest:
	@echo "Running ingest pipeline..."
	uv run python -m spotify_lifecycle.pipeline.ingest

run-enrich:
	@echo "Running enrich pipeline..."
	uv run python -m spotify_lifecycle.pipeline.enrich

run-playlists:
	@echo "Running playlist pipeline..."
	uv run python -m spotify_lifecycle.pipeline.playlists

run-aggregate:
	@echo "Running aggregate pipeline..."
	uv run python -m spotify_lifecycle.pipeline.aggregate

dashboard-build:
	@echo "Building dashboard with hashed assets..."
	python scripts/build_dashboard.py

dashboard-deploy:
	@echo "Building and deploying dashboard..."
	./scripts/deploy.sh dashboard

run-all:
	@echo "Running all pipelines sequentially..."
	./scripts/deploy.sh all

clean:
	@echo "Cleaning temporary files and caches..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf tmp/*.py 2>/dev/null || true
	@echo "Clean complete!"
