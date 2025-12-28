#!/bin/bash
# Build Lambda deployment package with dependencies

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$SCRIPT_DIR/.terraform/lambda_build"
OUTPUT_ZIP="$SCRIPT_DIR/.terraform/lambda_package.zip"

echo "[INFO] Building Lambda deployment package..."

# Clean up old build
rm -rf "$BUILD_DIR"
rm -f "$OUTPUT_ZIP"
mkdir -p "$BUILD_DIR"

# Install dependencies
PY_VERSION="${LAMBDA_PY_VERSION:-3.12}" # Match Lambda runtime (default python3.12)

echo "[INFO] Installing Python dependencies for Linux x86_64 (Python ${PY_VERSION})..."
cd "$PROJECT_ROOT"
python -m pip install \
  --target "$BUILD_DIR" \
  --platform manylinux2014_x86_64 \
  --python-version "${PY_VERSION}" \
  --only-binary=:all: \
  --implementation cp \
  spotipy \
  boto3 \
  pydantic \
  python-dotenv

# Copy source code (maintain package structure)
echo "[INFO] Copying source code..."
mkdir -p "$BUILD_DIR/spotify_lifecycle"
cp -r "$PROJECT_ROOT/src/spotify_lifecycle/"* "$BUILD_DIR/spotify_lifecycle/"

# Remove unnecessary files
echo "[INFO] Cleaning up..."
find "$BUILD_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR" -type f -name "*.pyc" -delete
find "$BUILD_DIR" -type f -name "*.pyo" -delete
find "$BUILD_DIR" -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR" -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
find "$BUILD_DIR" -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true

# Create zip file
echo "[INFO] Creating zip package..."
cd "$BUILD_DIR"
zip -r "$OUTPUT_ZIP" . -q

# Check size
SIZE=$(du -h "$OUTPUT_ZIP" | cut -f1)
echo "[INFO] Package created: $OUTPUT_ZIP ($SIZE)"

# Cleanup
rm -rf "$BUILD_DIR"

echo "[INFO] Lambda package build complete!"
