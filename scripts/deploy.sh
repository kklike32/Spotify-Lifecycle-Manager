#!/bin/bash
# Deployment script for Spotify Lifecycle Manager
# This script handles:
# 1. Building Lambda deployment package
# 2. Applying Terraform infrastructure changes
# 3. Uploading dashboard files to S3

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TERRAFORM_DIR="$PROJECT_ROOT/infra/terraform"
DASHBOARD_DIR="$PROJECT_ROOT/dashboard/site"
DASHBOARD_BUILD_DIR="$DASHBOARD_DIR/dist"
BUILD_SCRIPT="$PROJECT_ROOT/scripts/build_dashboard.py"

# Parse arguments
DEPLOY_TYPE="${1:-all}"  # all, lambda, terraform, dashboard

print_header() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}"
}

print_error() {
    echo -e "${RED}ERROR: $1${NC}"
}

print_success() {
    echo -e "${GREEN}SUCCESS: $1${NC}"
}

print_info() {
    echo -e "${YELLOW}INFO: $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check if AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed"
        exit 1
    fi
    
    # Check if Terraform is installed
    if ! command -v terraform &> /dev/null; then
        print_error "Terraform is not installed"
        exit 1
    fi
    
    # Check if uv is installed
    if ! command -v uv &> /dev/null; then
        print_error "uv is not installed"
        exit 1
    fi

    # Check if Python 3 is available (for asset build script)
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed"
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS credentials not configured"
        exit 1
    fi
    
    print_success "All prerequisites met"
}

# Build Lambda deployment package
build_lambda() {
    print_header "Building Lambda Deployment Package"
    
    cd "$TERRAFORM_DIR"
    
    if [ ! -f "build_lambda_package.sh" ]; then
        print_error "build_lambda_package.sh not found in $TERRAFORM_DIR"
        exit 1
    fi
    
    ./build_lambda_package.sh
    
    if [ $? -eq 0 ]; then
        print_success "Lambda package built successfully"
    else
        print_error "Lambda package build failed"
        exit 1
    fi
}

# Apply Terraform changes
apply_terraform() {
    print_header "Applying Terraform Infrastructure"
    
    cd "$TERRAFORM_DIR"
    
    # Initialize Terraform (safe to run multiple times)
    terraform init -upgrade
    
    # Apply changes
    terraform apply -auto-approve
    
    if [ $? -eq 0 ]; then
        print_success "Terraform applied successfully"
    else
        print_error "Terraform apply failed"
        exit 1
    fi
}

# Upload dashboard files to S3
upload_dashboard() {
    print_header "Uploading Dashboard Files to S3"

    print_info "Building dashboard assets with hashed filenames"
    if [ ! -f "$BUILD_SCRIPT" ]; then
        print_error "Build script not found: $BUILD_SCRIPT"
        exit 1
    fi
    
    python3 "$BUILD_SCRIPT"
    
    # Get bucket name from Terraform output
    cd "$TERRAFORM_DIR"
    DASHBOARD_BUCKET=$(terraform output -raw dashboard_bucket_name 2>/dev/null)
    
    if [ -z "$DASHBOARD_BUCKET" ]; then
        print_error "Could not get dashboard bucket name from Terraform"
        exit 1
    fi
    
    print_info "Uploading to bucket: $DASHBOARD_BUCKET"

    MANIFEST_PATH="$DASHBOARD_BUILD_DIR/manifest.json"
    if [ ! -f "$MANIFEST_PATH" ]; then
        print_error "manifest.json not found at $MANIFEST_PATH"
        exit 1
    fi

    HASHED_FILES=$(
        MANIFEST_PATH="$MANIFEST_PATH" python3 - <<'PY'
import json
import os
import sys

manifest_path = os.environ.get("MANIFEST_PATH")
if not manifest_path:
    sys.exit("MANIFEST_PATH not set")
with open(manifest_path) as f:
    manifest = json.load(f)
print(" ".join(manifest.values()))
PY
    )

    if [ -z "$HASHED_FILES" ]; then
        print_error "No hashed assets found in manifest"
        exit 1
    fi

    # Upload HTML and manifest with short cache
    aws s3 cp "$DASHBOARD_BUILD_DIR/index.html" "s3://$DASHBOARD_BUCKET/index.html" \
        --content-type "text/html" \
        --cache-control "public, max-age=60, must-revalidate"

    aws s3 cp "$MANIFEST_PATH" "s3://$DASHBOARD_BUCKET/manifest.json" \
        --content-type "application/json" \
        --cache-control "public, max-age=60, must-revalidate"

    # Upload hashed assets with long cache
    for file in $HASHED_FILES; do
        EXT="${file##*.}"
        case "$EXT" in
            js)
                CONTENT_TYPE="application/javascript"
                ;;
            css)
                CONTENT_TYPE="text/css"
                ;;
            *)
                CONTENT_TYPE="application/octet-stream"
                ;;
        esac

        aws s3 cp "$DASHBOARD_BUILD_DIR/$file" "s3://$DASHBOARD_BUCKET/$file" \
            --content-type "$CONTENT_TYPE" \
            --cache-control "public, max-age=31536000, immutable"
    done

    print_success "Dashboard files uploaded successfully"

    # Prune old hashed assets (keep newest 2 per type for rollback)
    prune_hashed_assets "$DASHBOARD_BUCKET" "app." 2
    prune_hashed_assets "$DASHBOARD_BUCKET" "styles." 2

    # Get and display dashboard URL
    DASHBOARD_URL=$(terraform output -raw dashboard_url 2>/dev/null)
    if [ -n "$DASHBOARD_URL" ]; then
        print_info "Dashboard URL: $DASHBOARD_URL"
    fi
}

prune_hashed_assets() {
    local bucket="$1"
    local prefix="$2"
    local keep_count="${3:-2}"

    print_info "Pruning old assets for prefix '${prefix}' (keeping ${keep_count})"

    # Fetch and sort keys by LastModified (newest first) in a macOS-safe way
    keys_json=$(
        aws s3api list-objects-v2 \
            --bucket "$bucket" \
            --prefix "$prefix" \
            --output json 2>/dev/null || true
    )

    if [ -z "$keys_json" ]; then
        print_info "No assets found for prefix '${prefix}'"
        return
    fi

    keys_list=$(
        printf '%s' "$keys_json" | python3 - <<'PY'
import json, sys
raw = sys.stdin.read().strip()
if not raw:
    sys.exit(0)
data = json.loads(raw)
items = data.get("Contents") or []
items.sort(key=lambda x: x.get("LastModified"), reverse=True)
for item in items:
    key = item.get("Key")
    # Keep only hashed assets (exclude base app.js/styles.css)
    if not key:
        continue
    # Accept patterns like app.<hash>.js or styles.<hash>.css (two dots)
    dot_count = key.count(".")
    if dot_count < 2:
        continue
    if key.startswith("app.") and key.endswith(".js"):
        print(key)
    elif key.startswith("styles.") and key.endswith(".css"):
        print(key)
PY
    )

    if [ -z "$keys_list" ]; then
        print_info "No assets found for prefix '${prefix}'"
        return
    fi

    # Read keys into array (bash 3 compatible)
    IFS=$'\n' read -r -a keys <<< "$keys_list"

    if [ "${#keys[@]}" -le "$keep_count" ]; then
        print_info "No old assets to prune for prefix '${prefix}'"
        return
    fi

    for ((i=keep_count; i<${#keys[@]}; i++)); do
        key="${keys[$i]}"
        if [ -n "$key" ]; then
            print_info "Deleting old asset: $key"
            aws s3 rm "s3://$bucket/$key"
        fi
    done
}

# Main deployment logic
main() {
    print_header "Spotify Lifecycle Manager - Deployment"
    
    check_prerequisites
    
    case "$DEPLOY_TYPE" in
        all)
            build_lambda
            apply_terraform
            upload_dashboard
            ;;
        lambda)
            build_lambda
            apply_terraform
            ;;
        terraform)
            apply_terraform
            ;;
        dashboard)
            upload_dashboard
            ;;
        *)
            print_error "Invalid deployment type: $DEPLOY_TYPE"
            echo "Usage: $0 [all|lambda|terraform|dashboard]"
            exit 1
            ;;
    esac
    
    print_header "Deployment Complete"
}

# Run main
main
