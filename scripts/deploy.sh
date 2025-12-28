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
    
    # Get bucket name from Terraform output
    cd "$TERRAFORM_DIR"
    DASHBOARD_BUCKET=$(terraform output -raw dashboard_bucket_name 2>/dev/null)
    
    if [ -z "$DASHBOARD_BUCKET" ]; then
        print_error "Could not get dashboard bucket name from Terraform"
        exit 1
    fi
    
    print_info "Uploading to bucket: $DASHBOARD_BUCKET"
    
    # Upload HTML, CSS, and JS files
    aws s3 cp "$DASHBOARD_DIR/index.html" "s3://$DASHBOARD_BUCKET/" --content-type "text/html"
    aws s3 cp "$DASHBOARD_DIR/styles.css" "s3://$DASHBOARD_BUCKET/" --content-type "text/css"
    aws s3 cp "$DASHBOARD_DIR/app.js" "s3://$DASHBOARD_BUCKET/" --content-type "application/javascript"
    
    if [ $? -eq 0 ]; then
        print_success "Dashboard files uploaded successfully"
        
        # Get and display dashboard URL
        DASHBOARD_URL=$(terraform output -raw dashboard_url 2>/dev/null)
        if [ -n "$DASHBOARD_URL" ]; then
            print_info "Dashboard URL: $DASHBOARD_URL"
        fi
    else
        print_error "Dashboard upload failed"
        exit 1
    fi
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
