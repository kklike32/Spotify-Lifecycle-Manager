#!/bin/bash
set -e

# Spotify Lifecycle Manager - Terraform Deployment Script
# This script provides a safe, one-command deployment process

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="${SCRIPT_DIR}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Terraform installation
    if ! command -v terraform &> /dev/null; then
        log_error "Terraform is not installed. Install from: https://www.terraform.io/downloads"
        exit 1
    fi
    
    # Check AWS CLI installation
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Install from: https://aws.amazon.com/cli/"
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured. Run: aws configure"
        exit 1
    fi
    
    log_info "All prerequisites satisfied"
}

check_tfvars() {
    if [ ! -f "${TERRAFORM_DIR}/terraform.tfvars" ]; then
        log_warn "terraform.tfvars not found"
        log_info "Creating from template..."
        cp "${TERRAFORM_DIR}/terraform.tfvars.example" "${TERRAFORM_DIR}/terraform.tfvars"
        log_warn "Please edit terraform.tfvars and re-run this script"
        exit 0
    fi
}

terraform_init() {
    log_info "Initializing Terraform..."
    cd "${TERRAFORM_DIR}"
    terraform init
}

terraform_validate() {
    log_info "Validating Terraform configuration..."
    cd "${TERRAFORM_DIR}"
    terraform validate
}

terraform_plan() {
    log_info "Planning Terraform deployment..."
    cd "${TERRAFORM_DIR}"
    terraform plan -out=tfplan
    
    log_warn "Review the plan above carefully"
    read -p "Continue with deployment? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        log_info "Deployment cancelled"
        rm -f tfplan
        exit 0
    fi
}

terraform_apply() {
    log_info "Applying Terraform configuration..."
    cd "${TERRAFORM_DIR}"
    terraform apply tfplan
    rm -f tfplan
    
    log_info "Deployment complete!"
}

show_outputs() {
    log_info "Deployment outputs:"
    cd "${TERRAFORM_DIR}"
    terraform output -json | jq -r '.deployment_summary.value | to_entries | .[] | "\(.key): \(.value)"'
    
    log_info ""
    log_info "Dashboard URL:"
    terraform output -raw dashboard_website_url
    echo ""
}

setup_secrets() {
    log_info ""
    log_warn "IMPORTANT: Set up Spotify API secrets in SSM Parameter Store"
    log_info "Run the following commands (replace with your values):"
    echo ""
    echo "aws ssm put-parameter \\"
    echo "  --name \"/spotify-lifecycle/spotify/client_id\" \\"
    echo "  --value \"YOUR_CLIENT_ID\" \\"
    echo "  --type \"SecureString\""
    echo ""
    echo "aws ssm put-parameter \\"
    echo "  --name \"/spotify-lifecycle/spotify/client_secret\" \\"
    echo "  --value \"YOUR_CLIENT_SECRET\" \\"
    echo "  --type \"SecureString\""
    echo ""
    echo "aws ssm put-parameter \\"
    echo "  --name \"/spotify-lifecycle/spotify/refresh_token\" \\"
    echo "  --value \"YOUR_REFRESH_TOKEN\" \\"
    echo "  --type \"SecureString\""
    echo ""
}

# Main execution
main() {
    log_info "Spotify Lifecycle Manager - Deployment"
    log_info "========================================"
    
    check_prerequisites
    check_tfvars
    terraform_init
    terraform_validate
    terraform_plan
    terraform_apply
    show_outputs
    setup_secrets
    
    log_info ""
    log_info "${GREEN}Deployment successful!${NC}"
    log_info "Next steps:"
    log_info "1. Set up Spotify API secrets (see commands above)"
    log_info "2. Upload dashboard files: make upload-dashboard"
    log_info "3. Test Lambda functions: aws lambda invoke --function-name spotify-lifecycle-ingest /tmp/output.json"
}

main
