# Spotify Lifecycle Manager - Terraform Infrastructure

This directory contains Infrastructure as Code (IaC) for deploying the complete Spotify Lifecycle Manager to AWS.

## What Gets Deployed

**Storage:**

- 4 DynamoDB tables (play events, tracks, artists, state)
- 2 S3 buckets (cold storage + static dashboard)

**Compute:**

- 4 Lambda functions (ingest, enrich, playlist, aggregate)
- 4 EventBridge schedules (hourly, daily, weekly triggers)

**Monitoring:**

- CloudWatch Log Groups (7-day retention)
- CloudWatch Alarms (Lambda errors)
- AWS Budget alerts (optional)

**Security:**

- IAM role with least-privilege permissions
- SSM Parameter Store for secrets
- S3 bucket policies and encryption

## Prerequisites

1. **Terraform** (>= 1.5.0)

   ```bash
   brew install terraform
   ```

2. **AWS CLI** (>= 2.0)

   ```bash
   brew install awscli
   aws configure
   ```

3. **AWS Account**
   - See: `copilot/docs/cloud/ACCOUNT_SETUP.md`

4. **Spotify API Credentials**
   - See: `copilot/docs/spotify/OAUTH_SETUP.md`

## Quick Start (One-Command Deployment)

```bash
# From repository root
cd infra/terraform

# Run deployment script (interactive)
./deploy.sh
```

The script will:

1. Check prerequisites
2. Initialize Terraform
3. Validate configuration
4. Show deployment plan
5. Apply changes (after confirmation)
6. Display outputs and next steps

## Manual Deployment

If you prefer manual control:

```bash
# 1. Copy terraform.tfvars template
cp terraform.tfvars.example terraform.tfvars

# 2. Edit terraform.tfvars with your values
vim terraform.tfvars

# 3. Initialize Terraform
terraform init

# 4. Validate configuration
terraform validate

# 5. Preview changes
terraform plan

# 6. Apply changes
terraform apply
```

## Configuration

### Required Variables

Edit `terraform.tfvars`:

```hcl
# AWS Configuration
aws_region = "us-east-1"
environment = "production"

# Spotify Configuration
source_playlist_id = "your-playlist-id"
lookback_days = 7
daily_trend_days = 365

# Cost Guardrails (optional)
budget_notification_email = "you@example.com"
budget_limit_monthly = 5
```

### Secrets Management

**IMPORTANT:** Do NOT store secrets in `terraform.tfvars` or Terraform state.

After deployment, create SSM parameters manually:

```bash
# Spotify API Client ID
aws ssm put-parameter \
  --name "/spotify-lifecycle/spotify/client_id" \
  --value "YOUR_CLIENT_ID" \
  --type "SecureString" \
  --description "Spotify API client ID"

# Spotify API Client Secret
aws ssm put-parameter \
  --name "/spotify-lifecycle/spotify/client_secret" \
  --value "YOUR_CLIENT_SECRET" \
  --type "SecureString" \
  --description "Spotify API client secret"

# Spotify OAuth Refresh Token
aws ssm put-parameter \
  --name "/spotify-lifecycle/spotify/refresh_token" \
  --value "YOUR_REFRESH_TOKEN" \
  --type "SecureString" \
  --description "Spotify OAuth refresh token"
```

## Post-Deployment Steps

### 1. Upload Dashboard Files

```bash
# From repository root
aws s3 sync dashboard/site/ s3://$(terraform -chdir=infra/terraform output -raw dashboard_bucket_name)/ \
  --exclude "README.md" \
  --cache-control "max-age=3600"
```

### 2. Test Lambda Functions

```bash
# Test ingestion
aws lambda invoke \
  --function-name spotify-lifecycle-ingest \
  --log-type Tail \
  /tmp/ingest-output.json

# Test enrichment
aws lambda invoke \
  --function-name spotify-lifecycle-enrich \
  --log-type Tail \
  /tmp/enrich-output.json

# Test aggregation
aws lambda invoke \
  --function-name spotify-lifecycle-aggregate \
  --log-type Tail \
  /tmp/aggregate-output.json
```

### 3. View Dashboard

```bash
# Get dashboard URL
echo "https://$(terraform output -raw cloudfront_domain_name)"

# Open in browser
open "https://$(terraform output -raw cloudfront_domain_name)"
```

### 4. Confirm Budget Alerts (Optional)

If you provided `budget_notification_email`, check your email for AWS Budget subscription confirmation.

## Viewing Outputs

```bash
# Show all outputs
terraform output

# Show specific output
terraform output cloudfront_domain_name

# Show deployment summary (JSON)
terraform output -json deployment_summary
```

## Updating Infrastructure

```bash
# 1. Edit Terraform files or variables
vim variables.tf

# 2. Preview changes
terraform plan

# 3. Apply changes
terraform apply
```

## Destroying Infrastructure

**WARNING:** This will delete all data (DynamoDB tables, S3 buckets).

```bash
# Preview what will be destroyed
terraform plan -destroy

# Destroy all resources
terraform destroy
```

## Cost Estimates

**Expected Monthly Cost:** $0.50 - $2.00

**Free Tier (First 12 Months):**

- Lambda: 1M requests, 400,000 GB-seconds (we use ~2,000)
- DynamoDB: 25 GB storage, 25 RCU/WCU (we use <1 GB)
- S3: 5 GB storage, 20,000 GET, 2,000 PUT (we use <1 GB)
- CloudWatch: 5 GB logs, 10 alarms (we use <100 MB, 4 alarms)

**After Free Tier:**

- Storage: $0.018/month (DynamoDB) + $0.005/month (S3)
- Compute: $0.28/month (Lambda)
- Monitoring: $0.005/month (CloudWatch)

See: `copilot/docs/cost/COST_PHILOSOPHY.md` for detailed analysis.

## Troubleshooting

### Terraform Init Fails

```bash
# Clear Terraform cache
rm -rf .terraform .terraform.lock.hcl

# Re-initialize
terraform init
```

### AWS Credentials Not Found

```bash
# Configure AWS CLI
aws configure

# Verify credentials
aws sts get-caller-identity
```

### S3 Bucket Name Conflicts

S3 bucket names are globally unique. The Terraform configuration automatically appends your AWS account ID to bucket names to avoid conflicts.

If you still encounter conflicts, edit `terraform.tfvars`:

```hcl
raw_bucket_name = "my-unique-spotify-raw"
dashboard_bucket_name = "my-unique-spotify-dashboard"
```

### Lambda Deployment Package Too Large

The Terraform configuration packages all Python code from `src/`. If the package exceeds Lambda limits (50 MB compressed), consider:

1. Using Lambda Layers for dependencies
2. Excluding unnecessary files in `lambda.tf` (already configured)
3. Building deployment package separately

## File Structure

```
infra/terraform/
├── main.tf              # Provider configuration
├── variables.tf         # Input variables
├── outputs.tf           # Output values
├── dynamodb.tf          # DynamoDB tables
├── s3.tf                # S3 buckets
├── iam.tf               # IAM roles and policies
├── lambda.tf            # Lambda functions
├── eventbridge.tf       # EventBridge schedules
├── alarms.tf            # CloudWatch alarms
├── budget.tf            # AWS Budget
├── secrets.tf           # SSM Parameter Store
├── terraform.tfvars.example  # Configuration template
├── deploy.sh            # One-command deployment script
├── .gitignore           # Git ignore patterns
└── README.md            # This file
```

## Documentation

- **Architecture:** `copilot/docs/architecture/OVERVIEW.md`
- **Deployment Guide:** `copilot/docs/cloud/DEPLOYMENT.md`
- **Cost Guardrails:** `copilot/docs/cloud/COST_GUARDRAILS.md`
- **Security Model:** `copilot/docs/cloud/SECURITY_MODEL.md`

## Support

For issues or questions:

1. Check documentation: `copilot/docs/`
2. Review runbooks: `copilot/docs/runbooks/`
3. Inspect CloudWatch Logs: `aws logs tail /aws/lambda/spotify-lifecycle-ingest --follow`

## License

Personal project. Not licensed for distribution.
