# Custom Tools

This directory contains custom tools for managing and monitoring the Spotify Tracking Lifecycle project.

## Overview

**Tools Created**: 4 custom Python scripts
**Total Lines**: ~1,300 lines of code
**Purpose**: Automate common operations, monitoring, and troubleshooting

---

## Tools

### 1. `check_dashboard_health.py` - Dashboard Health Checker

Validates that the dashboard has recent data and no anomalies. Checks both CloudFront (cached) and S3 (source) versions.

**Usage:**
```bash
# Check last 7 days (default)
python scripts/check_dashboard_health.py

# Check last 30 days with verbose output
python scripts/check_dashboard_health.py --days 30 --verbose

# Quick check (last 3 days)
python scripts/check_dashboard_health.py --days 3
```

**Features:**
- Data freshness validation (alerts if >48h old)
- Missing day detection
- Zero-play day identification (potential data issues)
- Play count anomaly detection (>2000/day)
- S3 vs CloudFront consistency check
- Top tracks validation

**Example Output:**
```
=== Dashboard Health Check ===
Checking last 7 days

--- Running Health Checks ---

=== Results ===

⚠️  Warnings (2):
  • Missing 1 day(s) in last 7 days: 2026-01-05
  • Found 1 day(s) with 0 plays: 2026-01-05

Summary:
  Checked: Last 7 days
  Issues: 0
  Warnings: 2
```

**Dependencies:** `boto3`, `requests`

---

### 2. `track_costs.py` - AWS Cost Tracker

Tracks AWS costs over time using Cost Explorer API with historical storage in SQLite. Ensures the project stays under the $2/month budget.

**Usage:**
```bash
# Update costs and show report
python scripts/track_costs.py --update --report

# Show report for last 60 days
python scripts/track_costs.py --report --days 60

# Update costs for current month only
python scripts/track_costs.py --update --days 10
```

**Features:**
- Fetches costs from AWS Cost Explorer API
- Stores historical data in SQLite (`tmp/cost_tracking.db`)
- Tracks costs by service (Lambda, DynamoDB, S3, CloudWatch, etc.)
- Month-to-date cost projection
- Budget alerts ($2/month target, 80% warning threshold)
- Trend analysis

**Example Output:**
```
=== AWS Cost Report (Last 30 Days) ===

Costs by Service:
  Lambda:      $ 0.0110
  DynamoDB:    $ 0.4000
  S3/CloudFront: $ 0.0100
  CloudWatch:  $ 0.7000
  Other:       $ 0.0005
  ───────────────────────
  Total:       $ 1.1215

Month-to-Date (2026-01):
  Total Cost:  $1.1215
  Days Counted: 7
  Projected:   $4.8064

Budget Status:
  Target:      $2.00/month
  Current:     $1.1215 (56.1%)
  Projected:   $4.8064 (240.3%)
  Status:      ❌ OVER BUDGET (by $2.8064)
```

**Dependencies:** `boto3`

**Database Schema:**
- `daily_costs`: Daily cost breakdown by service
- `monthly_summaries`: Aggregated monthly costs with projections

---

### 3. `stream_logs.py` - Lambda Log Streamer

Real-time CloudWatch log streaming with color-coded output by log level. Like `tail -f` for Lambda functions.

**Usage:**
```bash
# Stream ingest logs in real-time
python scripts/stream_logs.py ingest --follow

# Show last 10 minutes of errors from aggregate
python scripts/stream_logs.py aggregate --filter ERROR --since 10m

# Follow all functions (errors only)
python scripts/stream_logs.py all --follow --filter ERROR

# Show recent backfill logs
python scripts/stream_logs.py backfill --since 1h
```

**Features:**
- Color-coded by log level (ERROR=red, WARNING=yellow, INFO=green)
- Real-time streaming (`--follow`)
- Filter by pattern (`--filter`)
- Flexible time specs (10m, 1h, 24h)
- Stream all functions in parallel
- START/END/REPORT highlighting

**Example Output:**
```
Streaming logs for ingest
Log group: /aws/lambda/spotify-lifecycle-ingest
Since: 2026-01-06 10:00:00 UTC

[10:05:23.123] START RequestId: abc123...
[10:05:24.456] INFO Fetching currently playing track
[10:05:25.789] INFO Successfully ingested 3 events
[10:05:26.012] END RequestId: abc123...
[10:05:26.034] REPORT Duration: 2890.12 ms Memory Used: 128 MB
```

**Dependencies:** `boto3`

**Supported Functions:**
- `ingest` - Hourly ingestion
- `enrich` - Hourly enrichment
- `aggregate` - Daily aggregation
- `playlist` - Weekly playlist
- `backfill` - Daily backfill
- `all` - All functions in parallel

---

### 4. `explain_terraform_plan.py` - Terraform Plan Explainer

Parses Terraform plans, explains changes in plain English, and estimates cost impact.

**Usage:**
```bash
# Generate JSON plan and explain
cd infra/terraform
terraform plan -out=tfplan.binary
terraform show -json tfplan.binary > tfplan.json
python ../../scripts/explain_terraform_plan.py --plan-file tfplan.json

# Run plan directly
cd infra/terraform
python ../../scripts/explain_terraform_plan.py --run-plan

# Verbose mode (show attribute changes)
python ../../scripts/explain_terraform_plan.py --run-plan --verbose
```

**Features:**
- Parses Terraform JSON plans
- Groups changes by action (create, update, replace, delete)
- Explains each resource in plain English
- Estimates monthly cost impact
- Identifies risky changes (deletions, replacements, force recreates)
- Budget warnings
- Color-coded output (green=create, yellow=update, red=delete)

**Example Output:**
```
=== Terraform Plan Explanation ===

Resources to Create:

  CREATE aws_cloudwatch_metric_alarm.backfill_errors
         CloudWatch alarm for monitoring will be created
         Cost impact: +$0.1000/month

  CREATE aws_lambda_function.backfill
         Serverless function will be created
         Cost impact: +$0.0030/month

=== Summary ===

Resources to create:  2
Resources to update:  0
Resources to replace: 0
Resources to delete:  0

Cost Impact:
+$0.1030/month
✓ Under budget ($1.8970 remaining)
```

**Dependencies:** None (uses standard library)

**Cost Estimates:**
- Lambda functions: $0.003/month (hourly)
- CloudWatch alarms: $0.10/month
- Most other resources: Free or usage-based

---

## Installation

All tools require Python 3.11+.

**Install dependencies:**
```bash
# For dashboard health checker and cost tracker
pip install boto3 requests

# OR use uv (project uses uv)
uv pip install boto3 requests
```

**AWS Credentials:**

Ensure AWS CLI is configured:
```bash
aws configure
# Enter credentials for us-east-1 region
```

---

## Integration with Skills

These tools complement the Claude Code skills:

| Skill | Uses Tools |
|-------|-----------|
| `/aws-deploy` | - |
| `/cost-estimate` | `track_costs.py` |
| `/logs-debug` | `stream_logs.py` |
| `/alarm-check` | - |
| `/phase-doc` | - |

The skills provide guidance on when and how to use these tools.

---

## Tool Comparison Matrix

| Tool | Real-time | Historical | Alerting | Cost Impact |
|------|-----------|------------|----------|-------------|
| **check_dashboard_health.py** | ✅ Yes | ❌ No | ⚠️ Via exit code | None |
| **track_costs.py** | ❌ No | ✅ Yes (SQLite) | ✅ Budget warnings | None |
| **stream_logs.py** | ✅ Yes | ⚠️ Limited | ❌ No | None |
| **explain_terraform_plan.py** | ✅ Yes | ❌ No | ⚠️ Risky changes | Shows estimate |

---

## Automation Examples

### Daily Cron Jobs

Add to crontab for automated monitoring:

```bash
# Daily dashboard health check (8 AM)
0 8 * * * cd ~/Spotify-Tracking-Lifecycle && python scripts/check_dashboard_health.py --days 7

# Daily cost update and report (9 AM)
0 9 * * * cd ~/Spotify-Tracking-Lifecycle && python scripts/track_costs.py --update --report

# Weekly cost summary email (Monday 10 AM)
0 10 * * 1 cd ~/Spotify-Tracking-Lifecycle && python scripts/track_costs.py --report --days 30 | mail -s "AWS Cost Report" you@example.com
```

### Pre-Deployment Checks

Before deploying infrastructure changes:

```bash
#!/bin/bash
# scripts/pre_deploy_checks.sh

echo "=== Pre-Deployment Checks ==="

# 1. Check dashboard health
echo -e "\n1. Checking dashboard health..."
python scripts/check_dashboard_health.py --days 3
if [ $? -ne 0 ]; then
    echo "⚠️  Dashboard health check failed"
fi

# 2. Review Terraform changes
echo -e "\n2. Reviewing Terraform plan..."
cd infra/terraform
python ../../scripts/explain_terraform_plan.py --run-plan

# 3. Check current costs
echo -e "\n3. Checking current costs..."
cd ../..
python scripts/track_costs.py --report --days 7

echo -e "\n=== Pre-Deployment Checks Complete ==="
```

### Post-Deployment Monitoring

After deploying, monitor logs:

```bash
#!/bin/bash
# Monitor deployment for 5 minutes

echo "Monitoring Lambda logs for errors (5 minutes)..."
timeout 300 python scripts/stream_logs.py all --follow --filter ERROR
```

---

## Troubleshooting

### `check_dashboard_health.py`

**Error: "Failed to fetch S3 data: Access Denied"**
- Check AWS credentials: `aws sts get-caller-identity`
- Verify IAM permissions for S3 read access

**Warning: "CloudFront cache is outdated"**
- Normal if recent deployment
- Cache invalidation can take 5-10 minutes
- Force invalidation: `aws cloudfront create-invalidation --distribution-id E2VJK97T08AAAS --paths "/*"`

### `track_costs.py`

**Error: "No costs fetched from AWS"**
- Cost Explorer API requires ~24h delay for data
- Check if Cost Explorer is enabled in your account
- Verify region is us-east-1

**Database locked error**
- Close other instances of the tool
- Database: `tmp/cost_tracking.db`
- Reset: `rm tmp/cost_tracking.db`

### `stream_logs.py`

**Error: "Log group not found"**
- Verify function name is correct
- Check Lambda exists: `aws lambda list-functions`
- Logs may not exist if function never ran

**Colors not showing**
- Use a TTY (not redirected output)
- Disable with `--no-color`

### `explain_terraform_plan.py`

**Error: "terraform command not found"**
- Install Terraform: https://www.terraform.io/downloads
- Verify installation: `terraform --version`

**Empty plan**
- No changes to apply
- Check Terraform working directory

---

## Contributing

When adding new tools:

1. Follow the established pattern:
   - Docstring with usage examples
   - Argparse CLI with help text
   - Exit codes (0=success, 1=failure)
   - Color-coded output with `--no-color` option
   - Verbose mode where applicable

2. Add entry to this README with:
   - Purpose and features
   - Usage examples
   - Example output
   - Dependencies

3. Make executable: `chmod +x scripts/new_tool.py`

4. Test thoroughly:
   ```bash
   # Test help
   python scripts/new_tool.py --help

   # Test error handling
   python scripts/new_tool.py --invalid-arg

   # Test normal operation
   python scripts/new_tool.py
   ```

---

## Best Practices

1. **Use tools before deployment**
   - `explain_terraform_plan.py` - Review infrastructure changes
   - `track_costs.py` - Verify budget impact

2. **Monitor regularly**
   - `check_dashboard_health.py` - Daily health checks
   - `stream_logs.py` - During deployments

3. **Track trends**
   - `track_costs.py --update` - Update cost database regularly
   - Review historical data monthly

4. **Combine tools**
   - Chain tools in scripts (see automation examples)
   - Use exit codes for conditional logic

---

## Related Files

- Skills: `.claude/skills/` - Claude Code skills that use these tools
- MCP Servers: `.mcp.json` - AWS/Playwright/Terraform MCP integration
- Deployment: `scripts/deploy.sh` - Main deployment script
- Existing tools: `scripts/backfill_missing_summaries.py`, `scripts/build_dashboard.py`

---

## Summary

**4 Custom Tools** providing:
- Dashboard health monitoring
- Cost tracking with SQLite history
- Real-time log streaming
- Terraform plan explanation

**Result**: Comprehensive toolkit for day-to-day operations, troubleshooting, and cost management of the Spotify Tracking Lifecycle infrastructure.
