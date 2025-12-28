# -----------------------------------------------------------------------------
# AWS Budget: Monthly Cost Limit with Alerts
# -----------------------------------------------------------------------------

resource "aws_budgets_budget" "monthly" {
  count = var.budget_notification_email != "" ? 1 : 0

  name              = "${var.project_name}-monthly-budget"
  budget_type       = "COST"
  limit_amount      = var.budget_limit_monthly
  limit_unit        = "USD"
  time_period_start = "2025-01-01_00:00"
  time_unit         = "MONTHLY"

  cost_filter {
    name = "TagKeyValue"
    values = [
      "user:Project$SpotifyLifecycleManager"
    ]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = var.budget_threshold_percent
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_notification_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_notification_email]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = var.budget_threshold_percent
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_notification_email]
  }

  depends_on = [aws_sns_topic.alarms]
}

# -----------------------------------------------------------------------------
# Cost Allocation Tags (Enable in AWS Console)
# -----------------------------------------------------------------------------

# Note: Cost allocation tags must be activated manually in AWS Billing Console
# Navigate to: Billing > Cost Allocation Tags > Activate tags
# 
# Recommended tags to activate:
# - Project
# - Environment
# - ManagedBy
# - CostCenter
