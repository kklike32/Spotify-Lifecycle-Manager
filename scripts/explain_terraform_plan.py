#!/usr/bin/env python3
"""Terraform plan explainer with cost estimates.

Parses terraform plan output, explains what each change does, and estimates
cost impact for infrastructure changes.

Usage:
    python scripts/explain_terraform_plan.py [--plan-file FILE] [--verbose]

Examples:
    # Run terraform plan and explain
    cd infra/terraform && terraform plan -out=tfplan.binary
    terraform show -json tfplan.binary > tfplan.json
    python ../../scripts/explain_terraform_plan.py --plan-file tfplan.json

    # Pipe terraform plan output
    cd infra/terraform && terraform plan -no-color | \
      python ../../scripts/explain_terraform_plan.py

    # Run plan and explain in one command
    cd infra/terraform && \
      python ../../scripts/explain_terraform_plan.py --run-plan
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

# Cost estimates per resource type (monthly)
COST_ESTIMATES = {
    "aws_lambda_function": 0.003,  # Per hourly function
    "aws_cloudwatch_metric_alarm": 0.10,  # Standard alarm
    "aws_cloudwatch_log_metric_filter": 0.00,  # Free
    "aws_cloudwatch_event_rule": 0.00,  # Free (first 14)
    "aws_s3_bucket": 0.00,  # Bucket itself is free
    "aws_dynamodb_table": 0.00,  # On-demand pricing, varies
    "aws_sns_topic": 0.00,  # Free
    "aws_sns_topic_subscription": 0.00,  # Email is free for <1000/mo
    "aws_cloudfront_distribution": 0.00,  # Within free tier
    "aws_iam_role": 0.00,  # Free
    "aws_iam_policy": 0.00,  # Free
}

# Resource type descriptions
RESOURCE_DESCRIPTIONS = {
    "aws_lambda_function": "Serverless function",
    "aws_cloudwatch_metric_alarm": "CloudWatch alarm for monitoring",
    "aws_cloudwatch_log_metric_filter": "Log pattern filter",
    "aws_cloudwatch_event_rule": "EventBridge schedule",
    "aws_cloudwatch_event_target": "EventBridge target configuration",
    "aws_lambda_permission": "Permission for service to invoke Lambda",
    "aws_s3_bucket": "S3 storage bucket",
    "aws_dynamodb_table": "DynamoDB NoSQL table",
    "aws_sns_topic": "SNS notification topic",
    "aws_sns_topic_subscription": "SNS subscription (email, etc.)",
    "aws_cloudfront_distribution": "CDN distribution",
    "aws_iam_role": "IAM role for permissions",
    "aws_iam_policy": "IAM policy document",
    "aws_iam_role_policy_attachment": "Attach policy to role",
}


class Colors:
    """ANSI color codes."""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    CYAN = "\033[36m"
    DIM = "\033[2m"

    @classmethod
    def disable(cls) -> None:
        """Disable colors."""
        cls.RESET = ""
        cls.BOLD = ""
        cls.GREEN = ""
        cls.YELLOW = ""
        cls.RED = ""
        cls.CYAN = ""
        cls.DIM = ""


class TerraformPlanExplainer:
    """Explains terraform plan changes with cost estimates."""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.total_cost_change = 0.0
        self.risky_changes: list[str] = []

    def run_terraform_plan(self, working_dir: Path) -> dict[str, Any] | None:
        """Run terraform plan and return JSON output.

        Args:
            working_dir: Directory containing terraform files

        Returns:
            Parsed JSON plan or None if failed
        """
        print(f"Running terraform plan in {working_dir}...")

        try:
            # Run terraform plan with JSON output
            result = subprocess.run(
                ["terraform", "plan", "-no-color", "-json"],
                cwd=working_dir,
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                print(f"Error running terraform plan: {result.stderr}")
                return None

            # Parse JSON lines (terraform outputs JSON stream)
            changes = []
            for line in result.stdout.splitlines():
                if line.strip():
                    try:
                        data = json.loads(line)
                        if (
                            data.get("type") == "resource_drift"
                            or data.get("type") == "planned_change"
                        ):
                            changes.append(data)
                    except json.JSONDecodeError:
                        continue

            return {"resource_changes": changes} if changes else None

        except FileNotFoundError:
            print("Error: terraform command not found")
            print("Install terraform: https://www.terraform.io/downloads")
            return None
        except Exception as e:
            print(f"Error running terraform: {type(e).__name__}: {str(e)}")
            return None

    def parse_json_plan(self, plan_file: Path) -> dict[str, Any] | None:
        """Parse terraform plan JSON file.

        Args:
            plan_file: Path to JSON plan file

        Returns:
            Parsed plan or None if failed
        """
        try:
            with open(plan_file) as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: Plan file not found: {plan_file}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return None

    def explain_action(self, action: str) -> tuple[str, str]:
        """Get explanation and color for action.

        Args:
            action: Terraform action (create, update, delete, etc.)

        Returns:
            Tuple of (explanation, color)
        """
        actions = {
            "create": ("will be created", Colors.GREEN),
            "update": ("will be updated", Colors.YELLOW),
            "delete": ("will be destroyed", Colors.RED),
            "replace": ("will be replaced", Colors.YELLOW),
            "no-op": ("no changes", Colors.DIM),
        }
        return actions.get(action, (action, Colors.RESET))

    def estimate_cost_impact(self, resource_type: str, action: str) -> float:
        """Estimate monthly cost impact.

        Args:
            resource_type: AWS resource type
            action: Terraform action

        Returns:
            Estimated monthly cost change (positive for increase)
        """
        if action == "create":
            return COST_ESTIMATES.get(resource_type, 0.0)
        elif action == "delete":
            return -COST_ESTIMATES.get(resource_type, 0.0)
        else:
            return 0.0

    def check_risky_change(
        self,
        resource_type: str,
        resource_name: str,
        action: str,
        changes: dict[str, Any] | None,
    ) -> None:
        """Check if change is risky and add warning.

        Args:
            resource_type: AWS resource type
            resource_name: Resource name
            action: Terraform action
            changes: Change details
        """
        # Destructive actions are risky
        if action in ["delete", "replace"]:
            self.risky_changes.append(f"{action.upper()}: {resource_type}.{resource_name}")

        # Force replacement is risky
        if changes and changes.get("force_new_resource"):
            self.risky_changes.append(f"FORCE REPLACEMENT: {resource_type}.{resource_name}")

        # Certain resource changes are risky
        if resource_type == "aws_dynamodb_table" and action != "create":
            self.risky_changes.append(f"DATABASE CHANGE: {resource_type}.{resource_name}")

    def explain_resource_change(self, change: dict[str, Any]) -> None:
        """Explain a single resource change.

        Args:
            change: Resource change from plan
        """
        resource_type = change.get("type", "unknown")
        resource_name = change.get("name", "unknown")
        actions = change.get("change", {}).get("actions", [])

        # Determine primary action
        if "create" in actions:
            action = "create"
        elif "delete" in actions and "create" in actions:
            action = "replace"
        elif "delete" in actions:
            action = "delete"
        elif "update" in actions:
            action = "update"
        else:
            action = "no-op"

        # Get explanation and color
        explanation, color = self.explain_action(action)

        # Get resource description
        description = RESOURCE_DESCRIPTIONS.get(resource_type, resource_type)

        # Estimate cost
        cost_change = self.estimate_cost_impact(resource_type, action)
        self.total_cost_change += cost_change

        # Check if risky
        self.check_risky_change(
            resource_type,
            resource_name,
            action,
            change.get("change"),
        )

        # Print resource change
        print(f"{color}{action.upper():>8}{Colors.RESET} ", end="")
        print(f"{Colors.BOLD}{resource_type}.{resource_name}{Colors.RESET}")
        print(f"         {description} {explanation}")

        if cost_change != 0:
            sign = "+" if cost_change > 0 else ""
            cost_str = f"{sign}${abs(cost_change):.4f}/month"
            print(f"         {Colors.DIM}Cost impact: {cost_str}{Colors.RESET}")

        # Show important attribute changes
        if action == "update" and self.verbose:
            before = change.get("change", {}).get("before", {})
            after = change.get("change", {}).get("after", {})

            if before and after:
                print(f"         {Colors.DIM}Changes:{Colors.RESET}")
                for key in after:
                    if key in before and before[key] != after[key]:
                        print(
                            f"         {Colors.DIM}  {key}: "
                            f"{before[key]} → {after[key]}{Colors.RESET}"
                        )

        print()

    def explain_plan(self, plan: dict[str, Any]) -> None:
        """Explain the entire terraform plan.

        Args:
            plan: Parsed terraform plan
        """
        resource_changes = plan.get("resource_changes", [])

        if not resource_changes:
            print("No resource changes in plan")
            return

        print(f"{Colors.BOLD}=== Terraform Plan Explanation ==={Colors.RESET}\n")

        # Group changes by action
        creates = []
        updates = []
        deletes = []
        replaces = []

        for change in resource_changes:
            actions = change.get("change", {}).get("actions", [])

            if "create" in actions and "delete" in actions:
                replaces.append(change)
            elif "create" in actions:
                creates.append(change)
            elif "delete" in actions:
                deletes.append(change)
            elif "update" in actions:
                updates.append(change)

        # Explain each category
        if creates:
            print(f"{Colors.GREEN}{Colors.BOLD}Resources to Create:{Colors.RESET}\n")
            for change in creates:
                self.explain_resource_change(change)

        if updates:
            print(f"{Colors.YELLOW}{Colors.BOLD}Resources to Update:{Colors.RESET}\n")
            for change in updates:
                self.explain_resource_change(change)

        if replaces:
            print(f"{Colors.YELLOW}{Colors.BOLD}Resources to Replace:{Colors.RESET}\n")
            for change in replaces:
                self.explain_resource_change(change)

        if deletes:
            print(f"{Colors.RED}{Colors.BOLD}Resources to Delete:{Colors.RESET}\n")
            for change in deletes:
                self.explain_resource_change(change)

        # Print summary
        print(f"{Colors.BOLD}=== Summary ==={Colors.RESET}\n")
        print(f"Resources to create:  {Colors.GREEN}{len(creates)}{Colors.RESET}")
        print(f"Resources to update:  {Colors.YELLOW}{len(updates)}{Colors.RESET}")
        print(f"Resources to replace: {Colors.YELLOW}{len(replaces)}{Colors.RESET}")
        print(f"Resources to delete:  {Colors.RED}{len(deletes)}{Colors.RESET}")
        print()

        # Cost impact
        if self.total_cost_change != 0:
            sign = "+" if self.total_cost_change > 0 else ""
            color = Colors.RED if self.total_cost_change > 0.20 else Colors.GREEN

            print(f"{Colors.BOLD}Cost Impact:{Colors.RESET}")
            print(f"{color}{sign}${abs(self.total_cost_change):.4f}/month{Colors.RESET}")

            # Budget warning
            current_cost = 1.121  # From phase 16
            new_cost = current_cost + self.total_cost_change
            budget = 2.00

            if new_cost > budget:
                print(
                    f"{Colors.RED}⚠️  WARNING: New cost (${new_cost:.4f}) "
                    f"exceeds budget (${budget:.2f})!{Colors.RESET}"
                )
            elif new_cost > budget * 0.8:
                print(
                    f"{Colors.YELLOW}⚠️  Note: New cost (${new_cost:.4f}) "
                    f"approaching budget (${budget:.2f}){Colors.RESET}"
                )
            else:
                remaining = budget - new_cost
                print(
                    f"{Colors.GREEN}✓ Under budget " f"(${remaining:.4f} remaining){Colors.RESET}"
                )
            print()

        # Risky changes
        if self.risky_changes:
            print(f"{Colors.RED}{Colors.BOLD}⚠️  Risky Changes Detected:{Colors.RESET}")
            for warning in self.risky_changes:
                print(f"{Colors.RED}  • {warning}{Colors.RESET}")
            print()
            print(f"{Colors.YELLOW}Review carefully before applying!{Colors.RESET}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Explain terraform plan with cost estimates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
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
        """,
    )

    parser.add_argument(
        "--plan-file",
        type=Path,
        help="Path to JSON plan file (from terraform show -json)",
    )
    parser.add_argument(
        "--run-plan",
        action="store_true",
        help="Run terraform plan directly",
    )
    parser.add_argument(
        "--working-dir",
        type=Path,
        default=Path("infra/terraform"),
        help="Terraform working directory (default: infra/terraform)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show detailed attribute changes",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable color output",
    )

    args = parser.parse_args()

    # Disable colors if requested or not a TTY
    if args.no_color or not sys.stdout.isatty():
        Colors.disable()

    # Initialize explainer
    explainer = TerraformPlanExplainer(verbose=args.verbose)

    # Get plan
    plan = None
    if args.run_plan:
        plan = explainer.run_terraform_plan(args.working_dir)
    elif args.plan_file:
        plan = explainer.parse_json_plan(args.plan_file)
    else:
        parser.error("Either --run-plan or --plan-file is required")

    if not plan:
        return 1

    # Explain plan
    explainer.explain_plan(plan)

    return 0


if __name__ == "__main__":
    sys.exit(main())
