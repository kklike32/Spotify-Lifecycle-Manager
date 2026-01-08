#!/bin/bash
# Validate Terraform tag values to avoid characters that may cause CloudWatch TagResource errors.
# Specifically flags parentheses in tag values within Terraform files under infra/terraform.

set -euo pipefail

ROOT_DIR="$(dirname "$0")/.."
TF_DIR="$ROOT_DIR/infra/terraform"

errors=0

# Find .tf files and check for parentheses within lines that look like tag assignments
while IFS= read -r file; do
  # Search for lines inside tag blocks or tag maps that contain '(' or ')'
  # This is a lightweight heuristic; it may flag comments containing parentheses.
  matches=$(awk '/tags\s*=|tags\s*\{/,/}/ {print NR ":" $0}' "$file" | grep -E '\(|\)' || true)
  if [[ -n "$matches" ]]; then
    echo "Terraform tag value contains parentheses in $file:"
    echo "$matches"
    errors=1
  fi
done < <(find "$TF_DIR" -type f -name "*.tf")

if [[ "$errors" -eq 1 ]]; then
  echo "\nTag validation failed: avoid parentheses in tag values for CloudWatch resources (use '-' instead)."
  exit 1
fi

echo "Terraform tag validation passed."
