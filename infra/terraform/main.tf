terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # Remote state: shared across machines, versioned, encrypted.
  # No DynamoDB lock table needed - use_lockfile uses S3-native locking
  # (Terraform 1.10+), which is free vs. an always-on DynamoDB table.
  backend "s3" {
    bucket       = "spotify-lifecycle-terraform-state-kk"
    key          = "prod/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

locals {
  # Centralized default tag map (raw)
  default_tags_raw = {
    Project     = "SpotifyLifecycleManager"
    Environment = var.environment
    ManagedBy   = "Terraform"
    CostCenter  = "Personal"
  }

  # Sanitize tag keys/values to avoid invalid characters (e.g., parentheses)
  # Applies trimspace and replaces '(' and ')' with '-'
  default_tags_sanitized = {
    for k, v in local.default_tags_raw :
    trimspace(k) => replace(replace(trimspace(tostring(v)), "(", "-"), ")", "-")
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.default_tags_sanitized
  }
}
