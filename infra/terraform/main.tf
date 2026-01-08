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

  # Optional: Configure remote state (uncomment for production)
  # backend "s3" {
  #   bucket = "spotify-lifecycle-terraform-state"
  #   key    = "prod/terraform.tfstate"
  #   region = "us-east-1"
  # }
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
