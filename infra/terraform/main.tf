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

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "SpotifyLifecycleManager"
      Environment = var.environment
      ManagedBy   = "Terraform"
      CostCenter  = "Personal"
    }
  }
}
