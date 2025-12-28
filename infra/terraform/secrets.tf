# -----------------------------------------------------------------------------
# SSM Parameter Store: Spotify API Secrets
# -----------------------------------------------------------------------------

# Note: These parameters should be created manually or via AWS CLI for security
# Terraform can manage them, but storing secrets in state files is not recommended
# 
# To create parameters manually:
# 
# aws ssm put-parameter \
#   --name "/spotify-lifecycle/spotify/client_id" \
#   --value "YOUR_CLIENT_ID" \
#   --type "SecureString" \
#   --description "Spotify API client ID"
# 
# aws ssm put-parameter \
#   --name "/spotify-lifecycle/spotify/client_secret" \
#   --value "YOUR_CLIENT_SECRET" \
#   --type "SecureString" \
#   --description "Spotify API client secret"
# 
# aws ssm put-parameter \
#   --name "/spotify-lifecycle/spotify/refresh_token" \
#   --value "YOUR_REFRESH_TOKEN" \
#   --type "SecureString" \
#   --description "Spotify OAuth refresh token"

# Conditional creation: Only create if variables are provided (not recommended)
resource "aws_ssm_parameter" "spotify_client_id" {
  count = var.spotify_client_id != "" ? 1 : 0

  name        = "/${var.project_name}/spotify/client_id"
  description = "Spotify API client ID"
  type        = "SecureString"
  value       = var.spotify_client_id

  tags = {
    Name        = "${var.project_name}-spotify-client-id"
    Description = "Spotify API client ID (SecureString)"
  }
}

resource "aws_ssm_parameter" "spotify_client_secret" {
  count = var.spotify_client_secret != "" ? 1 : 0

  name        = "/${var.project_name}/spotify/client_secret"
  description = "Spotify API client secret"
  type        = "SecureString"
  value       = var.spotify_client_secret

  tags = {
    Name        = "${var.project_name}-spotify-client-secret"
    Description = "Spotify API client secret (SecureString)"
  }

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "spotify_refresh_token" {
  count = var.spotify_refresh_token != "" ? 1 : 0

  name        = "/${var.project_name}/spotify/refresh_token"
  description = "Spotify OAuth refresh token"
  type        = "SecureString"
  value       = var.spotify_refresh_token

  tags = {
    Name        = "${var.project_name}-spotify-refresh-token"
    Description = "Spotify OAuth refresh token (SecureString)"
  }

  lifecycle {
    ignore_changes = [value]
  }
}
