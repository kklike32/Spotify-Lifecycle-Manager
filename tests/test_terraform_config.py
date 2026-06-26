"""Static checks for Terraform defaults."""

import re
from pathlib import Path


def test_weekly_playlist_schedule_is_disabled_by_default():
    """Weekly playlist creation should not run unless explicitly re-enabled."""
    variables = Path("infra/terraform/variables.tf").read_text()
    eventbridge = Path("infra/terraform/eventbridge.tf").read_text()

    assert 'variable "enable_playlist_schedule"' in variables
    assert "default     = false" in variables
    assert re.search(
        r'state\s*=\s*var\.enable_playlist_schedule \? "ENABLED" : "DISABLED"',
        eventbridge,
    )
