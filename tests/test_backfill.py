"""Tests for backfill pipeline."""

from unittest.mock import MagicMock, patch

from spotify_lifecycle.pipeline.backfill import check_and_backfill_summaries


class TestCheckAndBackfillSummaries:
    """Tests for check_and_backfill_summaries function."""

    @patch("spotify_lifecycle.storage.s3.S3ColdStore")
    @patch("spotify_lifecycle.pipeline.backfill.boto3")
    def test_no_missing_summaries(self, mock_boto3, mock_s3_store_class):
        """Test when all summaries exist."""
        # Setup mocks
        mock_s3_client = MagicMock()
        mock_boto3.client.return_value = mock_s3_client

        # Mock list_objects_v2 to return events for 2 days
        mock_s3_client.list_objects_v2.return_value = {"KeyCount": 1}

        # Mock head_object to return success (summary exists)
        mock_s3_client.head_object.return_value = {}

        result = check_and_backfill_summaries(
            bucket_name="test-bucket", days_to_check=2, region_name="us-east-1"
        )

        assert result["checked_days"] == 2
        assert result["missing_days"] == 0
        assert result["backfilled"] == 0
        assert result["errors"] == 0
        assert result["missing_dates"] == []

    @patch("spotify_lifecycle.storage.s3.S3ColdStore")
    @patch("spotify_lifecycle.pipeline.backfill.boto3")
    def test_missing_summaries_backfilled(self, mock_boto3, mock_s3_store_class):
        """Test when summaries are missing and get backfilled."""
        # Setup mocks
        mock_s3_client = MagicMock()
        mock_boto3.client.return_value = mock_s3_client

        mock_store_instance = MagicMock()
        mock_s3_store_class.return_value = mock_store_instance

        # Mock list_objects_v2 to return events for 2 days
        mock_s3_client.list_objects_v2.return_value = {"KeyCount": 1}

        # Mock head_object to fail (summary doesn't exist)
        mock_s3_client.head_object.side_effect = Exception("NoSuchKey")

        # Mock write_daily_summary to succeed
        mock_store_instance.write_daily_summary.return_value = "test-key"

        result = check_and_backfill_summaries(
            bucket_name="test-bucket", days_to_check=2, region_name="us-east-1"
        )

        assert result["checked_days"] == 2
        # Should detect 3 missing (2 days + today)
        assert result["missing_days"] == 3
        assert result["backfilled"] == 3
        assert result["errors"] == 0
        assert len(result["missing_dates"]) == 3

    @patch("spotify_lifecycle.storage.s3.S3ColdStore")
    @patch("spotify_lifecycle.pipeline.backfill.boto3")
    def test_backfill_partial_failure(self, mock_boto3, mock_s3_store_class):
        """Test when some backfills fail."""
        # Setup mocks
        mock_s3_client = MagicMock()
        mock_boto3.client.return_value = mock_s3_client

        mock_store_instance = MagicMock()
        mock_s3_store_class.return_value = mock_store_instance

        # Mock list_objects_v2 to return events for 2 days
        mock_s3_client.list_objects_v2.return_value = {"KeyCount": 1}

        # Mock head_object to fail (summary doesn't exist)
        mock_s3_client.head_object.side_effect = Exception("NoSuchKey")

        # Mock write_daily_summary to fail on first call, succeed on others
        mock_store_instance.write_daily_summary.side_effect = [
            Exception("S3 Error"),
            "test-key-1",
            "test-key-2",
        ]

        result = check_and_backfill_summaries(
            bucket_name="test-bucket", days_to_check=2, region_name="us-east-1"
        )

        assert result["checked_days"] == 2
        assert result["missing_days"] == 3
        assert result["backfilled"] == 2  # 2 out of 3 succeeded
        assert result["errors"] == 1
        assert len(result["missing_dates"]) == 3

    @patch("spotify_lifecycle.storage.s3.S3ColdStore")
    @patch("spotify_lifecycle.pipeline.backfill.boto3")
    def test_no_events_found(self, mock_boto3, mock_s3_store_class):
        """Test when no events exist in the date range."""
        # Setup mocks
        mock_s3_client = MagicMock()
        mock_boto3.client.return_value = mock_s3_client

        # Mock list_objects_v2 to return no events
        mock_s3_client.list_objects_v2.return_value = {"KeyCount": 0}

        result = check_and_backfill_summaries(
            bucket_name="test-bucket", days_to_check=2, region_name="us-east-1"
        )

        assert result["checked_days"] == 2
        assert result["missing_days"] == 0
        assert result["backfilled"] == 0
        assert result["errors"] == 0
        assert result["missing_dates"] == []


class TestBackfillIntegration:
    """Integration tests for backfill functionality."""

    @patch("spotify_lifecycle.storage.s3.S3ColdStore")
    @patch("spotify_lifecycle.pipeline.backfill.boto3")
    def test_date_range_calculation(self, mock_boto3, mock_s3_store_class):
        """Test that date range is calculated correctly."""
        mock_s3_client = MagicMock()
        mock_boto3.client.return_value = mock_s3_client
        mock_s3_client.list_objects_v2.return_value = {"KeyCount": 0}

        days_to_check = 7
        check_and_backfill_summaries(
            bucket_name="test-bucket",
            days_to_check=days_to_check,
            region_name="us-east-1",
        )

        # Should check days_to_check + 1 (including today)
        assert mock_s3_client.list_objects_v2.call_count == days_to_check + 1
