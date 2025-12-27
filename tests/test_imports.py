"""Tests for spotify_lifecycle package."""


def test_imports():
    """Test that core modules can be imported."""
    from spotify_lifecycle import __version__

    assert __version__ == "0.1.0"

    from spotify_lifecycle.config import load_config
    from spotify_lifecycle.pipeline.ingest import compute_dedup_key

    assert callable(load_config)
    assert callable(compute_dedup_key)
