"""Hashing utilities."""

import hashlib


def sha256_hash(data: str) -> str:
    """Compute SHA256 hash of a string.

    Args:
        data: String to hash

    Returns:
        Hex digest
    """
    return hashlib.sha256(data.encode()).hexdigest()
