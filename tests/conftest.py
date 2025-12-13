"""Shared fixtures for fastmarc tests."""

import pytest
from pathlib import Path


@pytest.fixture(scope="session")
def marc_file():
    """Path to the test MARC file."""
    path = Path(__file__).parent.parent / "CUY.UCB_serials_test_combined.mrc"
    if not path.exists():
        pytest.skip(f"Test MARC file not found: {path}")
    return str(path)


@pytest.fixture
def reader(marc_file):
    """Create a MARCReader instance."""
    from fastmarc import MARCReader
    with open(marc_file, "rb") as f:
        yield MARCReader(f)
