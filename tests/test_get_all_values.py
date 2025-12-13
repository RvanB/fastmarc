"""Test get_all_values() functionality."""

import pytest
from fastmarc import MARCReader


class TestGetAllValues:
    """Test get_all_values method."""

    def test_get_all_values_control_field(self, marc_file):
        """Test get_all_values with control field (001)."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f).build_index()

            all_ids = reader.get_all_values("001")
            assert len(all_ids) == len(reader)
            # Each record should have a list
            assert all(isinstance(ids, list) for ids in all_ids)

    def test_get_all_values_data_field(self, marc_file):
        """Test get_all_values with data field (245$a)."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f).build_index()

            all_titles = reader.get_all_values("245$a")
            assert len(all_titles) == len(reader)
            # Each should be a list
            assert all(isinstance(titles, list) for titles in all_titles)
            # Most records should have at least one title
            non_empty = sum(1 for t in all_titles if t)
            assert non_empty > len(reader) * 0.8  # At least 80% have titles

    def test_get_all_values_repeating_field(self, marc_file):
        """Test get_all_values with repeating field (650$a)."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f).build_index()

            all_subjects = reader.get_all_values("650$a")
            assert len(all_subjects) == len(reader)

            # Some records should have multiple subjects
            multi = [s for s in all_subjects if len(s) > 1]
            assert len(multi) > 0

    def test_get_all_values_nonexistent(self, marc_file):
        """Test get_all_values with nonexistent field."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f).build_index()

            result = reader.get_all_values("999$z")
            assert len(result) == len(reader)
            # All should be empty lists
            assert all(r == [] for r in result)

    def test_get_all_values_requires_index(self, marc_file):
        """Test that get_all_values requires build_index."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f)

            with pytest.raises(RuntimeError, match="Index not built"):
                reader.get_all_values("245$a")

    def test_get_all_values_consistency(self, marc_file):
        """Test that get_all_values is consistent across calls."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f).build_index()

            result1 = reader.get_all_values("245$a")
            result2 = reader.get_all_values("245$a")

            assert result1 == result2
