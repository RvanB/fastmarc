"""Test dual indexing (map + mask modes)."""

import pytest
from fastmarc import MARCReader


class TestDualIndex:
    """Test using both map and mask indexes together."""

    def test_map_and_mask_together(self, marc_file):
        """Test using both map and mask indexes in same reader."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("control", "001", mode="map")
                     .add_index("title", "245$a", mode="mask")
                     .build_index())

            # Both should work
            assert len(reader) > 0

    def test_get_index_map(self, marc_file):
        """Test getting a map index."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("control", "001", mode="map")
                     .build_index())

            idx = reader.get_index("control")
            assert isinstance(idx, dict)
            assert len(idx) > 0
            # Values should be lists of indices
            for value, indices in list(idx.items())[:5]:
                assert isinstance(indices, list)
                assert all(isinstance(i, int) for i in indices)

    def test_get_index_mask_raises(self, marc_file):
        """Test that get_index raises for mask indexes."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("title", "245$a", mode="mask")
                     .build_index())

            with pytest.raises(ValueError, match="mode='mask'"):
                reader.get_index("title")

    def test_get_index_nonexistent_raises(self, marc_file):
        """Test that get_index raises for nonexistent index."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f).build_index()

            with pytest.raises(ValueError, match="not found"):
                reader.get_index("nonexistent")

    def test_map_index_collisions(self, marc_file):
        """Test that map index handles collisions (multiple records with same value)."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("title", "245$a", mode="map")
                     .build_index())

            idx = reader.get_index("title")

            # Find titles with collisions
            collisions = {title: idxs for title, idxs in idx.items() if len(idxs) > 1}

            if collisions:
                # Verify search returns all collision indices
                for title, expected_indices in list(collisions.items())[:3]:
                    results = reader.search("245$a", title)
                    assert set(results) == set(expected_indices)

    def test_search_with_both_index_types(self, marc_file):
        """Test searching with both map and mask indexes."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("control", "001", mode="map")
                     .add_index("title", "245$a", mode="mask")
                     .build_index())

            # Map search (exact)
            rec = reader.get_record(0)
            control_num = rec['001'].data if '001' in rec else None
            if control_num:
                map_results = reader.search("001", control_num)
                assert 0 in map_results

            # Mask search (fuzzy)
            mask_results = reader.search("245$a", "journal")
            assert isinstance(mask_results, list)

    def test_multiple_map_indexes(self, marc_file):
        """Test multiple map indexes on different fields."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("control", "001", mode="map")
                     .add_index("isbn", "020$a", mode="map")
                     .build_index())

            control_idx = reader.get_index("control")
            isbn_idx = reader.get_index("isbn")

            assert isinstance(control_idx, dict)
            assert isinstance(isbn_idx, dict)

    def test_multiple_mask_indexes(self, marc_file):
        """Test multiple mask indexes on different fields."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("title", "245$a", mode="mask")
                     .add_index("subject", "650$a", mode="mask")
                     .build_index())

            title_results = reader.search("245$a", "music")
            subject_results = reader.search("650$a", "music")

            assert isinstance(title_results, list)
            assert isinstance(subject_results, list)
