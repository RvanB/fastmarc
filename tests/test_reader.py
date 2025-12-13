"""Test core MARCReader functionality."""

import pytest
from fastmarc import MARCReader


class TestBasicReader:
    """Test basic reader operations."""

    def test_reader_len(self, marc_file):
        """Test getting record count."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f)
            count = len(reader)
            assert count > 0
            # Calling len again should return cached value
            assert len(reader) == count

    def test_reader_iteration(self, marc_file):
        """Test iterating through records."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f)
            count = 0
            for record in reader:
                count += 1
                assert hasattr(record, 'leader')
                if count == 10:
                    break
            assert count > 0
            
    def test_get_record(self, marc_file):
        """Test getting individual records by index."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f).build_index()

            # Get first record
            rec = reader.get_record(0)
            assert rec is not None
            assert hasattr(rec, 'leader')

            # Get middle record
            mid = len(reader) // 2
            rec = reader.get_record(mid)
            assert rec is not None

    def test_get_record_out_of_range(self, marc_file):
        """Test that out of range index raises error."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f).build_index()

            with pytest.raises(IndexError):
                reader.get_record(len(reader) + 1)

            with pytest.raises(IndexError):
                reader.get_record(-1)

    def test_context_manager(self, marc_file):
        """Test using reader as context manager."""
        with open(marc_file, "rb") as f:
            with MARCReader(f) as reader:
                count = len(reader)
                assert count > 0


class TestIndexing:
    """Test indexing functionality."""

    def test_add_index_auto_mode(self, marc_file):
        """Test add_index with auto-detected mode."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("control", "001")  # Should auto-detect as map
                     .add_index("title", "245$a")  # Should auto-detect as mask
                     .build_index())

            assert len(reader) > 0

    def test_add_index_explicit_map(self, marc_file):
        """Test add_index with explicit map mode."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("control", "001", mode="map")
                     .build_index())

            # Get the map index
            idx = reader.get_index("control")
            assert isinstance(idx, dict)
            assert len(idx) > 0

    def test_add_index_explicit_mask(self, marc_file):
        """Test add_index with explicit mask mode."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("title", "245$a", mode="mask")
                     .build_index())

            # Search should work
            results = reader.search("245$a", "music")
            assert isinstance(results, list)

    def test_duplicate_index_name_raises(self, marc_file):
        """Test that duplicate index names raise an error."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f).add_index("test", "001")

            with pytest.raises(ValueError, match="already registered"):
                reader.add_index("test", "245$a")

    def test_build_index_multiple_times(self, marc_file):
        """Test that build_index can be called multiple times safely."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f).add_index("title", "245$a")
            reader.build_index()
            count1 = len(reader)

            # Call again
            reader.build_index()
            count2 = len(reader)

            assert count1 == count2


class TestSearch:
    """Test search functionality."""

    def test_search_with_mask_index(self, marc_file):
        """Test fuzzy search with mask index."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("title", "245$a", mode="mask")
                     .build_index())

            results = reader.search("245$a", "music")
            assert isinstance(results, list)
            # Verify results are valid indices
            for idx in results[:5]:
                rec = reader.get_record(idx)
                title = rec['245']['a'] if '245' in rec and 'a' in rec['245'] else ""
                assert 'music' in title.lower()

    def test_search_with_map_index(self, marc_file):
        """Test exact search with map index."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("control", "001", mode="map")
                     .build_index())

            # Get a real control number first
            rec = reader.get_record(0)
            control_num = rec['001'].data if '001' in rec else None

            if control_num:
                results = reader.search("001", control_num)
                assert len(results) >= 1
                assert 0 in results

    def test_search_no_index_sequential(self, marc_file):
        """Test search without index performs sequential scan."""
        with open(marc_file, "rb") as f:
            reader = MARCReader(f).build_index()

            # Search a field we haven't indexed
            results = reader.search("260$a", "New York")
            assert isinstance(results, list)

    def test_search_nonexistent(self, marc_file):
        """Test searching for nonexistent values."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("title", "245$a")
                     .build_index())

            results = reader.search("245$a", "xyzabc123nonexistent")
            assert results == []


class TestCustomCharset:
    """Test custom charset for efficient indexing."""

    def test_custom_charset_digits(self, marc_file):
        """Test custom charset with digits only."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("title", "245$a")
                     .build_index(charset="0123456789"))

            # Should still work
            results = reader.search("245$a", "2020")
            assert isinstance(results, list)

    def test_custom_charset_small(self, marc_file):
        """Test custom charset with small character set."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("title", "245$a")
                     .build_index(charset="aeiou"))

            results = reader.search("245$a", "music")
            assert isinstance(results, list)

    def test_default_charset(self, marc_file):
        """Test default charset (a-z, 0-9)."""
        with open(marc_file, "rb") as f:
            reader = (MARCReader(f)
                     .add_index("title", "245$a")
                     .build_index())  # No charset = default

            results = reader.search("245$a", "music")
            assert isinstance(results, list)
