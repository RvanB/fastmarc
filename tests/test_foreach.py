"""Test foreach() functionality."""

import re
from collections import Counter
import pytest
from fastmarc import MARCReader


class TestForeach:
    """Test foreach method."""

    def test_foreach_single_field(self, marc_file):
        """Test foreach with a single field."""
        counts = Counter()

        def count_subjects(fields):
            for subject in fields.get("650$a", []):
                counts[subject.strip()] += 1

        with open(marc_file, "rb") as f:
            reader = MARCReader(f)
            reader.foreach(["650$a"], count_subjects)

        assert len(counts) > 0
        # Verify we actually counted subjects
        total = sum(counts.values())
        assert total > 0

    def test_foreach_multiple_fields(self, marc_file):
        """Test foreach with multiple fields."""
        years = Counter()

        def extract_year(fields):
            year = None
            # Try 008 first
            if "008" in fields and fields["008"]:
                if len(fields["008"][0]) >= 11:
                    year = fields["008"][0][7:11]
                    if year.isdigit():
                        years[year] += 1

        with open(marc_file, "rb") as f:
            reader = MARCReader(f)
            reader.foreach(["008"], extract_year)

        assert len(years) > 0

    def test_foreach_no_build_index(self, marc_file):
        """Test that foreach works without build_index."""
        count = 0

        def counter(fields):
            nonlocal count
            count += 1

        with open(marc_file, "rb") as f:
            reader = MARCReader(f)
            # Don't call build_index
            reader.foreach(["245$a"], counter)

        assert count > 0

    def test_foreach_vs_get_all_values(self, marc_file):
        """Test that foreach matches get_all_values."""
        foreach_results = []

        def collect(fields):
            foreach_results.append(fields.get("245$a", []))

        with open(marc_file, "rb") as f:
            reader = MARCReader(f).build_index()
            reader.foreach(["245$a"], collect)

        with open(marc_file, "rb") as f:
            reader = MARCReader(f).build_index()
            get_all_results = reader.get_all_values("245$a")

        assert foreach_results == get_all_results

    def test_foreach_empty_field(self, marc_file):
        """Test foreach with field that doesn't exist."""
        count = 0
        found_any = False

        def check(fields):
            nonlocal count, found_any
            count += 1
            if fields.get("999$z", []):
                found_any = True

        with open(marc_file, "rb") as f:
            reader = MARCReader(f)
            reader.foreach(["999$z"], check)

        assert count > 0
        assert not found_any  # Should not find any 999$z

    def test_foreach_filter(self, marc_file):
        """Test filtering with foreach."""
        music_subjects = []

        def filter_music(fields):
            for subject in fields.get("650$a", []):
                if "music" in subject.lower():
                    music_subjects.append(subject)

        with open(marc_file, "rb") as f:
            reader = MARCReader(f)
            reader.foreach(["650$a"], filter_music)

        # Verify all collected subjects contain "music"
        for subject in music_subjects:
            assert "music" in subject.lower()

    def test_foreach_multi_occurrence(self, marc_file):
        """Test detecting multiple occurrences of a field."""
        multi_count = 0

        def detect_multi(fields):
            nonlocal multi_count
            if len(fields.get("650$a", [])) > 3:
                multi_count += 1

        with open(marc_file, "rb") as f:
            reader = MARCReader(f)
            reader.foreach(["650$a"], detect_multi)

        # Should find at least some records with multiple subjects
        assert multi_count > 0

    def test_foreach_complex_processing(self, marc_file):
        """Test foreach with complex multi-field processing."""
        stats = {
            'has_title': 0,
            'has_subject': 0,
            'has_both': 0,
            'total': 0
        }

        def analyze(fields):
            stats['total'] += 1
            has_title = bool(fields.get("245$a", []))
            has_subject = bool(fields.get("650$a", []))

            if has_title:
                stats['has_title'] += 1
            if has_subject:
                stats['has_subject'] += 1
            if has_title and has_subject:
                stats['has_both'] += 1

        with open(marc_file, "rb") as f:
            reader = MARCReader(f)
            reader.foreach(["245$a", "650$a"], analyze)

        assert stats['total'] > 0
        assert stats['has_title'] > 0
        assert stats['has_subject'] > 0
        assert stats['has_both'] > 0
