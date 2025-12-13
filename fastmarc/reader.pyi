"""Type stubs for fastmarc.reader Cython extension."""

from typing import Any, Iterator, Literal
from typing_extensions import Self
import pymarc

class MARCReader:
    """Fast MARC21 record reader with optional indexing support."""

    fp: Any
    """File-like object containing MARC records."""

    @property
    def indexing_enabled(self) -> bool:
        """Check if indexing is enabled."""
        ...

    def __init__(self, fp: Any, **kwargs: Any) -> None:
        """
        Initialize MARC reader.

        Args:
            fp: File-like object opened in binary mode containing MARC records
            **kwargs: Additional arguments (reserved for future use)
        """
        ...

    def add_index(
        self,
        name: str,
        field_spec: str,
        mode: Literal["mask", "map"] | None = None,
    ) -> Self:
        """
        Register a named index for a field (mask or map mode).

        Args:
            name: String identifier for this index (used in search operations)
            field_spec: Field specification (e.g., "001", "245$a", "020$a")
            mode: Indexing mode:
                  "mask" - Bitmask for substring search (fuzzy matching)
                  "map" - Hash map for exact lookup (O(1) retrieval)
                  None - Auto-detect: "map" for control fields (001-009), "mask" otherwise

        Returns:
            self (for method chaining)

        Raises:
            RuntimeError: If called after .build_index() has been called
            ValueError: If index name already registered or mode is invalid
        """
        ...

    def build_index(self, charset: str | None = None) -> Self:
        """
        Build the record index.

        Must be invoked before using search(), len(), get_record().
        Streaming iteration without calling .build_index() is supported.

        Args:
            charset: Optional custom character set for fuzzy indexing. If provided,
                    only these characters will be indexed for fuzzy search.

        Returns:
            self (for method chaining)
        """
        ...

    def close(self) -> None:
        """Release resources (mmap + C buffers). Safe to call multiple times."""
        ...

    def get_record(self, idx: int) -> pymarc.Record:
        """
        Return the pymarc.Record at the given index.

        Args:
            idx: Zero-based record index

        Returns:
            pymarc.Record at the specified index

        Raises:
            RuntimeError: If .build_index() not yet called
            IndexError: If index out of range
        """
        ...

    def get_seek_map(self) -> list[int]:
        """
        Return list of record start offsets (byte positions in file).

        Returns:
            List of file offsets, one per record
        """
        ...

    def get_index(self, name: str) -> dict[str, list[int]]:
        """
        Return the map index for a named index (value -> list of record indices).

        Args:
            name: Index name (must be mode="map")

        Returns:
            dict mapping field values (str) to list of record indices

        Raises:
            ValueError: If index name not found or index mode is not "map"
            RuntimeError: If .build_index() not yet called
        """
        ...

    def get_all_values(self, field_spec: str) -> list[list[str]]:
        """
        Get all values of a field from every record in the file.

        Scans through all records and extracts the specified field/subfield,
        returning a list of lists where each entry corresponds to one record.

        Args:
            field_spec: Field specification (e.g., "001", "245$a", "650$a")

        Returns:
            List of lists, one per record (length = number of records).
            Each inner list contains all occurrences of the field in that record.
            Empty inner list if the record doesn't have that field.

        Raises:
            RuntimeError: If .build_index() not yet called
        """
        ...

    def foreach(
        self, field_specs: list[str], callable: Any
    ) -> None:
        """
        Execute a callable on specified fields for every record.

        Efficiently loops through all records once, extracting the specified fields
        and calling the provided callable with a dict of field values for each record.
        This is more memory-efficient than get_all_values() since values are not
        accumulated in memory.

        Args:
            field_specs: List of field specifications (e.g., ["650$a", "008", "264$c"])
            callable: Python callable that accepts one argument - a dict mapping
                     field_spec -> list of values for that record

        Returns:
            None

        Example:
            ```python
            from collections import Counter

            # Count all subjects
            subject_counts = Counter()
            def count_subjects(fields):
                for subject in fields.get("650$a", []):
                    subject_counts[subject.strip()] += 1

            reader.foreach(["650$a"], count_subjects)
            print(f"Top subjects: {subject_counts.most_common(10)}")

            # Multi-field example: extract years from 008 or 264$c
            years = Counter()
            def extract_year(fields):
                if "008" in fields and fields["008"]:
                    year = fields["008"][0][7:11]
                    if year.isdigit():
                        years[year] += 1
                elif "264$c" in fields and fields["264$c"]:
                    import re
                    match = re.search(r'(19|20)\\d{2}', fields["264$c"][0])
                    if match:
                        years[match.group(0)] += 1

            reader.foreach(["008", "264$c"], extract_year)
            ```

        Note:
            Does not require .build_index() to be called first. Will build
            the record offset index if not already built, but does not build
            search indexes.
        """
        ...

    def search(self, field_spec: str, text: str) -> list[int]:
        """
        Search for records containing text in the specified field.

        Automatically uses index if available (map or mask mode), otherwise performs
        sequential scan through all records.

        Args:
            field_spec: Field specification (e.g., "245$a", "001", "020$a")
            text: Text to search for (exact match for map mode, substring for mask/sequential)

        Returns:
            List of record indices (may be empty, single item, or multiple for collisions)

        Raises:
            RuntimeError: If .build_index() not yet called
        """
        ...

    def __len__(self) -> int:
        """
        Get total record count.

        Returns:
            Number of records in the file

        Note:
            Automatically scans the file to count records if not already done.
            The count is cached for subsequent calls.
        """
        ...

    def __iter__(self) -> Iterator[pymarc.Record]:
        """
        Iterate through records.

        Yields:
            pymarc.Record objects

        Note:
            Works both with and without .build_index() being called.
            Resets iteration position to start.
        """
        ...

    def __next__(self) -> pymarc.Record:
        """
        Get next record in iteration.

        Returns:
            Next pymarc.Record

        Raises:
            StopIteration: When no more records available
        """
        ...

    def __enter__(self) -> Self:
        """Context manager entry."""
        ...

    def __exit__(self, *args: Any) -> None:
        """Context manager exit - calls close()."""
        ...
