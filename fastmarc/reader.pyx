# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True

import io
import mmap
import pymarc

from libc.stdlib cimport malloc, realloc, free
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t
from libc.stddef cimport size_t
from libc.string cimport memset

# ============================================================================
# Low-level MARC parsing helpers
# ============================================================================

cdef inline int _atoi4(const unsigned char* p) nogil:
    """Convert 4 ASCII digits to integer."""
    return (p[0]-48)*1000 + (p[1]-48)*100 + (p[2]-48)*10 + (p[3]-48)

cdef inline int _atoi5(const unsigned char* p) nogil:
    """Convert 5 ASCII digits to integer."""
    return (p[0]-48)*10000 + (p[1]-48)*1000 + (p[2]-48)*100 + (p[3]-48)*10 + (p[4]-48)

cdef inline bint _tag_eq3(const unsigned char* p, const unsigned char* t) nogil:
    """Compare 3-character tag."""
    return p[0] == t[0] and p[1] == t[1] and p[2] == t[2]

cdef inline uint8_t _to_lower(uint8_t ch) nogil:
    """Convert ASCII uppercase to lowercase."""
    if 65 <= ch <= 90:  # A-Z
        return ch + 32
    return ch

cdef inline bint _memmem_icase(const unsigned char* hay, int hlen,
                                const unsigned char* needle, int nlen) nogil:
    """Case-insensitive substring search in memory."""
    cdef int i, j
    cdef uint8_t h_ch, n_ch
    
    if nlen == 0:
        return 1
    if nlen > hlen:
        return 0
    
    i = 0
    while i <= hlen - nlen:
        h_ch = _to_lower(hay[i])
        n_ch = _to_lower(needle[0])
        if h_ch == n_ch:
            j = 1
            while j < nlen:
                h_ch = _to_lower(hay[i + j])
                n_ch = _to_lower(needle[j])
                if h_ch != n_ch:
                    break
                j += 1
            if j == nlen:
                return 1
        i += 1
    return 0

cdef inline bint _is_control_tag(const unsigned char* tag3) nogil:
    """Check if tag is a control field (00X)."""
    return tag3[0] == 48 and tag3[1] == 48  # '0','0'

cdef inline const unsigned char* _find_subfield_ptr(
    const unsigned char* rec_ptr, Py_ssize_t reclen,
    const unsigned char* tag3, char subfield, int* out_len) nogil:
    """
    Find a subfield in a MARC record and return pointer + length.
    
    Args:
        rec_ptr: Pointer to MARC record
        reclen: Record length
        tag3: 3-byte tag to find
        subfield: Subfield code or 0 for entire field
        out_len: Output parameter for subfield length
    
    Returns:
        Pointer to subfield data, or NULL if not found.
        Sets out_len[0] to the length of the subfield.
    """
    cdef int base = _atoi5(rec_ptr + 12)
    cdef const unsigned char* dirp = rec_ptr + 24
    cdef int dirlen = base - 24
    cdef int i = 0, flen = 0, pos = 0, j = 0, k = 0
    cdef const unsigned char* field

    out_len[0] = 0
    
    if reclen < 24 or base <= 24 or base > reclen:
        return NULL

    while i < dirlen:
        if _tag_eq3(dirp + i, tag3):
            flen = _atoi4(dirp + i + 3)
            pos  = _atoi5(dirp + i + 7)
            if base + pos >= reclen or flen <= 0:
                return NULL
            field = rec_ptr + base + pos
            
            if _is_control_tag(tag3) or subfield == 0:
                # Control field or whole field requested
                j = 0
                while j < flen and field[j] != 0x1E:
                    j += 1
                out_len[0] = j
                return field
            else:
                # Data field - search for subfield
                j = 2  # Skip indicators
                while j < flen:
                    if field[j] == 0x1F:  # Subfield delimiter
                        if j + 1 < flen and field[j + 1] == <uint8_t> subfield:
                            j += 2
                            k = j
                            while k < flen and field[k] not in (0x1F, 0x1E):
                                k += 1
                            out_len[0] = k - j
                            return field + j
                        else:
                            j += 2
                            continue
                    elif field[j] == 0x1E:  # Field terminator
                        break
                    j += 1
            return NULL
        i += 12
    return NULL


cdef bint record_contains(const unsigned char* rec_ptr, Py_ssize_t reclen,
                          bytes tag, char subfield, bytes needle):
    """Check if a MARC record contains a substring in a field/subfield (case-insensitive)."""
    cdef const unsigned char* tag3 = <const unsigned char*> tag
    cdef const unsigned char* ndl = <const unsigned char*> needle
    cdef int nlen = <int> len(needle)
    cdef int length = 0
    cdef const unsigned char* ptr
    
    if nlen == 0:
        return 1
    
    ptr = _find_subfield_ptr(rec_ptr, reclen, tag3, subfield, &length)
    if ptr == NULL:
        return 0
    
    return _memmem_icase(ptr, length, ndl, nlen)

cdef list get_subfields(const unsigned char* rec_ptr, Py_ssize_t reclen,
                        bytes tag, char subfield):
    """Return all occurrences of a tag/subfield as a list of bytes objects.

    For control fields (00X) or when subfield == 0, each field occurrence is
    returned as a single bytes value (entire field up to 0x1E terminator).
    For data fields, each matching subfield occurrence is returned.
    If no occurrences exist, returns an empty list.
    """
    cdef list out = []
    cdef const unsigned char* tag3 = <const unsigned char*> tag
    cdef int base = _atoi5(rec_ptr + 12)
    cdef int dirlen
    cdef const unsigned char* dirp
    cdef int i = 0, flen = 0, pos = 0, j = 0, k = 0
    cdef const unsigned char* field
    cdef bint is_control

    if reclen < 24 or base <= 24 or base > reclen:
        return out
    dirp = rec_ptr + 24
    dirlen = base - 24
    is_control = _is_control_tag(tag3) or subfield == 0

    while i < dirlen:
        if _tag_eq3(dirp + i, tag3):
            flen = _atoi4(dirp + i + 3)
            pos  = _atoi5(dirp + i + 7)
            if base + pos >= reclen or flen <= 0:
                i += 12
                continue
            field = rec_ptr + base + pos
            if is_control:
                # Entire field until 0x1E
                j = 0
                while j < flen and field[j] != 0x1E:
                    j += 1
                out.append(<bytes> field[:j])
            else:
                # Scan indicators then subfields
                j = 2
                while j < flen:
                    if field[j] == 0x1F:  # subfield delimiter
                        if j + 1 < flen:
                            if field[j + 1] == <uint8_t> subfield:
                                j += 2
                                k = j
                                while k < flen and field[k] not in (0x1F, 0x1E):
                                    k += 1
                                out.append(<bytes> field[j:k])
                                j = k
                                continue
                            else:
                                j += 2
                                continue
                        else:
                            break
                    elif field[j] == 0x1E:
                        break
                    else:
                        j += 1
        i += 12
    return out

# ============================================================================
# Bitmask indexing helpers
# ============================================================================

# Global state for mask type and custom character mapping
cdef int _mask_type = 3  # 0=uint8, 1=uint16, 2=uint32, 3=uint64, 4=multi-uint64
cdef int _mask_bytes_per_record = 8  # Bytes per mask
cdef int[256] _custom_char_map  # Custom character to bit mapping
cdef int _custom_fallback_bit = -1  # Fallback bit for unmapped chars

# Type-specific mask bit-setting functions
cdef inline void _mask_set_u8(uint8_t* m, int bit) nogil:
    """Set bit in uint8_t mask."""
    m[0] |= <uint8_t>(1 << bit)

cdef inline void _mask_set_u16(uint16_t* m, int bit) nogil:
    """Set bit in uint16_t mask."""
    m[0] |= <uint16_t>(1 << bit)

cdef inline void _mask_set_u32(uint32_t* m, int bit) nogil:
    """Set bit in uint32_t mask."""
    m[0] |= <uint32_t>(1 << bit)

cdef inline void _mask_set_u64(uint64_t* m, int bit) nogil:
    """Set bit in uint64_t mask."""
    m[0] |= <uint64_t>(1 << bit)

cdef inline void _mask_set_multi(uint64_t* m, int bit) nogil:
    """Set bit in multi-uint64 mask (for >64 bits)."""
    m[bit >> 6] |= <uint64_t>1 << (bit & 63)

# Character mapping functions
cdef inline int _map_char_default(uint8_t ch) nogil:
    """Default character mapping (0-25: a-z, 26-35: 0-9, 63: fallback)."""
    # Convert uppercase to lowercase
    if 65 <= ch <= 90:  # A-Z
        ch += 32
    # Lowercase letters
    if 97 <= ch <= 122:  # a-z
        return ch - 97
    # Digits
    if 48 <= ch <= 57:  # 0-9
        return 26 + (ch - 48)
    # Fallback bucket for all other characters
    return 63

cdef inline int _map_char_custom(uint8_t ch) nogil:
    """Map character using custom mapping table."""
    cdef int bit = _custom_char_map[ch]
    if bit >= 0:
        return bit
    if _custom_fallback_bit >= 0:
        return _custom_fallback_bit
    return 0  # No fallback, ignore unmapped chars

cdef inline int _map_char(uint8_t ch) nogil:
    """Map character (uses custom mapping if set, otherwise default)."""
    if _custom_fallback_bit >= 0 or _custom_char_map[0] >= 0:
        return _map_char_custom(ch)
    return _map_char_default(ch)

# ============================================================================
# MARCReader class
# ============================================================================

cdef class MARCReader:

    cdef public object fp
    cdef public object _mm

    cdef size_t* _offsets
    cdef int*    _lengths
    cdef Py_ssize_t _n
    cdef Py_ssize_t _cap

    cdef Py_ssize_t _i
    cdef list _iter_records  # Cached scan results for iteration
    
    # Explicit indexing (inactive until .build_index() is called)
    cdef bint _indexing_enabled        # Set True when user calls .build_index()
    cdef bint _index_built             # Whether index
    cdef void* _masks                  # Allocated after indexing enabled
    cdef int _mask_bytes_per_record    # Bytes per mask (computed at .build_index())
    cdef int _bits_per_mask            # Bits required (computed at .build_index())
    cdef list _index_fields            # Fields used for fuzzy bitmask indexing
    cdef str _charset                  # Optional custom charset

    # Named indexes: name -> index metadata
    cdef dict _indexes                 # name -> {"field_spec": str, "mode": str, "tag_bytes": bytes, "subfield_byte": int}
    cdef dict _map_indexes             # name -> {value: [idx, idx, ...]} for mode="map"
    cdef list _mask_index_names        # List of index names for mode="mask"

    def __cinit__(self, fp, **kwargs):
        self.fp = fp
        self._mm = None
        self._offsets = <size_t*> NULL
        self._lengths = <int*> NULL
        self._n = 0
        self._cap = 0
        self._i = 0
        self._iter_records = None

        # Indexing inactive until .build_index() invoked
        self._indexing_enabled = False
        self._index_built = False
        self._masks = NULL
        self._charset = None
        self._bits_per_mask = 0
        self._mask_bytes_per_record = 0
        self._index_fields = []  # Legacy: list of field_specs for fuzzy indexing

    def __init__(self, fp, **kwargs):
        # Initialize named index structures
        self._indexes = {}  # name -> metadata
        self._map_indexes = {}  # name -> {value: [idx1, idx2, ...]}
        self._mask_index_names = []  # list of names for mask indexes

    def add_index(self, name, field_spec, mode=None):
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

        Examples:
            # Auto-detect: control fields use map, data fields use mask
            reader = (MARCReader(fp)
                .add_index("control_num", "001")      # Map (auto-detected)
                .add_index("title", "245$a")          # Mask (auto-detected)
                .build_index())

            # Explicit mode:
            reader = (MARCReader(fp)
                .add_index("isbn", "020$a", mode="map")    # ISBN: exact lookup
                .add_index("subject", "650$a", mode="mask")  # Subjects: substring search
                .build_index())
        """
        if self._index_built:
            raise RuntimeError("Cannot add indexes after .build_index() has been called")

        if name in self._indexes:
            raise ValueError(f"Index '{name}' already registered")

        # Parse field spec to determine tag
        if "$" in field_spec:
            tag, subfield = field_spec.split("$", 1)
            tag_bytes = tag.encode("ascii")
            subfield_byte = ord(subfield[0]) if subfield else 0
        else:
            tag = field_spec
            tag_bytes = tag.encode("ascii")
            subfield_byte = 0

        # Auto-detect mode based on tag
        if mode is None:
            is_control = _is_control_tag(<const unsigned char*>tag_bytes)
            mode = "map" if is_control else "mask"

        if mode not in ("mask", "map"):
            raise ValueError(f"mode must be 'mask' or 'map', got '{mode}'")

        # Store index metadata
        self._indexes[name] = {
            "field_spec": field_spec,
            "mode": mode,
            "tag_bytes": tag_bytes,
            "subfield_byte": subfield_byte
        }

        if mode == "mask":
            # Add to mask indexes
            self._mask_index_names.append(name)
            # Also add to legacy _index_fields for now
            if field_spec not in self._index_fields:
                self._index_fields.append(field_spec)
        else:  # mode == "map"
            # Initialize map index
            self._map_indexes[name] = {}

        return self

    def _setup_charset(self, charset):
        """Setup custom character mapping for fuzzy indexing.

        Args:
            charset: String containing characters to index
        """
        global _custom_char_map, _custom_fallback_bit

        cdef int i
        cdef int ch_byte

        _custom_fallback_bit = -1
        # Initialize map to -1 (unmapped)
        for i in range(256):
            _custom_char_map[i] = -1

        # Map each character in charset to a bit position
        unique_chars = list(set(charset))
        for i, char in enumerate(unique_chars):
            ch_byte = ord(char)
            _custom_char_map[ch_byte] = i
            # Also map uppercase if lowercase
            if 97 <= ch_byte <= 122:  # a-z
                _custom_char_map[ch_byte - 32] = i  # Map A-Z to same bit
            elif 65 <= ch_byte <= 90:  # A-Z
                _custom_char_map[ch_byte + 32] = i  # Map a-z to same bit

        # Set fallback bit
        _custom_fallback_bit = len(unique_chars)

    def build_index(self, charset=None):
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
        if not self._index_built:
            # Store charset if provided
            if charset is not None:
                self._charset = charset

            # Setup custom character mapping if charset provided
            global _custom_char_map, _custom_fallback_bit
            if self._charset is not None:
                self._setup_charset(self._charset)
            else:
                # Reset to use default mapping
                _custom_fallback_bit = -1
                _custom_char_map[0] = -1

            # Enable indexing if we have fuzzy fields
            if self._index_fields and not self._indexing_enabled:
                self._indexing_enabled = True
                # Compute mask sizing
                if self._charset is not None:
                    charset_len = len(set(self._charset))
                    self._bits_per_mask = charset_len + 1
                else:
                    self._bits_per_mask = 256
                if self._bits_per_mask <= 8:
                    self._mask_bytes_per_record = 1
                elif self._bits_per_mask <= 16:
                    self._mask_bytes_per_record = 2
                elif self._bits_per_mask <= 32:
                    self._mask_bytes_per_record = 4
                elif self._bits_per_mask <= 64:
                    self._mask_bytes_per_record = 8
                else:
                    self._mask_bytes_per_record = ((self._bits_per_mask + 63)//64)*8

            self._build_index()
            self._build_masks()
            self._index_built = True
        return self
    

    def close(self):
        """Release resources (mmap + C buffers). Safe to call multiple times."""
        if self._mm is not None:
            try:
                self._mm.close()
            except Exception:
                pass
            self._mm = None
        if self._offsets != NULL:
            free(self._offsets)
            self._offsets = <size_t*> NULL
        if self._lengths != NULL:
            free(self._lengths)
            self._lengths = <int*> NULL
        if self._masks != NULL:
            free(self._masks)
            self._masks = NULL
        self._n = 0
        self._cap = 0

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, *args):
        """Context manager exit - calls close()."""
        self.close()

    cdef void _reserve(self, Py_ssize_t needed):
        """Ensure capacity for at least `needed` items in both arrays."""
        if needed <= self._cap:
            return
        cdef Py_ssize_t new_cap = self._cap * 2 if self._cap > 0 else 4096
        if new_cap < needed:
            new_cap = needed

        cdef void* p_off = NULL
        cdef void* p_len = NULL

        if self._offsets == NULL:
            p_off = malloc(<size_t>new_cap * sizeof(size_t))
        else:
            p_off = realloc(self._offsets, <size_t>new_cap * sizeof(size_t))

        if p_off == NULL:
            raise MemoryError("Unable to allocate offsets array")

        if self._lengths == NULL:
            p_len = malloc(<size_t>new_cap * sizeof(int))
        else:
            p_len = realloc(self._lengths, <size_t>new_cap * sizeof(int))

        if p_len == NULL:
            free(p_off)
            raise MemoryError("Unable to allocate lengths array")

        self._offsets = <size_t*> p_off
        self._lengths = <int*> p_len
        self._cap = new_cap

    cdef inline void _append(self, size_t pos, int L):
        """Append one (pos, L) to the arrays."""
        if self._n >= self._cap:
            self._reserve(self._n + 1)
        self._offsets[self._n] = pos
        self._lengths[self._n] = L
        self._n += 1

    cdef list _scan_records(self):
        """
        Scan through all records in the file and return offsets.

        Returns:
            List of (offset, length) tuples for each record
        """
        cdef Py_ssize_t i = 0
        cdef Py_ssize_t size = 0
        cdef int L = 0
        cdef list records = []
        cdef object mm = None
        cdef const uint8_t[:] buf

        try:
            fileno = self.fp.fileno()
            mm = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
            buf = mm
            size = buf.shape[0]

            i = 0
            while i + 5 <= size:
                # parse 5 ASCII digits as record length ('0' == 48)
                L = ((buf[i]   - 48) * 10000 +
                     (buf[i+1] - 48) * 1000  +
                     (buf[i+2] - 48) * 100   +
                     (buf[i+3] - 48) * 10    +
                     buf[i+4] - 48)

                if L <= 0 or i + L > size:
                    break

                records.append((<size_t>i, L))
                i += L

            mm.close()
            return records

        except Exception:
            try:
                if mm is not None:
                    mm.close()
            except Exception:
                pass

        # Fallback to file reading
        self.fp.seek(0, io.SEEK_SET)
        cdef long pos
        cdef bytes head
        while True:
            pos = self.fp.tell()
            head = self.fp.read(5)
            if not head:
                break
            try:
                L = int(head)
            except Exception:
                break
            if L <= 0:
                break
            records.append((<size_t>pos, L))
            self.fp.seek(L - 5, io.SEEK_CUR)
        self.fp.seek(0, io.SEEK_SET)
        return records

    cdef void _build_index(self):
        """Build the record offset index and store in C arrays."""
        cdef list records = self._scan_records()
        cdef size_t offset
        cdef int length
        cdef Py_ssize_t hint = len(records)

        # Pre-allocate capacity
        if hint > 0:
            self._reserve(hint)

        # Try to get mmap for zero-copy access
        cdef object mm = None
        try:
            fileno = self.fp.fileno()
            mm = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
            self._mm = mm
        except Exception:
            self._mm = None

        # Store all records
        for offset, length in records:
            self._append(offset, length)

    def __iter__(self):
        """Iterate through records without requiring build_index()."""
        # Scan records if not already done
        self._iter_records = self._scan_records()
        self._i = 0
        return self

    def __next__(self):
        """Get next record from iteration."""
        cdef Py_ssize_t idx = self._i
        cdef size_t pos
        cdef int L
        cdef bytes raw

        if self._iter_records is None or idx >= len(self._iter_records):
            raise StopIteration

        self._i = idx + 1
        pos, L = self._iter_records[idx]

        # Read the raw MARC data
        self.fp.seek(pos, io.SEEK_SET)
        raw = self.fp.read(L)

        return pymarc.Record(data=raw)

    
    def get_record(self, Py_ssize_t idx):
        """Return the pymarc.Record at the given index (requires prior .build_index())."""
        if not self._index_built:
            raise RuntimeError("Index not built. Call .build_index() first.")
            
        if idx < 0 or idx >= self._n:
            raise IndexError("Record index out of range")

        cdef size_t pos = self._offsets[idx]
        cdef int L = self._lengths[idx]
        cdef Py_ssize_t p
        cdef Py_ssize_t q

        if self._mm is not None:
            p = <Py_ssize_t>pos
            q = p + <Py_ssize_t>L
            raw = bytes(self._mm[p:q])
        else:
            self.fp.seek(pos, io.SEEK_SET)
            raw = self.fp.read(L)

        return pymarc.Record(data=raw)


    def get_seek_map(self):
        """
        Return Python list of record start offsets.
        """
        cdef Py_ssize_t n = self._n
        cdef list out = [0] * n
        cdef Py_ssize_t j
        for j in range(n):
            out[j] = self._offsets[j]
        return out

    cpdef dict get_index(self, str name):
        """Return the map index for a named index (value -> list of record indices).

        Args:
            name: Index name (must be mode="map")

        Returns:
            dict mapping field values (str) to list of record indices

        Raises:
            ValueError: If index name not found or index mode is not "map"
            RuntimeError: If .build_index() not yet called
        """
        if not self._index_built:
            raise RuntimeError("Index not built. Call .build_index() first.")

        if name not in self._indexes:
            raise ValueError(f"Index '{name}' not found. Use .add_index() to register it first.")

        index_meta = self._indexes[name]
        if index_meta["mode"] != "map":
            raise ValueError(f"Index '{name}' is mode='{index_meta['mode']}', only mode='map' indexes support get_index()")

        return self._map_indexes[name]

    def get_all_values(self, str field_spec):
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

        Example:
            reader = MARCReader(fp).build_index()

            # Returns [[title1], [title2], [], [title4], ...] - one per record
            all_titles = reader.get_all_values("245$a")

            # For repeating fields like 650$a
            all_subjects = reader.get_all_values("650$a")
            # Returns [["History", "Music"], [], ["Science"], ...]

            # Find records with multiple subjects
            for idx, subjects in enumerate(all_subjects):
                if len(subjects) > 3:
                    print(f"Record {idx} has {len(subjects)} subjects")

        Note: Requires .build_index() to have been called first.
        """
        if not self._index_built:
            raise RuntimeError("Index not built. Call .build_index() first.")

        # Parse field spec
        cdef bytes tag_bytes
        cdef char subfield_byte
        cdef list parts
        if "$" in field_spec:
            parts = field_spec.split("$", 1)
            tag_bytes = (<str>parts[0]).encode("ascii")
            subfield_byte = ord((<str>parts[1])[0]) if parts[1] else 0
        else:
            tag_bytes = field_spec.encode("ascii")
            subfield_byte = 0

        cdef list out = []
        cdef Py_ssize_t i
        cdef const uint8_t[:] buf = self._mm
        cdef const uint8_t* rec_ptr
        cdef int reclen
        cdef list occurrences
        cdef list record_values

        if self._mm is None:
            # Return empty lists for each record
            return [[] for _ in range(self._n)]

        # Scan all records
        for i in range(self._n):
            rec_ptr = &buf[self._offsets[i]]
            reclen = self._lengths[i]

            # Get all occurrences of this field/subfield in this record
            occurrences = get_subfields(rec_ptr, reclen, tag_bytes, subfield_byte)

            # Decode all occurrences for this record
            record_values = []
            for raw_val in occurrences:
                record_values.append(raw_val.decode('utf-8', errors='replace'))

            # Append this record's values (even if empty list)
            out.append(record_values)

        return out

    def foreach(self, list field_specs, callable):
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

        Note: Does not require .build_index() to be called first. Will build
              the record offset index if not already built, but does not build
              search indexes.
        """
        # Build record index if not already built (but not search indexes)
        if not self._index_built:
            if self._n == 0:
                self._build_index()

        # If still no records, return early
        if self._n == 0:
            return

        # Parse all field specs once
        cdef list parsed_specs = []
        cdef bytes tag_bytes
        cdef char subfield_byte
        cdef str spec

        for spec in field_specs:
            if "$" in spec:
                parts = spec.split("$", 1)
                tag_bytes = (<str>parts[0]).encode("ascii")
                subfield_byte = ord((<str>parts[1])[0]) if parts[1] else 0
            else:
                tag_bytes = spec.encode("ascii")
                subfield_byte = 0
            parsed_specs.append((spec, tag_bytes, subfield_byte))

        # Check if we have mmap available
        if self._mm is None:
            # Without mmap, we can't efficiently access records
            return

        cdef const uint8_t[:] buf = self._mm
        cdef const uint8_t* rec_ptr
        cdef int reclen
        cdef Py_ssize_t i
        cdef list occurrences
        cdef dict fields_dict
        cdef list decoded_values

        # Loop through all records
        for i in range(self._n):
            rec_ptr = &buf[self._offsets[i]]
            reclen = self._lengths[i]

            # Extract all requested fields for this record
            fields_dict = {}
            for spec, tag_bytes, subfield_byte in parsed_specs:
                occurrences = get_subfields(rec_ptr, reclen, tag_bytes, subfield_byte)

                # Decode all occurrences
                decoded_values = []
                for raw_val in occurrences:
                    decoded_values.append(raw_val.decode('utf-8', errors='replace'))

                fields_dict[spec] = decoded_values

            # Call the user's callable with the fields dict
            callable(fields_dict)

    def __len__(self):
        # Return cached count if available
        if self._n > 0:
            return self._n

        # Otherwise, scan records and cache the count
        cdef list records = self._scan_records()
        self._n = len(records)
        return self._n

    # ========================================================================
    # Indexing Support
    # ========================================================================

    cdef void _reserve_masks(self) except *:
        """Reserve space for masks array."""
        if self._masks != NULL:
            return  # Already allocated
        
        if not self._indexing_enabled or self._n == 0:
            return
        
        cdef size_t total_bytes = <size_t>self._n * <size_t>self._mask_bytes_per_record
        self._masks = malloc(total_bytes)
        
        if self._masks == NULL:
            raise MemoryError("Unable to allocate masks array")
    
    cdef void _build_masks(self) except *:
        """Build bitmasks and map indexes for all records."""
        cdef bint do_masks = self._indexing_enabled
        cdef bint do_maps = len(self._map_indexes) > 0
        
        if do_masks:
            self._reserve_masks()
        
        cdef Py_ssize_t i
        cdef size_t off
        cdef int reclen
        cdef const uint8_t[:] buf = self._mm
        cdef const uint8_t* rec_ptr
        cdef bytes tag_bytes, raw
        cdef char subc
        cdef void* mask_ptr
        cdef str field_spec
        cdef list analyzer_list
        cdef str value_str
        
        # Parse field specs once (only needed if building masks)
        cdef list parsed_specs = []
        if do_masks:
            for spec in self._index_fields:
                if "$" in spec:
                    tag, sub = spec.split("$", 1)
                    parsed_specs.append((tag.encode("ascii"), ord(sub[0]) if sub else 0, spec))
                else:
                    parsed_specs.append((spec.encode("ascii"), 0, spec))
        
        # Zero out all masks if using indexing
        if do_masks:
            memset(self._masks, 0, <size_t>self._n * <size_t>self._mask_bytes_per_record)

        # Build indexes for each record
        for i in range(self._n):
            off = self._offsets[i]
            rec_ptr = &buf[off]
            reclen = self._lengths[i]
            
            # Get pointer to this record's mask (if fuzzy indexing enabled)
            if do_masks:
                mask_ptr = <void*>(<char*>self._masks + i * self._mask_bytes_per_record)
            
            # Update fuzzy mask from each indexed field (if enabled)
            if do_masks:
                for tag_bytes, subc, field_spec in parsed_specs:
                    occurrences = get_subfields(rec_ptr, reclen, tag_bytes, subc)
                    if occurrences:
                        # Union all characters from all occurrences into mask
                        for raw_val in occurrences:
                            self._update_mask_fast(mask_ptr, <const uint8_t*>raw_val, len(raw_val))
            
            # Build map indexes (hash map: value -> [record_idx, ...])
            if do_maps:
                for name, index_meta in self._indexes.items():
                    if index_meta["mode"] == "map":
                        tag_bytes = index_meta["tag_bytes"]
                        subc = index_meta["subfield_byte"]
                        field_spec = index_meta["field_spec"]

                        occurrences = get_subfields(rec_ptr, reclen, tag_bytes, subc)
                        if occurrences:
                            seen_local = set()
                            for raw_val in occurrences:
                                value_str = raw_val.decode('utf-8', errors='replace')
                                if value_str in seen_local:
                                    continue
                                seen_local.add(value_str)
                                if value_str not in self._map_indexes[name]:
                                    self._map_indexes[name][value_str] = []
                                self._map_indexes[name][value_str].append(i)

        # Report efficiency if using custom charset
        if self._charset and do_masks:
            total_mb = (self._n * self._mask_bytes_per_record) / (1024.0 * 1024.0)
            bits_needed = self._bits_per_mask
            # Calculate efficiency
            bytes_optimal = (bits_needed + 7) // 8
            efficiency = (bytes_optimal / self._mask_bytes_per_record) * 100.0 if self._mask_bytes_per_record > 0 else 100.0
            type_name = ""
            if self._mask_bytes_per_record == 1:
                type_name = "uint8_t"
            elif self._mask_bytes_per_record == 2:
                type_name = "uint16_t"
            elif self._mask_bytes_per_record == 4:
                type_name = "uint32_t"
            elif self._mask_bytes_per_record == 8:
                type_name = "uint64_t"
            else:
                type_name = f"{self._mask_bytes_per_record}-byte"
            print(f"Custom charset '{self._charset}': {bits_needed} bits → {type_name} "
                  f"({self._mask_bytes_per_record} bytes/record, {efficiency:.1f}% efficient, {total_mb:.2f} MB total)")
    
    cdef inline void _update_mask_fast(self, void* mask, const uint8_t* data, Py_ssize_t n) nogil:
        """Fast mask update - dispatches to appropriate type."""
        global _mask_type
        cdef Py_ssize_t i
        cdef int bit
        cdef uint8_t* m8
        cdef uint16_t* m16
        cdef uint32_t* m32
        cdef uint64_t* m64
        
        if _mask_type == 0:  # uint8_t
            m8 = <uint8_t*>mask
            for i in range(n):
                bit = _map_char(data[i])
                _mask_set_u8(m8, bit)
        elif _mask_type == 1:  # uint16_t
            m16 = <uint16_t*>mask
            for i in range(n):
                bit = _map_char(data[i])
                _mask_set_u16(m16, bit)
        elif _mask_type == 2:  # uint32_t
            m32 = <uint32_t*>mask
            for i in range(n):
                bit = _map_char(data[i])
                _mask_set_u32(m32, bit)
        elif _mask_type == 3:  # uint64_t
            m64 = <uint64_t*>mask
            for i in range(n):
                bit = _map_char(data[i])
                _mask_set_u64(m64, bit)
        else:  # multi-uint64
            m64 = <uint64_t*>mask
            for i in range(n):
                bit = _map_char(data[i])
                _mask_set_multi(m64, bit)
    
    cdef inline void _build_query_mask(self, void* query_mask, const uint8_t* text, Py_ssize_t n) nogil:
        """Build query mask from text - dispatches to appropriate type."""
        global _mask_type
        cdef Py_ssize_t i
        cdef int bit
        cdef uint8_t* m8
        cdef uint16_t* m16
        cdef uint32_t* m32
        cdef uint64_t* m64
        cdef int num_lanes
        cdef int j
        
        if _mask_type == 0:  # uint8_t
            m8 = <uint8_t*>query_mask
            m8[0] = 0
            for i in range(n):
                bit = _map_char(text[i])
                _mask_set_u8(m8, bit)
        elif _mask_type == 1:  # uint16_t
            m16 = <uint16_t*>query_mask
            m16[0] = 0
            for i in range(n):
                bit = _map_char(text[i])
                _mask_set_u16(m16, bit)
        elif _mask_type == 2:  # uint32_t
            m32 = <uint32_t*>query_mask
            m32[0] = 0
            for i in range(n):
                bit = _map_char(text[i])
                _mask_set_u32(m32, bit)
        elif _mask_type == 3:  # uint64_t
            m64 = <uint64_t*>query_mask
            m64[0] = 0
            for i in range(n):
                bit = _map_char(text[i])
                _mask_set_u64(m64, bit)
        else:  # multi-uint64
            m64 = <uint64_t*>query_mask
            num_lanes = self._mask_bytes_per_record // 8
            for j in range(num_lanes):
                m64[j] = 0
            for i in range(n):
                bit = _map_char(text[i])
                _mask_set_multi(m64, bit)
    
    cpdef list search(self, str field_spec, str text):
        """
        Search for records containing text in the specified field.

        Automatically uses index if available (map or mask mode), otherwise performs
        sequential scan through all records.

        Args:
            field_spec: Field specification (e.g., "245$a", "001", "020$a")
            text: Text to search for (exact match for map mode, substring for mask/sequential)
        Returns:
            List of record indices (may be empty, single item, or multiple for collisions)
        Examples:
            # With index (fast)
            results = reader.search("245$a", "music")
            # Without index (sequential scan)
            results = reader.search("260$a", "New York")
        """
        if not self._index_built:
            raise RuntimeError("Index not built. Call .build_index() first.")

        # Look for an index with matching field_spec
        cdef str index_name = None
        cdef dict index_meta
        for name, meta in self._indexes.items():
            if meta["field_spec"] == field_spec:
                index_name = name
                index_meta = meta
                break

        # If index found, use it
        if index_name is not None:
            mode = index_meta["mode"]

            if mode == "map":
                return self._map_indexes[index_name].get(text, [])
            else:  # mode == "mask"
                # Get field spec components from metadata
                tag_bytes = index_meta["tag_bytes"]
                subfield_byte = index_meta["subfield_byte"]
                return self._search_with_mask(tag_bytes, subfield_byte, text)

        # No index found - perform sequential scan
        return self._search_sequential(field_spec, text)

    cdef list _search_with_mask(self, bytes tag_bytes, char subfield_byte, str text):
        """Perform mask-based (bitmask) search."""
        global _mask_type
        cdef Py_ssize_t i
        cdef bint pass_mask
        cdef list out = []
        cdef bytes text_bytes = text.encode("utf-8")
        cdef const uint8_t[:] buf = self._mm
        cdef const uint8_t* rec_ptr
        cdef int reclen
        cdef void* rec_mask_ptr
        cdef void* query_mask_ptr

        # Allocate query mask with appropriate size
        cdef uint8_t query_mask_u8
        cdef uint16_t query_mask_u16
        cdef uint32_t query_mask_u32
        cdef uint64_t query_mask_u64
        cdef uint64_t query_mask_multi[32]  # Support up to 2048 bits

        # Build query mask
        if _mask_type == 0:
            query_mask_ptr = &query_mask_u8
        elif _mask_type == 1:
            query_mask_ptr = &query_mask_u16
        elif _mask_type == 2:
            query_mask_ptr = &query_mask_u32
        elif _mask_type == 3:
            query_mask_ptr = &query_mask_u64
        else:
            query_mask_ptr = query_mask_multi

        # Build the query mask for the search text
        self._build_query_mask(query_mask_ptr, <const uint8_t*>text_bytes, len(text_bytes))

        # Search through records
        for i in range(self._n):
            # Get pointer to this record's mask
            rec_mask_ptr = <void*>(<char*>self._masks + i * self._mask_bytes_per_record)

            # Check bitmask prefilter
            pass_mask = self._check_mask_match(rec_mask_ptr, query_mask_ptr)

            if not pass_mask:
                continue

            # Passed filter - do exact verification
            rec_ptr = &buf[self._offsets[i]]
            reclen = self._lengths[i]

            if record_contains(rec_ptr, reclen, tag_bytes, subfield_byte, text_bytes):
                out.append(i)

        return out

    cdef list _search_sequential(self, str field_spec, str text):
        """Perform sequential scan (no index) through all records."""
        cdef bytes tag_bytes
        cdef char subfield_byte

        # Parse field spec
        if "$" in field_spec:
            parts = field_spec.split("$", 1)
            tag_bytes = parts[0].encode("ascii")
            subfield_byte = ord(parts[1][0]) if parts[1] else 0
        else:
            tag_bytes = field_spec.encode("ascii")
            subfield_byte = 0

        cdef bytes text_bytes = text.encode("utf-8")
        cdef const uint8_t[:] buf = self._mm
        cdef const uint8_t* rec_ptr
        cdef int reclen
        cdef list out = []
        cdef Py_ssize_t i

        # Scan through all records
        for i in range(self._n):
            rec_ptr = &buf[self._offsets[i]]
            reclen = self._lengths[i]

            if record_contains(rec_ptr, reclen, tag_bytes, subfield_byte, text_bytes):
                out.append(i)

        return out
    
    cdef inline bint _check_mask_match(self, void* rec_mask, void* query_mask) nogil:
        """Check if record mask contains all bits from query mask."""
        global _mask_type
        cdef uint8_t* rm8
        cdef uint8_t* qm8
        cdef uint16_t* rm16
        cdef uint16_t* qm16
        cdef uint32_t* rm32
        cdef uint32_t* qm32
        cdef uint64_t* rm64
        cdef uint64_t* qm64
        cdef int num_lanes, j
        
        if _mask_type == 0:  # uint8_t
            rm8 = <uint8_t*>rec_mask
            qm8 = <uint8_t*>query_mask
            return (rm8[0] & qm8[0]) == qm8[0]
        elif _mask_type == 1:  # uint16_t
            rm16 = <uint16_t*>rec_mask
            qm16 = <uint16_t*>query_mask
            return (rm16[0] & qm16[0]) == qm16[0]
        elif _mask_type == 2:  # uint32_t
            rm32 = <uint32_t*>rec_mask
            qm32 = <uint32_t*>query_mask
            return (rm32[0] & qm32[0]) == qm32[0]
        elif _mask_type == 3:  # uint64_t
            rm64 = <uint64_t*>rec_mask
            qm64 = <uint64_t*>query_mask
            return (rm64[0] & qm64[0]) == qm64[0]
        else:  # multi-uint64
            rm64 = <uint64_t*>rec_mask
            qm64 = <uint64_t*>query_mask
            num_lanes = self._mask_bytes_per_record // 8
            for j in range(num_lanes):
                if (rm64[j] & qm64[j]) != qm64[j]:
                    return 0
            return 1
    
    @property
    def indexing_enabled(self):
        """Check if indexing is enabled."""
        return self._indexing_enabled
