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
    
    # Explicit indexing (inactive until .index() is called)
    cdef bint _indexing_enabled        # Set True when user calls .index()
    cdef bint _index_built             # Whether index + hooks have run
    cdef void* _masks                  # Allocated after indexing enabled
    cdef int _mask_bytes_per_record    # Bytes per mask (computed at .index())
    cdef int _bits_per_mask            # Bits required (computed at .index())
    cdef list _index_fields            # Fields used for fuzzy bitmask indexing
    cdef str _charset                  # Optional custom charset
    cdef dict _field_hooks             # Field spec -> list of hook callables
    cdef list _multi_field_hooks       # (list[field_specs], hook_callable) per record
    
    # Exact indexing (hash map: field_spec -> {value: [idx, idx, ...]})
    cdef public dict _exact_indexes           # field_spec -> dict of value -> list of indices
    cdef list _exact_fields            # List of (tag_bytes, subfield_byte, field_spec) for exact indexing

    def __cinit__(self, fp, **kwargs):
        self.fp = fp
        self._mm = None
        self._offsets = <size_t*> NULL
        self._lengths = <int*> NULL
        self._n = 0
        self._cap = 0
        self._i = 0

        # Indexing inactive until .index() invoked
        self._indexing_enabled = False
        self._index_built = False
        self._masks = NULL
        self._charset = None
        self._bits_per_mask = 0
        self._mask_bytes_per_record = 0
        self._index_fields = []  # Will be populated via .index(*fields)
        self._exact_fields = []  # Will be populated via .add_index(field, fuzzy=False)

    def __init__(self, fp, **kwargs):
        # Initialize field hooks dict
        self._field_hooks = {}
        self._multi_field_hooks = []
        self._exact_indexes = {}  # field_spec -> {value: [idx1, idx2, ...]}

    # No automatic indexing: user must call .index() to build & run hooks.

    def add_index(self, field_spec, fuzzy=None):
        """
        Register a field for indexing (fuzzy bitmask or exact hash map).
        
        Args:
            field_spec: Field specification (e.g., "001", "245$a", "020$a")
            fuzzy: True for bitmask (substring search), False for exact (hash map lookup)
                   If None, auto-detect: False for control fields (001-009), True otherwise
        
        Returns:
            self (for method chaining)
        
        Examples:
            # Auto-detect: control fields are exact, data fields are fuzzy
            reader = (MARCReader(fp)
                .add_index("001")           # Exact (auto-detected)
                .add_index("245$a")         # Fuzzy (auto-detected)
                .index())
            
            # Explicit fuzzy/exact:
            reader = (MARCReader(fp)
                .add_index("020$a", fuzzy=False)  # ISBN: exact lookup
                .add_index("650$a", fuzzy=True)   # Subjects: substring search
                .index())
        """
        if self._index_built:
            raise RuntimeError("Cannot add indexes after .index() has been called")
        
        # Parse field spec to determine tag
        if "$" in field_spec:
            tag, subfield = field_spec.split("$", 1)
            tag_bytes = tag.encode("ascii")
            subfield_byte = ord(subfield[0]) if subfield else 0
        else:
            tag = field_spec
            tag_bytes = tag.encode("ascii")
            subfield_byte = 0
        
        # Auto-detect fuzzy vs exact based on tag
        if fuzzy is None:
            fuzzy = not _is_control_tag(<const unsigned char*>tag_bytes)
        
        if fuzzy:
            # Add to fuzzy/bitmask indexes
            if field_spec not in self._index_fields:
                self._index_fields.append(field_spec)
        else:
            # Add to exact/hash map indexes
            if field_spec not in self._exact_indexes:
                self._exact_indexes[field_spec] = {}
            # Store parsed spec for later use during indexing
            spec_tuple = (tag_bytes, subfield_byte, field_spec)
            if spec_tuple not in self._exact_fields:
                self._exact_fields.append(spec_tuple)
        
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
        Build the record index and run field hooks.
        Must be invoked before using search(), len(), get_record(), or relying
        on hooks. Streaming iteration without calling .index() is supported
        but will NOT execute hooks.

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
    
    def index(self, charset=None):
        """
        Build the index and run all field hooks.
        Explicit call required – no automatic build on iteration.

        Args:
            charset: Optional custom character set for fuzzy indexing

        Example:
            # Basic usage with hooks
            subjects = FieldCounter()
            reader = (MARCReader(fp)
                .hook("650$a", subjects)
                .index())
            print(subjects.counts.most_common(10))

            # With indexing and custom charset
            reader = (MARCReader(fp)
                .add_index("245$a")
                .index(charset="abcdefghijklmnopqrstuvwxyz0123456789"))

        Returns:
            self (for method chaining)
        """
        return self.build_index(charset=charset)

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

    cdef void _build_index(self):
        cdef Py_ssize_t i = 0
        cdef Py_ssize_t size = 0
        cdef Py_ssize_t hint = 0
        cdef int L = 0
        cdef object mm = None
        cdef const uint8_t[:] buf

        try:
            fileno = self.fp.fileno()
            mm = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
            buf = mm
            size = buf.shape[0]

            # pre-reserve some capacity (rough heuristic)
            hint = size // 1024
            if hint > 0:
                self._reserve(hint)

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

                self._append(<size_t>i, L)
                i += L

            self._mm = mm   # keep mmap alive for zero-copy iteration
            return

        except Exception:
            try:
                if mm is not None:
                    mm.close()
            except Exception:
                pass
            self._mm = None

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
            self._append(<size_t>pos, L)
            self.fp.seek(L - 5, io.SEEK_CUR)
        self.fp.seek(0, io.SEEK_SET)

    def __iter__(self):
        # Streaming iteration without indexing/hook execution (unless .index() has been called).
        self._i = 0
        return self

    def __next__(self):
        cdef Py_ssize_t idx = self._i
        cdef size_t pos
        cdef int L
        cdef Py_ssize_t p, q

        if idx >= self._n:
            raise StopIteration
        self._i = idx + 1

        pos = self._offsets[idx]
        L = self._lengths[idx]

        if self._mm is not None:
            p = <Py_ssize_t>pos
            q = p + <Py_ssize_t>L
            raw = bytes(self._mm[p:q])
        else:
            self.fp.seek(pos, io.SEEK_SET)
            raw = self.fp.read(L)

        return pymarc.Record(data=raw)

    
    def get_record(self, Py_ssize_t idx):
        """Return the pymarc.Record at the given index (requires prior .index())."""
        if not self._index_built:
            raise RuntimeError("Index not built. Call .index() first.")
            
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

    cpdef dict get_exact_index(self, str field_spec):
        """Return the exact index map for a field (value -> list of record indices).

        Args:
            field_spec: Field specification (e.g., "001", "245$a")

        Returns:
            dict mapping field values (str) to list of record indices. Empty dict if
            field was not indexed exactly or index() not yet called.
        """
        if not self._index_built:
            return {}
        if field_spec in self._exact_indexes:
            return self._exact_indexes[field_spec]
        return {}


    def __len__(self):
        if not self._index_built:
            raise RuntimeError("Index not built. Call .index() first.")
        return self._n

    # ========================================================================
    # Indexing Support
    # ========================================================================

    def hook(self, field_specs, hook_func):
        """Register a hook (single or multi-field) that always receives lists of values.

        Semantics:
          * Single field spec: The hook is invoked once per record with a list of all
            occurrences (possibly empty) of that field/subfield.
          * Multiple field specs: The hook is invoked once per record with a dict:
                { field_spec: [list of all occurrences], ... }
            Only specs with at least one occurrence may be included (to reduce noise).

        Args:
            field_specs: str (e.g., "245$a") or iterable of str (e.g., ["008", "264$c"]).
            hook_func:  Callable. For single spec it receives list[str]; for multiple
                        specs it receives dict[str, list[str]].

        Returns:
            self
        """
        # Normalize input
        if isinstance(field_specs, str):
            specs_list = [field_specs]
        else:
            specs_list = list(field_specs)
        if not specs_list:
            raise ValueError("field_specs must contain at least one field spec")
        if len(specs_list) == 1:
            spec = specs_list[0]
            if spec not in self._field_hooks:
                self._field_hooks[spec] = []
            self._field_hooks[spec].append(hook_func)
        else:
            self._multi_field_hooks.append((specs_list, hook_func))
        return self

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
        """Run field hooks and (if enabled) build bitmasks and exact indexes for all records."""
        cdef bint do_masks = self._indexing_enabled
        cdef bint do_exact = len(self._exact_fields) > 0
        
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
        
        # Also parse hook specs (might not be in index_fields)
        cdef list hook_specs = []
        for spec in self._field_hooks.keys():
            if "$" in spec:
                tag, sub = spec.split("$", 1)
                hook_specs.append((tag.encode("ascii"), ord(sub[0]) if sub else 0, spec))
            else:
                hook_specs.append((spec.encode("ascii"), 0, spec))
        
        # Zero out all masks if using indexing
        if do_masks:
            memset(self._masks, 0, <size_t>self._n * <size_t>self._mask_bytes_per_record)
        
        # Pre-parse specs used by multi-field hooks (may overlap with others)
        multi_parsed = {}
        if self._multi_field_hooks:
            for spec_list, hook in self._multi_field_hooks:
                for spec in spec_list:
                    if spec not in multi_parsed:
                        if "$" in spec:
                            tag, sub = spec.split("$", 1)
                            multi_parsed[spec] = (tag.encode("ascii"), ord(sub[0]) if sub else 0)
                        else:
                            multi_parsed[spec] = (spec.encode("ascii"), 0)

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
                    # Single-field hooks: pass list of decoded values (may be empty)
                    if field_spec in self._field_hooks:
                        hook_list = self._field_hooks[field_spec]
                        values_list = [b.decode('utf-8', errors='replace') for b in occurrences]
                        for hook in hook_list:
                            hook(values_list)
            
            # Build exact indexes (hash map: value -> [record_idx, ...])
            if do_exact:
                for tag_bytes, subc, field_spec in self._exact_fields:
                    occurrences = get_subfields(rec_ptr, reclen, tag_bytes, subc)
                    if occurrences:
                        seen_local = set()
                        for raw_val in occurrences:
                            value_str = raw_val.decode('utf-8', errors='replace')
                            if value_str in seen_local:
                                continue
                            seen_local.add(value_str)
                            if value_str not in self._exact_indexes[field_spec]:
                                self._exact_indexes[field_spec][value_str] = []
                            self._exact_indexes[field_spec][value_str].append(i)
                    # Execute hooks for exact-indexed fields
                    if field_spec in self._field_hooks:
                        hook_list = self._field_hooks[field_spec]
                        values_list = [b.decode('utf-8', errors='replace') for b in occurrences]
                        for hook in hook_list:
                            hook(values_list)
            
            # Process hook-only fields (not in index_fields or exact_fields)
            for tag_bytes, subc, field_spec in hook_specs:
                if field_spec not in self._index_fields:
                    # Check if it's also not in exact fields
                    is_exact = False
                    for etag, esubc, espec in self._exact_fields:
                        if espec == field_spec:
                            is_exact = True
                            break
                    if not is_exact:
                        occurrences = get_subfields(rec_ptr, reclen, tag_bytes, subc)
                        values_list = [b.decode('utf-8', errors='replace') for b in occurrences]
                        hook_list = self._field_hooks[field_spec]
                        for hook in hook_list:
                            hook(values_list)

            # Multi-field hooks (invoke once per record)
            if self._multi_field_hooks:
                # Collect lists of all occurrences for each requested spec
                values_cache_lists = {}
                for spec, parsed in multi_parsed.items():
                    tag_bytes = parsed[0]
                    subc = parsed[1]
                    occs = get_subfields(rec_ptr, reclen, tag_bytes, subc)
                    if occs:
                        values_cache_lists[spec] = [b.decode('utf-8', errors='replace') for b in occs]
                # Invoke each multi hook with subset relevant to it
                for spec_list, hook in self._multi_field_hooks:
                    submap = {spec: values_cache_lists[spec] for spec in spec_list if spec in values_cache_lists}
                    hook(submap)
        
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
        Search for records containing text in the specified field specification.
        
        Automatically uses exact (hash map) or fuzzy (bitmask) search based on
        how the field was indexed via add_index()/index().
        
        Args:
            field_spec: Field specification string (e.g., "245$a", "001", "020$a")
            text: Text to search for (exact match when exact-indexed, substring when fuzzy)
        Returns:
            List of record indices (may be empty, single item, or multiple for collisions)
        Examples:
            # Exact lookup (001 indexed exact)
            results = reader.search("001", "12345")
            # Fuzzy search (245$a indexed fuzzy)
            results = reader.search("245$a", "history")
        """
        if not self._index_built:
            raise RuntimeError("Index not built. Call .index() first.")
        
        # Check if exact index exists for this field
        if field_spec in self._exact_indexes:
            return self._exact_indexes[field_spec].get(text, [])
        
        # Otherwise perform fuzzy bitmask search
        if not self._indexing_enabled:
            raise RuntimeError("No fuzzy index built for this field. Use add_index() with fuzzy=True.")
        
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
                out.append(i)  # Return index, not offset
        
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
