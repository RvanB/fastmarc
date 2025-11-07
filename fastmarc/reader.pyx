# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True

import io
import mmap
import pymarc
cimport cython

from libc.stdlib cimport malloc, realloc, free
from libc.stdint cimport uint8_t, uint16_t, uint32_t, uint64_t
from libc.stddef cimport size_t
from libc.string cimport memset
from cpython.list cimport PyList_New, PyList_SET_ITEM
from cpython.long cimport PyLong_FromSize_t

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

cdef bytes get_subfield(const unsigned char* rec_ptr, Py_ssize_t reclen,
                        bytes tag, char subfield):
    """Extract a specific subfield from a MARC record."""
    cdef const unsigned char* tag3 = <const unsigned char*> tag
    cdef int length = 0
    cdef const unsigned char* ptr = _find_subfield_ptr(rec_ptr, reclen, tag3, subfield, &length)
    
    if ptr == NULL:
        return b""
    return <bytes> ptr[:length]

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

# ============================================================================
# Bitmask indexing helpers
# ============================================================================

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
    
    # Optional indexing support
    cdef bint _indexing_enabled
    cdef void* _masks  # Type depends on _mask_bytes_per_record
    cdef int _mask_bytes_per_record  # Bytes per mask (1, 2, 4, 8, or multiples of 8)
    cdef int _bits_per_mask  # Total bits needed
    cdef list _index_fields
    cdef str _charset  # Custom charset (if any)

    def __cinit__(self, fp, enable_indexing=False, index_fields=None, mask_lanes=None, 
                  charset=None, **kwargs):
        self.fp = fp
        self._mm = None
        self._offsets = <size_t*> NULL
        self._lengths = <int*> NULL
        self._n = 0
        self._cap = 0
        self._i = 0
        
        # Indexing
        self._indexing_enabled = enable_indexing
        self._masks = NULL
        self._charset = charset
        
        if enable_indexing:
            if charset is not None:
                # Custom charset - calculate minimum bits needed
                charset_len = len(set(charset))
                self._bits_per_mask = charset_len + 1  # +1 for fallback
            elif mask_lanes is not None:
                # Explicit mask_lanes provided (legacy parameter)
                self._bits_per_mask = mask_lanes * 64
            else:
                # Default: 256 bits
                self._bits_per_mask = 256
                
            # Calculate bytes per mask based on bits needed
            if self._bits_per_mask <= 8:
                self._mask_bytes_per_record = 1  # uint8_t
            elif self._bits_per_mask <= 16:
                self._mask_bytes_per_record = 2  # uint16_t
            elif self._bits_per_mask <= 32:
                self._mask_bytes_per_record = 4  # uint32_t
            elif self._bits_per_mask <= 64:
                self._mask_bytes_per_record = 8  # uint64_t
            else:
                # Multi-uint64: round up to multiple of 8
                self._mask_bytes_per_record = ((self._bits_per_mask + 63) // 64) * 8
        else:
            self._bits_per_mask = 0
            self._mask_bytes_per_record = 0
            
        self._index_fields = list(index_fields) if index_fields else ["001", "245$a"]

    def __init__(self, fp, enable_indexing=False, index_fields=None, mask_lanes=None,
                 charset=None, **kwargs):
        global _custom_char_map, _custom_fallback_bit
        
        # Setup custom character mapping if provided
        cdef int i
        cdef int ch_byte
        
        if charset is not None and enable_indexing:
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
        else:
            # Reset to use default mapping
            _custom_fallback_bit = -1
            _custom_char_map[0] = -1
        
        self._build_index()
        if self._indexing_enabled:
            self._build_masks()

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
        """Return the pymarc.Record at the given index."""
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


    def __len__(self):
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
        """Build bitmasks for all indexed records."""
        if not self._indexing_enabled:
            return
        
        self._reserve_masks()
        
        cdef Py_ssize_t i
        cdef size_t off
        cdef int reclen
        cdef const uint8_t[:] buf = self._mm
        cdef const uint8_t* rec_ptr
        cdef bytes tag_bytes, raw
        cdef char subc
        cdef void* mask_ptr
        
        # Parse field specs once
        cdef list parsed_specs = []
        for spec in self._index_fields:
            if "$" in spec:
                tag, sub = spec.split("$", 1)
                parsed_specs.append((tag.encode("ascii"), ord(sub[0]) if sub else 0))
            else:
                parsed_specs.append((spec.encode("ascii"), 0))
        
        # Zero out all masks
        memset(self._masks, 0, <size_t>self._n * <size_t>self._mask_bytes_per_record)
        
        # Build masks for each record
        for i in range(self._n):
            off = self._offsets[i]
            rec_ptr = &buf[off]
            reclen = self._lengths[i]
            
            # Get pointer to this record's mask
            mask_ptr = <void*>(<char*>self._masks + i * self._mask_bytes_per_record)
            
            # Update mask from each indexed field
            for tag_bytes, subc in parsed_specs:
                raw = get_subfield(rec_ptr, reclen, tag_bytes, subc)
                if raw:
                    self._update_mask_fast(mask_ptr, <const uint8_t*>raw, len(raw))
        
        # Report efficiency if using custom charset
        if self._charset:
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
    
    cpdef list search(self, str tag, str subfield, str text):
        """
        Search for records containing text in field/subfield.
        Requires enable_indexing=True when creating the reader.
        
        Args:
            tag: 3-character MARC tag (e.g., "245")
            subfield: Subfield code (e.g., "a") or empty string for entire field
            text: Substring to search for
            
        Returns:
            List of record indices (use get_record(idx) to retrieve)
        """
        if not self._indexing_enabled:
            raise RuntimeError("Indexing not enabled. Create reader with enable_indexing=True")
        
        global _mask_type
        cdef Py_ssize_t i
        cdef bint pass_mask
        cdef list out = []
        cdef bytes tag_bytes = tag.encode("ascii")
        cdef char subfield_byte = ord(subfield[0]) if subfield else 0
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
