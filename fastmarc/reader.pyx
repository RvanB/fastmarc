# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True

import io
import mmap
import pymarc
cimport cython

from libc.stdlib cimport malloc, realloc, free
from libc.stdint cimport uint8_t
from libc.stddef cimport size_t
from cpython.list cimport PyList_New, PyList_SET_ITEM
from cpython.long cimport PyLong_FromSize_t


cdef class MARCReader:
    """
    Fast MARC reader that:
      - Scans once to build C arrays of (offset, length) for each record.
      - Keeps an mmap alive (when possible) so iteration slices bytes directly
        from memory (no seeks/reads per record).
      - Yields pymarc.Record objects on iteration.

    Public API:
      - FastMARCReader(fp)
      - for rec in FastMARCReader(fp):  # rec is pymarc.Record
      - len(reader)                     # number of records
      - reader.get_seek_map()           # [offset0, offset1, ...]
      - reader.close()                  # releases mmap and C buffers
    """

    # Python attributes
    cdef public object fp
    cdef public object _mm

    # C-level index storage
    cdef size_t* _offsets
    cdef int*    _lengths          # MARC length <= 99999 fits in int
    cdef Py_ssize_t _n
    cdef Py_ssize_t _cap

    # iteration cursor
    cdef Py_ssize_t _i

    # ---- lifecycle ---------------------------------------------------------

    def __cinit__(self, fp, **kwargs):
        self.fp = fp
        self._mm = None
        self._offsets = <size_t*> NULL
        self._lengths = <int*> NULL
        self._n = 0
        self._cap = 0
        self._i = 0

    def __init__(self, fp, **kwargs):
        self._build_index()

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
        self._n = 0
        self._cap = 0

    def __dealloc__(self):
        self.close()

    # ---- internal: dynamic array helpers ----------------------------------

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

    # ---- indexing ----------------------------------------------------------

    cdef void _build_index(self):
        """
        Build arrays of (offset, length) for each MARC record.

        Fast path:
          - Use mmap + a typed memoryview; compute lengths from 5 ASCII digits.
          - Append (i, L) and advance i by L until exhausted.
          - Keep the mmap *open* for iteration (zero-seek, zero-read).

        Fallback:
          - Seek/stream once; read 5-byte leader per record to get L.
          - Append (pos, L) and seek forward L-5.
        """
        cdef Py_ssize_t i = 0
        cdef Py_ssize_t size = 0
        cdef Py_ssize_t hint = 0
        cdef int L = 0
        cdef object mm = None
        cdef const uint8_t[:] buf

        # ------- Try mmap fast path -------
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

        # ------- Fallback: sequential scan -------
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

    # ---- iteration ---------------------------------------------------------

    def __iter__(self):
        self._i = 0
        return self

    def __next__(self):
        """
        Return the next pymarc.Record.

        Using mmap:
          - bytes(self._mm[pos:pos+L]) → single copy from RAM.
        Fallback:
          - Single seek to pos + single read of L bytes.
        """
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

    # ---- helpers -----------------------------------------------------------

    def get_seek_map(self):
        """
        Return Python list of record start offsets.
        Built with C-API (PyList_New/SET_ITEM) for speed.
        """
        cdef Py_ssize_t n = self._n
        cdef object out = PyList_New(n)
        cdef Py_ssize_t j
        for j in range(n):
            PyList_SET_ITEM(out, j, PyLong_FromSize_t(self._offsets[j]))
        return out

    def __len__(self):
        return self._n
