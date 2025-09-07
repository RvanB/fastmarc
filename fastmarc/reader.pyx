# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True
import io
import mmap
import pymarc
cimport cython

cdef class MARCReader:
    cdef public object fp
    cdef list _seek_map
    cdef Py_ssize_t _i

    def __cinit__(self, fp, **kwargs):
        self.fp = fp
        self._seek_map = []
        self._i = 0

    def __init__(self, fp, **kwargs):
        self._build_seek_map()

    cdef void _build_seek_map(self):
        """
        Build a list of starting byte offsets for each MARC record.
        Prefer a single pass via mmap; fall back to streaming if needed.
        """
        cdef Py_ssize_t i = 0
        cdef Py_ssize_t size = 0
        cdef int L = 0
        cdef object mm = None
        cdef const unsigned char[:] buf  # typed memoryview (zero-copy on mmap)

        # ---- fast path: mmap + typed memoryview ----
        try:
            fileno = self.fp.fileno()
            mm = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)

            # create a typed memoryview over the mmap (no copy)
            buf = mm
            size = buf.shape[0]

            i = 0
            while i + 5 <= size:
                # parse 5-digit ASCII length directly from the buffer
                L = ((buf[i]   - 48) * 10000 +
                     (buf[i+1] - 48) * 1000  +
                     (buf[i+2] - 48) * 100   +
                     (buf[i+3] - 48) * 10    +
                      buf[i+4] - 48)

                if L <= 0 or i + L > size:
                    break

                self._seek_map.append(i)
                i += L

            mm.close()
            return

        except Exception:
            # ensure mmap is closed if it was opened but an error occurred later
            try:
                if mm is not None:
                    mm.close()
            except Exception:
                pass
            # fall through to streaming fallback

        # ---- fallback: streaming read ----
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
            self._seek_map.append(pos)
            self.fp.seek(L - 5, io.SEEK_CUR)
        self.fp.seek(0, io.SEEK_SET)

    def __iter__(self):
        self._i = 0
        return self

    def __next__(self):
        if self._i >= len(self._seek_map):
            raise StopIteration

        pos = self._seek_map[self._i]
        self.fp.seek(pos, io.SEEK_SET)
        head = self.fp.read(5)
        if not head:
            raise StopIteration
        L = int(head)
        data_rest = self.fp.read(L - 5)

        self._i += 1
        return pymarc.Record(data=head + data_rest)

    def get_seek_map(self):
        return list(self._seek_map)
