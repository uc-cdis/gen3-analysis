import io
import struct

from . import gzip

BLOCK = 2**16


def compress(data, eof=False):
    """
    Generate BGZF blocks from data.
    """
    # Generate BGZF BLOCK blocks.
    blocks = (data[i : i + BLOCK] for i in range(0, len(data) + eof, BLOCK))

    for block in blocks:

        tmp = io.BytesIO()

        g = gzip.GzipFile(mode="wb", fileobj=tmp)

        g.write(block)
        g.close()

        bgzf = bytearray(tmp.getvalue())

        # Modify gzipped block to have extra fields.
        bgzf[3:4] = struct.pack("<B", 4)

        # Overwrite the date and extras fields.
        bgzf[4:8] = struct.pack("<L", 0)  # Date
        bgzf[8:9] = struct.pack("<B", 0)  # Extras

        # Add the Byte Count subfield.
        bgzf[9:10] += struct.pack("<HBBHH", 6, 66, 67, 2, len(bgzf) + 8 - 1)

        yield bytes(bgzf)


class BGZFWriter:
    """
    Utility class for writing BGZF blocks to file-like objects.
    """

    def __init__(self, filename=None, mode="wb", fileobj=None):
        self.handle = fileobj if fileobj is not None else open(filename, mode)

    def write(self, data):
        for block in compress(data):
            self.handle.write(block)

        return len(data)

    def close(self):
        for block in compress(b"", eof=True):
            self.handle.write(block)

    def __getattr__(self, attr):
        return getattr(self.handle, attr)


class BGZFBufferedWriter(io.BufferedWriter):
    """
    Utility class for writing buffered BGZF blocks to file-like objects.

    This utility class is capable of performing block packing. By default, this
    will occur as byte-packing in units of BLOCK chunks. Write calls may
    specify the pack=False flag to specify that the bytes being supplied are
    not to be split across BGZF block boundaries unless they are too large to
    be stored in a single BGZF block.
    """

    def __init__(self, filename=None, mode="wb", fileobj=None):
        self.buff = 0
        self.bgzf = BGZFWriter(filename=filename, mode=mode, fileobj=fileobj)

        return super().__init__(self.bgzf, BLOCK)

    def write(self, b, pack=True):
        self.buff += len(b)

        # Don't want packing.
        # Data can fit inside a single BGZF block.
        # Current block is too full to fit data without splitting.
        if not pack and len(b) <= BLOCK and self.buff > BLOCK:
            self.flush()
            self.buff = len(b)

        self.buff %= BLOCK

        return super().write(b)

    def flush(self):
        self.buff = 0

        return super().flush()
