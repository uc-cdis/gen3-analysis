import io
import struct

from . import exceptions


def bytes_to_cigar(b):
    """
    Convert half-encoded cigar ops to string representation.
    """
    CIG_ENC = "MIDNSHP=X"

    cigar = struct.unpack("<%dI" % int(len(b) / 4), b)

    # ENCODING: | op_length - 28 bits | op - 4 bits |
    return [(op >> 4, CIG_ENC[op & 0x0000000F]) for op in cigar]


def cigar_to_coverage(cigar):
    """
    Convert cigar bytes to record reference coverage.
    """
    # Only matches, deletions and skips contribute to reference coverage.
    return sum(op_len for op_len, op in cigar if op in "MDN=X")


def bytes_to_record(b):
    """
    Parses and returns a record coordinate tuple.
    """
    ref, pos = struct.unpack("<2i", b[4:12])

    nlen = struct.unpack("<B", b[12:13])[0]
    clen = struct.unpack("<H", b[16:18])[0]

    cigar_beg = 36 + nlen
    cigar_end = 36 + nlen + 4 * clen

    cigar = b[cigar_beg:cigar_end]
    cigar = bytes_to_cigar(cigar)

    coverage = cigar_to_coverage(cigar)

    # TODO double check base-1 vs base-0 here
    return ref, pos + 1, pos + coverage, b


def file_to_record_coordinate_tuple(bam):
    """
    Deserialize BAM record coordinate tuples from file-like object.
    Returns a tuple of the form: (ref, pos, end, record)
        :ref: - Numerical reference.
        :pos: - Left-most coordinate starting position, base-1.
        :end: - Right-most coordinate ending position, base-1.
        :record: - Record bytes.
    """
    try:
        record_length = struct.unpack("<I", bam.read(4))[0]
    except struct.error:
        raise EOFError()

    try:
        buffered = bam.read(record_length)
    except struct.error:
        raise exceptions.TruncatedError("unexpected EOF encountered")

    return bytes_to_record(struct.pack("<I", record_length) + buffered)


def file_to_record_coordinate_tuples(bam):
    """
    Deserialize BAM record coordinate tuples from file-like object.
    Generates tuples of the form: (ref, pos, end, record)
        :ref: - Numerical reference.
        :pos: - Left-most coordinate starting position, base-1.
        :end: - Right-most coordinate ending position, base-1.
        :record: - Record bytes.
    """
    while True:
        try:
            yield file_to_record_coordinate_tuple(bam)
        except EOFError:
            break
