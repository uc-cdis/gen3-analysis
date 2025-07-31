import functools
import gzip
import io
import os

import pytest

from bamrest.slicing import coordinate
from tests import TEST_DIR

BAM_PATH = os.path.join(TEST_DIR, "data/slice_testing.bam")
BAI_PATH = os.path.join(TEST_DIR, "data/slice_testing.bam.bai")
EXPECTED_OUTPUT_CHR1 = os.path.join(TEST_DIR, "data/slice_testing.expected_chr1.bam")
EXPECTED_OUTPUT_UNMAPPED = os.path.join(TEST_DIR, "data/slice_testing.expected_unmapped.bam")


@pytest.mark.parametrize(
    "regions,unmapped,expected",
    [
        ([("unmapped", None, None)], True, EXPECTED_OUTPUT_UNMAPPED),
        ([("chr1", None, None)], False, EXPECTED_OUTPUT_CHR1),
    ],
)
def test_slicing(request, regions, unmapped, expected):
    """
    request: pytest built-in
    regions: list of 'unmapped' or chromosome (name, x, y) tuples to return from bam
    unmapped: Bool
    expected: path to expected output

    NOTE: Reads in whole file in memory, should only be used for small files.
    But we're unit testing, so this seems okay to me. :)
    """

    expected_chunks = get_chunks(gzip.GzipFile(expected))
    actual_chunks = get_slice(regions, unmapped)

    assert len(actual_chunks) == len(expected_chunks)
    for exp_r, new_r in zip(expected_chunks, actual_chunks):
        assert exp_r == new_r


# helper functions
def get(fil, offset=0):
    fil.seek(offset)
    return fil


def get_slice(regions, unmapped):

    # generate slices
    with open(BAM_PATH, "rb") as bam, open(BAI_PATH, "rb") as bai:
        slices = coordinate(
            functools.partial(get, bam),
            functools.partial(get, bai),
            regions,
            include_unmapped=unmapped,
        )

        # compress with gzip to compare to expected
        compressed = io.BytesIO()
        for s in slices:
            compressed.write(s)
        compressed.flush()
        decompressor = gzip.GzipFile(fileobj=io.BytesIO(compressed.getvalue()), mode="rb")
        compressed.close()

        return get_chunks(decompressor)


def get_chunks(file_obj):
    """
    The bam consists of header, remainder, and chunks sections.
    """

    from bamrest import header, record

    def _process_header(file_obj):
        exp_header = None
        assert file_obj.read(4) == b"BAM\x01"
        exp_header_length = header.deserialize("<I", file_obj)
        buffered = file_obj.read(exp_header_length)
        assert len(buffered) == exp_header_length
        exp_header = header.bytes2header(buffered)
        return exp_header

    def _process_remainder(file_obj, header):
        remainder = 4 + (8 * len(header["SQ"])) + sum(len(sq["SN"]) + 1 for sq in header["SQ"])
        file_obj.read(remainder)

    def _process_chunks(file_obj):
        return [i[3] for i in record.file_to_record_coordinate_tuples(file_obj)]

    file_header = _process_header(file_obj)
    _process_remainder(file_obj, file_header)
    return _process_chunks(file_obj)
