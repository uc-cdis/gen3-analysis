import io
import math
import struct

from . import exceptions


def deserialize(fmt, f):
    return struct.unpack(fmt, f.read(struct.calcsize(fmt)))[0]


def reg2bin(beg, end):
    """
    Returns bin associated with the specified region.

    Beg inclusive.
    End exclusive.
    """
    # Based off the algorithm presented in:
    # https://samtools.github.io/hts-specs/SAMv1.pdf

    if beg >> 14 == (end - 1) >> 14:
        return int(((1 << 15) - 1) / 7 + (beg >> 14))
    if beg >> 17 == (end - 1) >> 17:
        return int(((1 << 12) - 1) / 7 + (beg >> 17))
    if beg >> 20 == (end - 1) >> 20:
        return int(((1 << 9) - 1) / 7 + (beg >> 20))
    if beg >> 23 == (end - 1) >> 23:
        return int(((1 << 6) - 1) / 7 + (beg >> 23))
    if beg >> 26 == (end - 1) >> 26:
        return int(((1 << 3) - 1) / 7 + (beg >> 26))

    return 0


def reg2bins(beg, end):
    """
    Generates bin ids which overlap the specified region.

    Beg inclusive.
    End exclusive.
    """
    # Based off the algorithm presented in:
    # https://samtools.github.io/hts-specs/SAMv1.pdf

    yield 0

    yield from range(1 + (beg >> 26), 1 + ((end - 1) >> 26) + 1)
    yield from range(9 + (beg >> 23), 9 + ((end - 1) >> 23) + 1)
    yield from range(73 + (beg >> 20), 73 + ((end - 1) >> 20) + 1)
    yield from range(585 + (beg >> 17), 585 + ((end - 1) >> 17) + 1)
    yield from range(4681 + (beg >> 14), 4681 + ((end - 1) >> 14) + 1)


def file2dict(bai):
    """
    Deserialize a BAI file to a dict.
    """
    bai_dict = {
        "references": [],
        "unplaced": None,
    }

    # NOTE We preload the whole file-like object into memory since we'll
    # be representing the whole thing in memory anyways, and we don't
    # necessarily know if the file-like object is performant under small
    # read sizes.
    buffered = io.BytesIO(bai.read())

    if buffered.read(4) != b"BAI\x01":
        raise exceptions.BAIFormatError("magic number not found")

    mapped_end = 0
    for i in range(deserialize("<i", buffered)):
        ref, region_mapped_end = file2ref(buffered)
        bai_dict["references"].append(ref)
        mapped_end = region_mapped_end if region_mapped_end > mapped_end else mapped_end

    try:
        bai_dict["unplaced"] = deserialize("<Q", buffered)
    except struct.error:
        # NOTE Unplaced / unmapped reads are optional.
        pass

    bai_dict["mapped_end"] = mapped_end
    return bai_dict


def file2ref(bai):
    """
    Deserialize a BAI file to a ref dict.
    """
    ref_dict = {
        "bins": {},
        "intervals": [],
        "unmapped_beg": None,
        "unmapped_end": None,
        "num_mapped": None,
        "num_unmapped": None,
    }

    region_mapped_end = 0

    for i in range(deserialize("<i", bai)):

        bin_dict = file2bin(bai)

        if bin_dict["id"] == 37450:
            ref_dict["unmapped_beg"] = bin_dict["chunks"][0][0]
            ref_dict["unmapped_end"] = bin_dict["chunks"][0][1]
            ref_dict["num_mapped"] = bin_dict["chunks"][1][0]
            ref_dict["num_unmapped"] = bin_dict["chunks"][1][1]
            continue

        chunk_end = bin_dict["chunks"][0][1]
        region_mapped_end = chunk_end if region_mapped_end < chunk_end else region_mapped_end

        ref_dict["bins"][bin_dict["id"]] = bin_dict

    for i in range(deserialize("<i", bai)):
        ref_dict["intervals"].append(file2interval(bai))

    return ref_dict, region_mapped_end


def file2bin(bai):
    """
    Deserialize a BAI file to a bin dict.
    """
    bin_dict = {
        "id": None,
        "chunks": [],
    }

    try:
        bin_dict["id"] = deserialize("<I", bai)
    except struct.error:
        raise exceptions.BAITruncatedError("unexpected EOF encountered")

    for i in range(deserialize("<i", bai)):
        bin_dict["chunks"].append(file2chunk(bai))

    return bin_dict


def file2chunk(bai):
    """
    Deserialize a BAI file to a chunk tuple.
    """
    try:
        return (
            deserialize("<Q", bai),
            deserialize("<Q", bai),
        )
    except struct.error:
        raise exceptions.BAITruncatedError("unexpected EOF encountered")


def file2interval(bai):
    """
    Deserialize a BAI file to an interval.
    """
    try:
        return deserialize("<Q", bai)
    except struct.error:
        raise exceptions.BAITruncatedError("unexpected EOF encountered")


def region_offset(bai_dict, ref, beg, end):
    """
    Returns the byte offset of the first chunk containing alignments within
    the specified region, and the offset within the chunk of the first
    record within the specified region.

    Raises ValueError if the region is not included in the bai dict.
    """
    linear = linear_offset(bai_dict, ref, beg)

    offset = None

    # Find the earliest chunk that overlaps the linear offset.
    bins = bai_dict["references"][ref]["bins"]
    for b in reg2bins(beg, end):

        # Skip any bins that weren't indexed - nothing in them.
        if b not in bins:
            continue

        for lower, upper in bins[b]["chunks"]:

            if upper < linear:
                continue

            offset = min(offset, lower) if offset is not None else lower

    if offset is None:
        raise ValueError("region not included in bai")

    # Convert and return the non-virtual offsets.
    return offset >> 16, offset & 0x0000FFFF


def linear_offset(bai_dict, ref, beg, end=None):
    """
    Returns the lower limit (virtual) file offset.
    """
    assert ref >= 0, "ref must be >= 0"
    assert beg >= 0, "beg must be >= 0"

    TILE_SIZE = 2**14

    # Divies the genome up into TILE_SIZE bp chunks.
    # Calculates the tile a bp corresponds to, looks up the offset.
    tile = int(math.floor(beg / TILE_SIZE))

    try:
        ref_dict = bai_dict["references"][ref]
    except IndexError:
        raise IndexError("reference %d" % ref)

    try:
        vir = ref_dict["intervals"][tile]
    except IndexError as err:
        vir = 0

    return vir
