import io
import logging
import struct

from . import gzip, header, index, record, writer


def merged(regions):
    """
    Merge coordinate sorted regions.
    """
    regions = iter(regions)
    try:
        prev_ref, prev_beg, prev_end = next(regions)
    except StopIteration:
        return

    for curr_ref, curr_beg, curr_end in regions:

        if curr_ref != prev_ref or curr_beg > prev_end:
            yield prev_ref, prev_beg, prev_end
            prev_ref = curr_ref
            prev_beg = curr_beg
            prev_end = curr_end
            continue

        if curr_beg < prev_end and curr_end > prev_end:
            prev_end = curr_end

    yield prev_ref, prev_beg, prev_end


def implicit_to_explicit_region(bam_head, ref, beg, end):
    """
    Converts an implicit region to an explicit region.
    """
    refs = [sq["SN"] for sq in bam_head["SQ"]]
    lens = {sq["SN"]: int(sq["LN"]) for sq in bam_head["SQ"]}

    if beg is None and end is not None:
        beg = lens[ref] - end
        end = lens[ref]

    end = end if end is not None else lens[ref]
    beg = beg if beg is not None else lens[ref] - end

    return refs.index(ref), beg, end


def implicit_to_explicit_regions(bam_head, regions):
    """
    Converts implicit regions to explicit regions.
    """
    return (implicit_to_explicit_region(bam_head, *r) for r in regions)


def get_chunk(bam_f, b_off, v_off):
    try:
        b = bam_f(b_off)
        # TODO to be replaced with BGZF inflater
        bam_inflater = gzip.GzipFile(fileobj=b)
        bam_inflater = io.BufferedReader(bam_inflater, 2**15)
        bam_inflater.seek(v_off)
        return record.file_to_record_coordinate_tuples(bam_inflater)
    except OSError:
        # TODO: log
        pass


def fetch_unmapped_records(bam_f, mapped_end):
    # gzip offset
    eb_offset = mapped_end >> 16

    # byte offset
    file_offset = mapped_end & 0x0000FFFF

    tuples = get_chunk(bam_f, eb_offset, file_offset)
    return tuples


def fetch_records(bam_f, bai_dict, regions, include_unmapped=False):
    """
    Return a generator of alignment records for a given bam, bai and regions.

    :param bam_f: Function taking an integer offset value and returning a
    file-like object for a BAM format file positioned at the given offset.
    The file-like object must have a 'read' function that returns bytes.

    :param bai_dict: parsed bai as python dict

    :param regions: 1-based coordinate range tuples. Must be of the form
    (int, int, int). Either int may be substituted with a None type. This
    semanticly matches the (suffix-)byte-range-spec of RFC7233.

        region description: (ref, beg, end)
            ref - Reference sequence.
            beg - Position of first base in region.
            end - Position of last base in region.

    :param include_unmapped: include unmapped unplaced reads

    A key may be provided to specify the desired ordering of references.

    Guarantees that each record will be returned at most once.
    """
    # We track the last region fetched, using the fact that these will be
    # sorted in some order to guarantee that we don't return duplicates.
    prev = False
    prev_ref = None
    prev_beg = None
    prev_end = None

    for curr_ref, curr_beg, curr_end in regions:

        try:
            b_off, v_off = index.region_offset(
                bai_dict,
                curr_ref,
                curr_beg,
                curr_end,
            )
        except ValueError as err:
            continue

        tuples = get_chunk(bam_f, b_off, v_off)
        for ref, pos, end, rec in tuples:
            # Check that the record does not exceed the current region.
            # If so, stop fetching records for this region, as they will
            # not overlap due to sorting guarantee.
            # TODO double check inclusion / exclusion
            if any(
                [
                    ref > curr_ref,
                    pos > curr_end,
                ]
            ):
                break

            # Check that the record would not have been included in the
            # previous region fetched. If so, skip it, as it has already
            # been emitted.
            # TODO double check inclusion / exclusion
            if prev and all(
                [
                    ref == prev_ref,
                    pos <= prev_end,
                ]
            ):
                continue

            # Check that the record does not precede the current region.
            # If so, skip it, as we have not yet reached the records we
            # are interested in.
            # TODO double check inclusion / exclusion
            if any(
                [
                    ref < curr_ref,
                    end < curr_beg,
                ]
            ):
                continue

            yield rec

        # Update the log of the last region fetched.
        prev = True
        prev_ref = curr_ref
        prev_beg = curr_beg
        prev_end = curr_end

    if include_unmapped:
        tuples = fetch_unmapped_records(bam_f, bai_dict["mapped_end"])
        for ref, pos, end, rec in tuples:
            yield rec


def _coordinate(bam_head, records):
    """
    Helper function for coordinate slicing.
    Handles compression of the header and sliced records.
    """
    compressed = io.BytesIO()
    compressor = writer.BGZFBufferedWriter(fileobj=compressed)

    # TODO replace this block w/ yield compressor.compress(composite_header)
    compressor.write(header.header2bytes(bam_head))
    yield compressed.getvalue()
    compressed.truncate(0)
    compressed.seek(0)

    for rec in records:
        # TODO replace w/ yield compressor.compress(aln.to_bytes())
        compressor.write(rec)
        yield compressed.getvalue()
        compressed.truncate(0)
        compressed.seek(0)

    # TODO replace w/ yield compressor.flush(eof=True)
    compressor.flush()
    compressor.close()
    yield compressed.getvalue()


def coordinate(bam_f, bai_f, regions, include_unmapped=False):
    """
    Returns a generator of sliced bytes.

    :param bam_f: Function taking integer offset value and returning
    file-like objects for BAM format files positioned at the given offset.

    :param bai_fs: Function taking integer offset value and returning
    file-like objects for BAI format files positioned at the given offset.

    :param regions: 0-indexed coordinate range tuples. Must be of the form
    (str, int, int). Either int may be substituted with a None type. Semanticly
    matches the (suffix-)byte-range-spec of RFC7233.

        slice description: (ref, beg, end)
            ref - Reference sequence string identifier.
            beg - Position of first base in region.
            end - Position of last base in region.

    :param include_unmapped: include unmapped unplaced reads

    A key may be provided to specify the desired ordering of references.

    Guarantees that each record will be returned at most once for each BAM that
    contains said record and overlaps the specified regions.
    """
    # We do header and index parsing up front to catch errors early.
    header_buff = gzip.GzipFile(fileobj=bam_f(0))
    header_buff = io.BufferedReader(header_buff, 2**15)

    bam_head = header.file2dict(header_buff)
    bai_dict = index.file2dict(bai_f(0))

    # Filter out any references that don't exist in the bam.
    # NOTE If we want to error on reference mismatches, error here.
    refs = {sq["SN"] for sq in bam_head["SQ"]}
    regions = filter(lambda r: r[0] in refs, regions)
    # NOTE If we want to error on unsatisfiable regions, error here.
    regions = implicit_to_explicit_regions(bam_head, regions)

    regions = sorted(regions)
    regions = merged(regions)

    records = fetch_records(bam_f, bai_dict, regions, include_unmapped)

    return _coordinate(bam_head, records)
