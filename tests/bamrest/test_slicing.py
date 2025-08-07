import functools
import gzip
import io
import os

import pytest

from gen3analysis.bamrest.slicing import coordinate
from tests.bamrest import TEST_DIR


import pysam

BAM_PATH = os.path.join(TEST_DIR, "data/slice_testing.bam")
BAI_PATH = os.path.join(TEST_DIR, "data/slice_testing.bam.bai")
EXPECTED_OUTPUT_CHR1 = os.path.join(TEST_DIR, "data/slice_testing.expected_chr1.bam")
EXPECTED_OUTPUT_CHR2 = os.path.join(
    TEST_DIR, "data/slice_testing.expected_chr1_with_region.bam"
)
EXPECTED_OUTPUT_UNMAPPED = os.path.join(
    TEST_DIR, "data/slice_testing.expected_unmapped.bam"
)

GDC_BAM_PATH = "/Users/paulineribeyre/Downloads/GDC_BAM/3c7b6176-c578-4d2d-bfdd-d2a2fed509a2.rna_seq.chimeric.gdc_realn.bam"
GDC_BAI_PATH = "/Users/paulineribeyre/Downloads/GDC_BAM/3c7b6176-c578-4d2d-bfdd-d2a2fed509a2.rna_seq.chimeric.gdc_realn.bam.bai"
EXPECTED_OUTPUT_GDC_BAM_PATH1 = "/Users/paulineribeyre/Downloads/GDC_BAM/TCGA-H5-A2HR-01A-11R-A180-07.chr7.158192358.158192478.bam"
EXPECTED_OUTPUT_GDC_BAM_PATH2 = "/Users/paulineribeyre/Downloads/GDC_BAM/TCGA-H5-A2HR-01A-11R-A180-07.chr7.73769179.125673677.bam"


@pytest.mark.parametrize(
    "regions,unmapped,expected",
    [
        # ([("unmapped", None, None)], True, EXPECTED_OUTPUT_UNMAPPED),
        ([("chr1", None, None)], False, EXPECTED_OUTPUT_CHR1),
    ],
)
def test_slicing1(request, regions, unmapped, expected):
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
        # print(exp_r)
        # print(new_r)
        # print()
        assert exp_r == new_r


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bam_path,bai_path,regions,unmapped,expected",
    [
        (BAM_PATH, BAI_PATH, [("chr1", None, None)], False, EXPECTED_OUTPUT_CHR1),
        # 10	65	chr1	10001
        (BAM_PATH, BAI_PATH, [("chr1", 10001, 10361)], False, EXPECTED_OUTPUT_CHR2),
        # (
        #     GDC_BAM_PATH, GDC_BAI_PATH,
        #     [("chr7", 158192358, 158192478)],
        #     False,
        #     EXPECTED_OUTPUT_GDC_BAM_PATH1,
        # ),
        # (
        #     GDC_BAM_PATH, GDC_BAI_PATH,
        #     [("chr7", 73769179, 125673677)],
        #     False,
        #     EXPECTED_OUTPUT_GDC_BAM_PATH2,
        # ),
        # number of reads: 293361
        # D7T4KXP1:317:C2542ACXX:3:1208:1907:88424	147	chrUn_JTFH01001608v1_decoy	699
        # D7T4KXP1:317:C2542ACXX:3:1306:19484:175359	97	HCV-1	9431
        # TODO test multiple regions
    ],
)
# 73769132 73769199
# 73769132 73769266
# 73769139 73769100
async def test_slicing2(bam_path, bai_path, client, regions, unmapped, expected):
    # import pysam
    # import io

    # file_path = BAM_PATH
    # reference_fasta_path = "your_reference.fasta"

    _region = regions[0]
    # convert format ("chr7", 73769179, 125673677) => "chr7:73769179-125673677"
    region = _region[0]
    if _region[1] and _region[2]:
        region += f":{_region[1]}-{_region[2]}"
    # print('test_slicing2', region)

    res = await client.post(
        f"/slicing/view/{bam_path}?bai={bai_path}",
        json={"regions": [region]},
        # headers={"Authorization": f"bearer {TEST_ACCESS_TOKEN}"},
    )
    assert res.status_code == 200, res.text
    # print(res.text)
    # actual_data = res.text

    # async with client.stream("GET", "http://localhost:8000/stream-bam") as response:
    #     response.raise_for_status()
    # async for chunk in res.aiter_bytes():
    #     # Process each chunk (bytes)
    #     print(f"Received chunk of size: {len(chunk)} bytes")

    # print(type(res))
    out_path = f"{bam_path}_test_output"  # TODO use tempfile
    with open(out_path, "wb") as f:
        for chunk in res.iter_bytes():
            if chunk:
                f.write(chunk)

    # print("assertions")
    # with open(expected, "rb") as f:
    #     expected_data = f.read()
    #     with open(out_path, "rb") as f2:
    #         actual_data = f2.read()
    # print(len(expected_data), len(actual_data))
    # assert expected_data == actual_data
    try:
        assert pysam.view(out_path) == pysam.view(expected)
    finally:
        os.unlink(out_path)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "file_path,regions,unmapped,expected",
    [
        (BAM_PATH, [("chr1", None, None)], False, EXPECTED_OUTPUT_CHR1),
        # 10	65	chr1	10001
        # (BAM_PATH, [("chr1", 10001, 10011)], False, EXPECTED_OUTPUT_CHR1),  # TODO add a test for region not None
        # (GDC_BAM_PATH, [("chr7", 158192358, 158192478)], False, EXPECTED_OUTPUT_GDC_BAM_PATH1),
        (
            GDC_BAM_PATH,
            [("chr7", 73769179, 125673677)],
            False,
            EXPECTED_OUTPUT_GDC_BAM_PATH2,
        ),
        # number of reads: 293361
        # D7T4KXP1:317:C2542ACXX:3:1208:1907:88424	147	chrUn_JTFH01001608v1_decoy	699
        # D7T4KXP1:317:C2542ACXX:3:1306:19484:175359	97	HCV-1	9431
    ],
)
# 73769132 73769199
# 73769132 73769266
# 73769139 73769100
async def test_slicing3(file_path, regions, unmapped, expected):

    # import io

    # reference_fasta_path = "your_reference.fasta"

    _region = regions[0]

    # convert format ("chr7", 73769179, 125673677) => "chr7:73769179-125673677"
    region = _region[0]
    if _region[1] and _region[2]:
        region += f":{_region[1]}-{_region[2]}"
    print(region)

    # Open the file with the reference
    # samfile = None
    # try:
    #     try:
    #         file_type = "b" # "b" for bam or "c" for cram
    #         samfile = pysam.AlignmentFile(file_path, f"r{file_type}", index_filename=BAI_PATH) #, reference_filename=reference_fasta_path)
    #     except ValueError as e:
    #         print(f"Error opening file: {e}. Ensure reference file is correct and accessible.")
    #         raise

    #     # Fetch reads from a specific region (e.g., "chr1" from position 1000 to 2000)
    #     # region = regions[0]
    #     # print('test_slicing3', region)
    #     # region = ("chr1", 1000, 2000)
    #     # print('test_slicing3', region)

    #     # for read in samfile.fetch(*region):
    #     #     print(read)

    #     # import pysam
    #     # outfile = pysam.AlignmentFile(f"{file_path}_test_output", "rb", template=samfile)
    #     # for read in samfile.fetch(*region):
    #     #     outfile.write(read)
    #     # outfile.close()

    #     # print('done')
    #     # print(EXPECTED_OUTPUT_CHR1)
    #     # expected_chunks = get_chunks(gzip.GzipFile(expected))
    #     # # for ec in expected_chunks:
    #     #     # print(e.decode('utf-8'))
    #     #     # print(read.query_name, read.reference_start, read.reference_end)

    #     # from gen3analysis.bamrest import writer, header
    #     # compressed = io.BytesIO()
    #     # compressor = writer.BGZFBufferedWriter(fileobj=compressed)

    #     # # TODO replace this block w/ yield compressor.compress(composite_header)
    #     # # compressor.write(header.header2bytes(bam_head))
    #     # # yield compressed.getvalue()
    #     # # compressed.truncate(0)
    #     # # compressed.seek(0)

    #     # compressed = io.BytesIO()
    #     # for s in samfile.fetch(*region):
    #     #     # print(s)
    #     #     compressed.write(str(s).encode(encoding="utf-8") )
    #     # compressed.flush()
    #     # # print(compressed.getvalue())
    #     # decompressor = gzip.GzipFile(fileobj=io.BytesIO(compressed.getvalue()), mode="rb")
    #     # compressed.close()
    #     # print(get_chunks(decompressor))
    #     # # print(decompressor.read())

    #     # # for rec in expected_chunks:
    #     # #     # TODO replace w/ yield compressor.compress(aln.to_bytes())
    #     # #     compressor.write(rec)
    #     # #     print(compressed.getvalue())
    #     # #     compressed.truncate(0)
    #     # #     compressed.seek(0)

    #     # # TODO replace w/ yield compressor.flush(eof=True)
    #     # compressor.flush()
    #     # compressor.close()
    #     # # yield compressed.getvalue()

    # finally:
    #     if samfile:
    #         samfile.close()

    # Open the input BAM file in read mode
    out = f"{file_path}_test_output"
    with pysam.AlignmentFile(file_path, "rb") as samfile:
        # Open the output BAM file in write mode, using the input file's header as a template
        with pysam.AlignmentFile(out, "wb", template=samfile) as outfile:
            # Iterate over all reads in the input file
            # for read in samfile.fetch(*region):
            for read in samfile.fetch(region=region):
                # Write each read to the output file
                outfile.write(read)

    # with open(expected, "rb") as f:
    #     expected_data = f.read()
    #     with open(out, "rb") as f2:
    #         actual_data = f2.read()

    assert pysam.view(expected) == pysam.view(out)
    os.unlink(out)  # TODO put back

    # assert len(expected_data) == len(actual_data)
    # 255343 == 258899 # samfile.fetch("chr7", 73769179, 125673677)
    # 255343 == 258937 # samfile.fetch(region="chr7:73769179-125673677")
    # assert expected_data == actual_data

    # print_bam_file(file_path)
    # print_bam_file(expected)
    # print_bam_file(out)
    # os.unlink(out); os.unlink(f"{out}.bai")


def print_bam_file(path):
    limit = 5
    if not os.path.exists(f"{path}.bai"):
        pysam.index(path)
    print(f"\n======== {path}:\n")
    with pysam.AlignmentFile(path, "rb") as f:
        i = 0
        for read in f.fetch():
            # if "chr7" not in str(read):
            #     continue
            print(read)
            # for read in f.fetch(until_eof=True):
            # if i >= 293361 - limit:
            #     print(read)
            i += 1
            # if i == limit:
            #     break
    print("\n========\n")


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
        decompressor = gzip.GzipFile(
            fileobj=io.BytesIO(compressed.getvalue()), mode="rb"
        )
        compressed.close()

        return get_chunks(decompressor)


def get_chunks(file_obj):
    """
    The bam consists of header, remainder, and chunks sections.
    """

    from gen3analysis.bamrest import header, record

    def _process_header(file_obj):
        exp_header = None
        assert file_obj.read(4) == b"BAM\x01"
        exp_header_length = header.deserialize("<I", file_obj)
        buffered = file_obj.read(exp_header_length)
        assert len(buffered) == exp_header_length
        exp_header = header.bytes2header(buffered)
        return exp_header

    def _process_remainder(file_obj, header):
        remainder = (
            4 + (8 * len(header["SQ"])) + sum(len(sq["SN"]) + 1 for sq in header["SQ"])
        )
        file_obj.read(remainder)

    def _process_chunks(file_obj):
        return [i[3] for i in record.file_to_record_coordinate_tuples(file_obj)]

    file_header = _process_header(file_obj)
    _process_remainder(file_obj, file_header)
    return _process_chunks(file_obj)
