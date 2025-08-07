import os
from pydantic import BaseModel
from typing import List

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse, StreamingResponse
from starlette.background import BackgroundTasks
from starlette.status import HTTP_200_OK

from gen3analysis.auth import Auth
from gen3analysis.config import logger


import json
import logging
from urllib import parse

import requests

# from gen3analysis.bamrest import blueprint, exceptions
# from gdcdatamodel2 import models as md
from urllib3 import response

# from gdcapi.utils import request as gdcapi_request

# VALID_LABELS = [md.AlignedReads.label]


import pysam
import tempfile
import io
import gzip
import struct

slicing = APIRouter()


"""
https://docs.gdc.cancer.gov/API/Users_Guide/BAM_Slicing/

Example:
{
    "regions": [
        "chr1",
        "chr2:10000",
        "chr3:10000-20000"
    ],
    "gencode": [
        "BRCA1",
        "BRCA2"
    ]
}

curl --header "X-Auth-Token: $token" --request POST /slicing/view/2912e314-f6a7-4f4a-94ac-20db2c8f793b --header "Content-Type: application/json" -d@Payload --output post_regions_slice.bam

curl --header "X-Auth-Token: $token" --request POST /slicing/view/2912e314-f6a7-4f4a-94ac-20db2c8f793b --header "Content-Type: application/json" -d@Payload --output post_brca12_slice.bam

Response:
bam_data_stream
a BAM-formatted file containing the header of the source BAM file, as well as any alignment records that are found to overlap the specified regions, sorted by chromosomal coordinate.

- The functionality of this API differs from the usual functionality of samtools in that alignment records that overlap multiple regions will not be returned multiple times.
- A request with no region or gene specified will return the BAM header, which makes it easy to inspect the references to which the alignment records were aligned.
- A request for regions that are not included in the source BAM is not considered an error, and is treated the same as if no records existed for the region.
- Examples provided for BAM slicing functionality are intended for use with GDC harmonized data (i.e. BAM files available in the GDC Data Portal).
- Bam slicing does not create an associated bam index (.bai) file. For applications requiring a .bai file users will need to generate this file from the bam slice using a tool and command such as samtools index.

Examples: Specifying unmapped reads
Unmapped reads are found in GDC BAM files.
{
    "regions": [
        "unmapped"
    ]
}
"""

"""
https://portal.gdc.cancer.gov/auth/api/v0/slicing/view/9063a470-699b-495b-9c4f-0feaba4120c0?region=chr7:158192358-158192478
=> TCGA-H5-A2HR-01A-11R-A180-07.chr7.158192358.158192478.bam

https://portal.gdc.cancer.gov/auth/api/v0/slicing/view/9063a470-699b-495b-9c4f-0feaba4120c0?region=unmapped
=> TCGA-H5-A2HR-01A-11R-A180-07.unmapped.bam
"""


class SlicingRequest(BaseModel):
    regions: List[str] = []
    gencode: List[str] = []  # TODO can the params be empty? is one of 2 required?


@slicing.api_route(
    # "/view/{bam:str}",
    "/view/{bam:path}",
    methods=["GET", "POST"],
    status_code=HTTP_200_OK,
)
async def get_slicing_view(
    background_tasks: BackgroundTasks,
    bam: str,
    body: SlicingRequest,
    auth: Auth = Depends(Auth),
) -> dict:
    # return get_coordinates_slices_for_bam(bam)

    # GENCODE_VERSION = 22
    # TODO are these files sensitive? what are they? can i copy/paste them to this repo?
    # gencode_mapping_file = f"/Users/paulineribeyre/Downloads/gencode_{GENCODE_VERSION}_gene_mapping.json"

    # TODO until_eof=True for unmapped
    # bam_path = bam

    url = "https://pauline.planx-pla.net/user/data/download/"
    bam_guid = "PREFIX/a03fff27-ff92-4d17-bfb1-3c1908ba90ac"
    bai_guid = "PREFIX/131e3a2c-8e7d-4e49-9775-16adab4475f8"

    # url = "http://0.0.0.0:8000/data"
    # # bam_path = "/Users/paulineribeyre/Projects/gen3-analysis/tests/bamrest/data/slice_testing.bam"
    # # bai_path = "/Users/paulineribeyre/Projects/gen3-analysis/tests/bamrest/data/slice_testing.bam.bai"
    # bam_guid = "/Users/paulineribeyre/Downloads/GDC_BAM/3c7b6176-c578-4d2d-bfdd-d2a2fed509a2.rna_seq.chimeric.gdc_realn.bam"
    # bai_guid = "/Users/paulineribeyre/Downloads/GDC_BAM/3c7b6176-c578-4d2d-bfdd-d2a2fed509a2.rna_seq.chimeric.gdc_realn.bam.bai"

    # TODO: clean this up for multiple regions
    region = body.regions[0]
    # regions = [f"{region[0]}:{region[1]}-{region[2]}"]
    # region = ("chr7", 73769179, 125673677)
    contig = region.split(":")[0]
    region_start = int(region.split(":")[1].split("-")[0])
    region_end = int(region.split("-")[1])
    region = (contig, region_start, region_end)
    regions = [region]

    # region_pattern = "^[a-zA-Z0-9]+(:([0-9]+)?(-[0-9]+)?)?$"
    # region = ("chr7", 158192358, 158192478)
    print("get_slicing_view - region =", region)

    # with open(f"{bam_path}.bai", "rb") as f:
    #     bai_data = f.read()

    # BAM file
    # get presigned url
    print("Getting BAM file")
    bam_res = requests.get(
        f"{url}/{bam_guid}",
        stream=True,
        verify=True,
    )
    bam_res.raise_for_status()
    bam_presigned_url = bam_res.json().get("url")
    assert bam_presigned_url, bam_res.json()
    # get file data from presigned url
    # headers["Range"] = f"bytes={int(off)}-"
    bam_res = requests.get(
        bam_presigned_url,
        # headers=headers,
        # cookies=gdcapi_request.create_auth_cookies(),
        stream=True,
        verify=True,
    )
    bam_res.raise_for_status()
    # bam_res = todoname(bam_path)
    # requests_session = requests.session()
    # requests_session.mount('file://', LocalFileAdapter())
    # bam_res = requests_session.get(f'file://{bam_path}', stream=True)
    # bam_res.raise_for_status()

    # BAI file
    # get presigned url
    print("Getting BAI file")
    bai_res = requests.get(
        f"{url}/{bai_guid}",
        stream=True,  # TODO stream maybe not needed
        verify=True,
    )
    bai_res.raise_for_status()
    bai_presigned_url = bai_res.json().get("url")
    assert bai_presigned_url, bai_res.json()
    # get file data from presigned url
    # headers["Range"] = f"bytes={int(off)}-"
    bai_res = requests.get(
        bai_presigned_url,
        # headers=headers,
        # cookies=gdcapi_request.create_auth_cookies(),
        stream=True,
        verify=True,
    )
    bai_res.raise_for_status()
    # bai_res = todoname(bai_path)
    # requests_session.mount('file://', LocalFileAdapter())
    # bai_res = requests_session.get(f'file://{bai_path}', stream=True)
    # bai_res.raise_for_status()

    header_buff = gzip.GzipFile(fileobj=bam_res.raw)
    header_buff = io.BufferedReader(header_buff, 2**15)
    bam_header, bam_header_dict = header_file2dict(header_buff)
    bai_dict = index_file2dict(bai_res.raw)

    import json
    # print('bam_header_dict', json.dumps(bam_header_dict, indent=2))
    # print('bai_dict', json.dumps(bai_dict, indent=2))

    # Filter out any references that don't exist in the bam.
    # example: if the requested regions are "chr1:x-y" and "chr2:x-y", but the BAM only
    # includes "chr1" and "chr3", filter the requested regions down to down "chr1:x-y".
    # NOTE If we want to error on reference mismatches, error here.
    refs = {sq["SN"] for sq in bam_header_dict["SQ"]}
    regions2 = filter(lambda r: r[0] in refs, regions)

    # NOTE If we want to error on unsatisfiable regions, error here.
    regions = implicit_to_explicit_regions(bam_header_dict, regions2)
    # list(regions)

    regions = sorted(regions)
    regions = merged(regions)

    for curr_ref, curr_beg, curr_end in regions:
        # print("fetch_records", curr_ref, curr_beg, curr_end)

        # try:
        b_off_start, v_off = index_region_offset(
            bai_dict,
            curr_ref,
            curr_beg,
            curr_end,
        )
        # except ValueError as err:
        #     continue
        # print('START: b_off, v_off', b_off_start, v_off)
        # tuples = get_chunk(bam_f, b_off, v_off)

        b_off_end, v_off = index_region_offset_end(
            bai_dict,
            curr_ref,
            curr_beg,
            curr_end,
        )
        # print('END: b_off, v_off', b_off_end, v_off)

    # byte_ranges = get_bam_ranges_from_bai(parsed_bai, tid, region_start, region_end)
        print("Byte ranges to download from BAM file:", b_off_start, b_off_end)
    # for start, end in byte_ranges:
    #     print(f"bytes={start}-{end-1}")


    # BAM file
    # get presigned url
    print("Getting BAM file range")
    bam_res = requests.get(
        f"{url}/{bam_guid}",
        stream=True,
        verify=True,
    )
    bam_res.raise_for_status()
    bam_presigned_url = bam_res.json().get("url")
    assert bam_presigned_url, bam_res.json()
    # get file data from presigned url
    headers = {}
    headers["Range"] = f"bytes={int(b_off_start)}-"
    if b_off_start != b_off_end:
        headers["Range"] += str(b_off_end)
    bam_res = requests.get(
        bam_presigned_url,
        headers=headers,
        # cookies=gdcapi_request.create_auth_cookies(),
        # stream=True,
        verify=True,
    )
    bam_res.raise_for_status()
    # bam_stream = io.BytesIO(bam_res.text.encode('utf-8')).getvalue()
    # bam_path = io.BytesIO(bam_res.text.encode('utf-8'))

    with tempfile.NamedTemporaryFile(suffix=".bam", delete=False, mode="w+b") as temp_bam1:
        temp_bam1.write(b"BAM\x01")
        print(bam_header)
        temp_bam1.write(bam_header) #.encode('utf-8'))
        temp_bam1.write(bam_res.text.encode('utf-8'))
        temp_bam1.flush()
        # temp_bam_path = temp_bam.name  # Save the path before closing
        bam_path = temp_bam1.name
        # pysam.index(bam_path)

    # raise Exception("done")

    file_type = "b"  # "b" for bam or "c" for cram

    # with pysam.BGZFile(bam_stream, 'rb') as bam_path:
    try:
        with tempfile.NamedTemporaryFile(delete=False, mode="w+") as temp_bam:
            # Open input BAM file
            # with pysam.AlignmentFile(bam_path, f"r{file_type}", index_filename=bai_path) as samfile:
            print('bam_path', bam_path)
            with pysam.AlignmentFile(bam_path, f"r{file_type}") as samfile:
                # Open the output BAM file, using the input file's header as a template
                with pysam.AlignmentFile(
                    temp_bam, f"w{file_type}", template=samfile
                ) as outfile:
                    # Iterate over reads in the specified region and write to buffer
                    for alignment in samfile.fetch(region=f"{region[0]}:{region[1]}-{region[2]}"):
                        outfile.write(alignment)
    finally:
        os.unlink(bam_path)
        os.unlink(temp_bam.name)

    async def file_iterator():
        with open(temp_bam.name, "rb") as f:
            while chunk := f.read(8192):  # Read in 8KB chunks
                yield chunk
        # Clean up the temporary file after streaming
        os.unlink(temp_bam.name)

    return StreamingResponse(file_iterator(), media_type="application/octet-stream")
    # return StreamingResponse(
    #     file_iterator(),
    #     media_type="text/plain",
    #     headers={"Content-Disposition": "attachment; filename=my_temp_file.txt"}
    # )


# from requests_testadapter import Resp
# import os

# class LocalFileAdapter(requests.adapters.HTTPAdapter):
#     def build_response_from_file(self, request):
#         file_path = request.url[7:]
#         with open(file_path, 'rb') as file:
#             buff = bytearray(os.path.getsize(file_path))
#             file.readinto(buff)
#             resp = Resp(buff)
#             r = self.build_response(request, resp)

#             return r

#     def send(self, request, stream=False, timeout=None,
#              verify=True, cert=None, proxies=None):

#         return self.build_response_from_file(request)


# class todoname:
#     def __init__(self, file_path):
#         self.raw = stream_file_like_request(file_path)

# def stream_file_like_request(file_path, chunk_size=8192):
#     """
#     Opens a local file and yields its content in chunks,
#     mimicking a streaming GET request response.

#     Args:
#         file_path (str): The path to the local file.
#         chunk_size (int): The size of each chunk to read.

#     Yields:
#         bytes: Chunks of data from the file.
#     """
#     try:
#         with open(file_path, 'rb') as f:
#             while True:
#                 chunk = f.read(chunk_size)
#                 if not chunk:
#                     break  # End of file
#                 yield chunk
#     except FileNotFoundError:
#         print(f"Error: File not found at {file_path}")
#     except Exception as e:
#         print(f"An error occurred: {e}")

# # Example usage:
# # Create a dummy file for demonstration
# with open("my_local_file.txt", "wb") as f:
#     f.write(b"This is the first line.\n")
#     f.write(b"This is the second line.\n")
#     f.write(b"And the third line.\n")

# # Simulate processing the stream
# print("Streaming content from local file:")
# for data_chunk in stream_file_like_request("my_local_file.txt"):
#     print(f"Received chunk: {data_chunk.decode('utf-8').strip()}")

# # Clean up the dummy file
# import os
# os.remove("my_local_file.txt")


def deserialize(fmt, f):
    return struct.unpack(fmt, f.read(struct.calcsize(fmt)))[0]


import re


def bytes2header(b):
    """
    Parses and returns a dictionary representation of a SAM header byte buffer.
    """
    ret = {
        "HD": {},
        "SQ": [],
        "RG": [],
        "PG": [],
        "CO": [],
    }

    CODE_REGEX = r"^@(?P<code>[A-Z][A-Z])(?P<rest>\t.*)"
    code_regex = re.compile(CODE_REGEX)

    TAG_REGEX = r"\t(?P<tag>[A-Za-z][A-Za-z0-9]):(?P<val>[^\t]+)"
    tag_regex = re.compile(TAG_REGEX)

    TAGS_REGEX = rf"^(?:{TAG_REGEX})+$"
    tags_regex = re.compile(TAGS_REGEX)

    header = b.decode("us-ascii")

    for i, line in enumerate(header.strip("\n").split("\n")):

        match = code_regex.match(line)
        if not match:
            raise Exception("malformed code on line %d" % i)

        code = match.group("code")
        rest = match.group("rest")

        if code not in ret:
            raise Exception("unknown code on line %d" % i)

        if code == "HD" and i:
            raise Exception("header code found on line %d" % i)

        if code == "CO":
            ret[code].append(rest[1:])
            continue

        if tags_regex.match(rest) is None:
            raise Exception("malformed tags on line %d" % i)

        record = {}
        for match in tag_regex.finditer(rest):
            tag = match.group("tag")
            val = match.group("val")

            record[tag] = val

        if code == "HD":
            ret[code] = record
        else:
            ret[code].append(record)

    return b, ret


def header_file2dict(bam):
    """
    Deserialize BAM header from file-like object.
    """
    if bam.read(4) != b"BAM\x01":
        raise Exception("magic number not found")

    try:
        header_length = deserialize("<I", bam)
    except Exception:
        raise Exception("unexpected EOF encountered")

    # NOTE We preload the whole BAM header into memory since we'll be
    # representing the whole thing in memory anyways, and we don't necessarily
    # know if the file-like object is performant under small read sizes.
    buffered = bam.read(header_length)
    if len(buffered) < header_length:
        raise Exception("unexpected EOF encountered")

    header, header_dict = bytes2header(buffered)

    # NOTE We're discarding the extra reference information, as it's encoded in
    # the SAM header that we just parsed. No need to re-read the same info, and
    # having two places with potentially conflicting information isn't great...
    remainder = (
        4
        + (8 * len(header_dict["SQ"]))
        + sum(len(sq["SN"]) + 1 for sq in header_dict["SQ"])
    )
    bam.read(remainder)

    return header, header_dict


def index_file2dict(bai):
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
    # buffered = io.BytesIO(bai.read(1024))  # TODO switch back. Needed for using requests with local file

    if buffered.read(4) != b"BAI\x01":
        raise Exception("magic number not found")

    mapped_end = 0
    for i in range(deserialize("<i", buffered)):
        ref, region_mapped_end = file2ref(buffered)
        bai_dict["references"].append(ref)
        mapped_end = region_mapped_end if region_mapped_end > mapped_end else mapped_end

    try:
        bai_dict["unplaced"] = deserialize("<Q", buffered)
    except Exception:
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
        region_mapped_end = (
            chunk_end if region_mapped_end < chunk_end else region_mapped_end
        )

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
        raise Exception("unexpected EOF encountered")

    for i in range(deserialize("<i", bai)):
        bin_dict["chunks"].append(file2chunk(bai))

    return bin_dict


def file2interval(bai):
    """
    Deserialize a BAI file to an interval.
    """
    try:
        return deserialize("<Q", bai)
    except struct.error:
        raise Exception("unexpected EOF encountered")


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
        raise Exception("unexpected EOF encountered")


def implicit_to_explicit_region(bam_head, ref, beg, end):
    """
    Converts an implicit region to an explicit region.
    """
    # print('implicit_to_explicit_region', ref, beg, end)
    # ("chr1", 10001, 10011)
    # import json; print('bam_head', json.dumps(bam_head, indent=2))

    refs = [sq["SN"] for sq in bam_head["SQ"]]
    lens = {sq["SN"]: int(sq["LN"]) for sq in bam_head["SQ"]}
    # print('refs', refs)
    # print('lens', lens)

    if beg is None and end is not None:
        beg = lens[ref] - end
        end = lens[ref]

    end = end if end is not None else lens[ref]
    beg = beg if beg is not None else lens[ref] - end

    # print(refs.index(ref), beg, end)
    return refs.index(ref), beg, end


def implicit_to_explicit_regions(bam_head, regions):
    """
    Converts implicit regions to explicit regions.
    """
    return (implicit_to_explicit_region(bam_head, *r) for r in regions)


def merged(regions):
    """
    Merge coordinate sorted regions.
    TODO understand what this does
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


def index_region_offset(bai_dict, ref, beg, end):
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


def index_region_offset_end(bai_dict, ref, beg, end):
    """
    Returns the byte offset of the last chunk containing alignments within
    the specified region.

    Raises ValueError if the region is not included in the bai dict.
    """
    # print("--- start index_region_offset_end")
    # print("ref, beg, end", ref, beg, end)
    linear = linear_offset(bai_dict, ref, beg)
    linear_end = linear_offset(bai_dict, ref, end)
    # print("linear", linear)
    # print("linear_end", linear_end)

    offset = None

    # Find the earliest chunk that overlaps the linear offset.
    bins = bai_dict["references"][ref]["bins"]
    # print('bins', bins)
    for b in reg2bins(beg, end):

        # Skip any bins that weren't indexed - nothing in them.
        if b not in bins:
            # print('  skipping')
            continue
        # print('b', b)

        for lower, upper in bins[b]["chunks"]:
            # print('lower, upper', lower, upper)

            if upper < linear_end:
                continue

            offset = max(offset, upper) if offset is not None else upper

    if offset is None:
        raise ValueError("region not included in bai")

    print("offset", offset, offset >> 16)
    # Convert and return the non-virtual offsets.
    return offset >> 16, offset & 0x0000FFFF


import math


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
