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

    # bam_path = "/Users/paulineribeyre/Projects/gen3-analysis/tests/bamrest/data/slice_testing.bam"
    # bai_path = "/Users/paulineribeyre/Projects/gen3-analysis/tests/bamrest/data/slice_testing.bam.bai"
    # bam_path = "/Users/paulineribeyre/Downloads/GDC_BAM/3c7b6176-c578-4d2d-bfdd-d2a2fed509a2.rna_seq.chimeric.gdc_realn.bam"
    # bai_path = "/Users/paulineribeyre/Downloads/GDC_BAM/3c7b6176-c578-4d2d-bfdd-d2a2fed509a2.rna_seq.chimeric.gdc_realn.bam.bai"

    bam_guid = "PREFIX/a03fff27-ff92-4d17-bfb1-3c1908ba90ac"
    bai_guid = "PREFIX/131e3a2c-8e7d-4e49-9775-16adab4475f8"
    bam_path = bam

    region = body.regions[0]  # TODO: what to do with multiple regions?
    # region_pattern = "^[a-zA-Z0-9]+(:([0-9]+)?(-[0-9]+)?)?$"
    # region = ("chr7", 158192358, 158192478)
    print("get_slicing_view - region =", region)

    with open(f"{bam_path}.bai", "rb") as f:
        bai_data = f.read()

    # BAM file
    # get presigned url
    bam_res = requests.get(
        f"https://pauline.planx-pla.net/user/data/download/{bam_guid}",
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

    # BAI file
    # get presigned url
    bai_res = requests.get(
        f"https://pauline.planx-pla.net/user/data/download/{bai_guid}",
        stream=True,
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

    header_buff = gzip.GzipFile(fileobj=bam_res.raw)
    header_buff = io.BufferedReader(header_buff, 2**15)
    bam_head = header_file2dict(header_buff)
    bai_dict = index_file2dict(bai_res.raw)

    print('bam_head', bam_head)
    print('bai_dict', bai_dict)


    # byte_ranges = get_bam_ranges_from_bai(parsed_bai, tid, region_start, region_end)
    # print("Byte ranges to download from BAM file:")
    # for start, end in byte_ranges:
    #     print(f"bytes={start}-{end-1}")


    raise Exception("done")

    file_type = "b"  # "b" for bam or "c" for cram

    # print('bam_path', bam_path)
    with tempfile.NamedTemporaryFile(delete=False, mode="w+") as temp_bam:
        # Open input BAM file
        # with pysam.AlignmentFile(bam_path, f"r{file_type}", index_filename=bai_path) as samfile:
        with pysam.AlignmentFile(bam_path, f"r{file_type}") as samfile:
            # Open the output BAM file, using the input file's header as a template
            with pysam.AlignmentFile(temp_bam, f"w{file_type}", template=samfile) as outfile:
                # Iterate over reads in the specified region and write to buffer
                for read in samfile.fetch(region=region):
                    outfile.write(read)

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

    return ret

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

    header_dict = bytes2header(buffered)

    # NOTE We're discarding the extra reference information, as it's encoded in
    # the SAM header that we just parsed. No need to re-read the same info, and
    # having two places with potentially conflicting information isn't great...
    remainder = (
        4 + (8 * len(header_dict["SQ"])) + sum(len(sq["SN"]) + 1 for sq in header_dict["SQ"])
    )
    bam.read(remainder)

    return header_dict


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
