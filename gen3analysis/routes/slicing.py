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

    bam_path = bam

    region = body.regions[0]  # TODO: what to do with multiple regions?
    region_pattern = "^[a-zA-Z0-9]+(:([0-9]+)?(-[0-9]+)?)?$"
    # region = ("chr7", 158192358, 158192478)
    print("get_slicing_view - region =", region)

    file_type = "b"  # "b" for bam or "c" for cram

    # print('bam_path', bam_path)
    with tempfile.NamedTemporaryFile(delete=False, mode="w+") as temp_bam:
        # Open input BAM file
        # with pysam.AlignmentFile(bam_path, f"r{file_type}", index_filename=bai_path) as samfile:
        with pysam.AlignmentFile(bam_path, f"r{file_type}") as samfile:
            # Open the output BAM file, using the input file's header as a template
            with pysam.AlignmentFile(temp_bam, "wb", template=samfile) as outfile:
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
