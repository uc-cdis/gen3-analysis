import json
from typing import Dict, Optional, List

from fastapi import APIRouter, Depends, HTTPException
from fastapi import Cookie
from glom import glom
from pydantic import BaseModel, Field
from starlette import status
from starlette.responses import JSONResponse

from gen3analysis.auth import Auth
from gen3analysis.config import logger
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.query_builders.cases import cases
from gen3analysis.settings import settings


download = APIRouter()


@download.post("/download/biospecimen")
async def download_tar_post(
    object_ids: Optional[List[str]] = None,
    object_type: Optional[str] = None,
    compress: bool = False,
    limit: int = 100,
):
    """
    Create tar files for biospecimen downloads for either json or TSV
    """
    # Validate limit
    if limit < 1 or limit > 1000:
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 1000")

    # Query data objects
    data_objects = await query_data_objects(
        object_ids=object_ids, object_type=object_type, limit=limit
    )

    if not data_objects:
        raise HTTPException(
            status_code=404, detail="No data objects found matching the query"
        )

    # Generate filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    extension = "tar.gz" if compress else "tar"
    filename = f"data_export_{timestamp}.{extension}"

    # Set appropriate content type
    content_type = "application/gzip" if compress else "application/x-tar"

    # Create streaming response
    return StreamingResponse(
        generate_tar_stream(data_objects, compress=compress),
        media_type=content_type,
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "X-Object-Count": str(len(data_objects)),
        },
    )
