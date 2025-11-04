from typing import Dict, Optional, List

from fastapi import APIRouter, Depends, Path
from fastapi import Cookie
from pydantic import BaseModel, Field
from starlette import status
from starlette.responses import JSONResponse

from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.filters.gen3GQLFilters import parse_gql_filter
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.query_builders.files.files import files_query, file_summary_query
from gen3analysis.settings import settings

files = APIRouter()


class FilesRequest(BaseModel):
    filter: Optional[Dict] = Field(default=None, description="filter (optional)")
    fields: Optional[List[str]] = Field(
        default=["file_id"], description="fields (optional)"
    )
    size: Optional[int] = Field(
        default=10,
        ge=1,
        le=settings.MAX_CASES,
        description="number of files to return (optional) default: 10",
    )
    offset: int = Field(
        default=0,
        ge=0,
        le=settings.MAX_CASES,
        description="offset (optional) default: 0",
    )


class FilesSummaryRequest(BaseModel):
    id: str = Field(default=None, description="file id")
    fields: Optional[List[str]] = Field(
        default=["file_id"], description="fields (optional)"
    )


@files.post(
    path="/",
    dependencies=[Depends(get_guppy_client)],
    status_code=status.HTTP_200_OK,
    description="Performs a files query and returns case metadata for the matching files.",
    summary="Query case metadata",
    responses={
        status.HTTP_200_OK: {"description": "Successfully processed the cohort query"},
        status.HTTP_400_BAD_REQUEST: {
            "description": "The request body is missing required fields or has invalid values."
        },
        status.HTTP_401_UNAUTHORIZED: {
            "description": "User unauthorized when accessing endpoint"
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "User does not have access to requested data"
        },
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Something went wrong internally when processing the request"
        },
    },
)
async def query_files(
    body: FilesRequest,
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
    access_token: Optional[str] = Cookie(default=None, alias="access_token"),
):
    filter = body.filter
    size = body.size
    offset = body.offset
    gql_filters = parse_gql_filter(filter)

    results = await files_query(
        gen3_graphql_client, gql_filters, body.fields, size, offset, access_token
    )
    return JSONResponse(status_code=status.HTTP_200_OK, content=results)


@files.get(
    path="/{file_id}",
    dependencies=[Depends(get_guppy_client)],
    status_code=status.HTTP_200_OK,
    description="Performs a files query and returns file metadata for file id",
    summary="Get file id metadata",
    responses={
        status.HTTP_200_OK: {"description": "Successfully processed the file query"},
        status.HTTP_400_BAD_REQUEST: {
            "description": "The request body is missing required fields or has invalid values."
        },
        status.HTTP_401_UNAUTHORIZED: {
            "description": "User unauthorized when accessing endpoint"
        },
        status.HTTP_403_FORBIDDEN: {
            "description": "User does not have access to requested data"
        },
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Something went wrong internally when processing the request"
        },
    },
)
async def get_file_by_id(
    file_id: str = Path(..., description="file id"),
    access_token: Optional[str] = Cookie(default=None, alias="access_token"),
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
):
    results = await file_summary_query(gen3_graphql_client, file_id, access_token)
    return JSONResponse(status_code=status.HTTP_200_OK, content=results)
