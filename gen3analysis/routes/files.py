from typing import Dict, Optional, Tuple, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from fastapi import Cookie
from pydantic import BaseModel
from starlette import status
from starlette.responses import JSONResponse
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.filters.gen3GQLFilters import parse_gql_filter
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.query_builders.files.files import files_query, file_summary_query
from gen3analysis.routes.survival import MAX_CASES

files = APIRouter()


class CasesRequest(BaseModel):
    filters: Dict = Query(default=None, description="filter (optional)")
    fields: list = Query(default=["file_id"], description="fields (optional)")
    size: int = Query(
        default=10,
        ge=1,
        le=MAX_CASES,
        description="number of files to return (optional) default: 10",
    )
    offset: int = Query(
        default=0, ge=0, le=MAX_CASES, description="offset (optional) default: 0"
    )
    access_token: Optional[str] = None


class CaseSummaryRequest(BaseModel):
    id: str = Query(default=None, description="case id", required=True)
    fields: list = Query(default=["file_id"], description="fields (optional)")
    access_token: Optional[str] = None


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
@files.post(path="/")
async def query_files(body: CasesRequest):
    filters = body.filters
    size = body.size
    offset = body.offset
    access_token: Optional[Tuple[Any]] = (Cookie(None),)
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client)

    gql_filters = parse_gql_filter(filters)

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
    access_token: Optional[Tuple[Any]] = Cookie(None),
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
):
    results = await file_summary_query(gen3_graphql_client, file_id, access_token)
    return JSONResponse(status_code=status.HTTP_200_OK, content=results)
