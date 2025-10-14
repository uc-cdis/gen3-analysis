from typing import Dict, Optional, Tuple, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from fastapi import Cookie
from pydantic import BaseModel
from starlette import status
from starlette.responses import JSONResponse
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.filters.gen3GQLFilters import parse_gql_filter
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.query_builders.ssm.ssms import ssms_query

ssms = APIRouter()


class SSMSRequest(BaseModel):
    id: str = Query(default=None, description="ssms id")
    filters: Dict = Query(default=None, description="filter (optional)")
    fields: list = Query(default=["file_id"], description="fields (optional)")
    access_token: Optional[str] = None


@ssms.post(
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
@ssms.post(path="/{ssms_id")
async def query_ssms(body: SSMSRequest):
    filters = body.filters
    expand = body.expand
    id = body.id
    access_token: Optional[Tuple[Any]] = (Cookie(None),)
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client)
    gql_filters = parse_gql_filter(filters)

    results = await ssms_query(
        gen3_graphql_client=gen3_graphql_client,
        id=id,
        fields=body.fields,
        expand=expand,
        access_token=access_token,
    )
    return JSONResponse(status_code=status.HTTP_200_OK, content=results)


@ssms.get(
    path="/{ssms_id}",
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
async def get_ssms_by_id(
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
    ssm_id: str = Path(..., description="ssms id"),
    fields: list = Query(default=["file_id"], description="fields (optional)"),
    expand: list = Query(default=None, description="expand (optional)"),
    access_token: Optional[Tuple[Any]] = Cookie(None),
):
    results = await ssms_query(
        gen3_graphql_client=gen3_graphql_client,
        id=ssm_id,
        fields=fields,
        expand=expand,
        access_token=access_token,
    )
    return JSONResponse(status_code=status.HTTP_200_OK, content=results)
