from typing import Dict, Optional, List
from fastapi import APIRouter, Depends, Query, Path
from fastapi import Cookie
from pydantic import BaseModel, Field
from starlette import status
from starlette.responses import JSONResponse
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.filters.gen3GQLFilters import parse_gql_filter
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.query_builders.ssm.ssms import ssms_query, ssms_id_query

ssms = APIRouter()


class SSMSRBaseRequest(BaseModel):
    expand: Optional[List[str]] = Field(
        default=None, description="which fields to expand (optional)"
    )
    fields: Optional[List[str]] = Field(default=None, description="fields (optional)")


class SSMSRequest(SSMSRBaseRequest):
    filter: Optional[Dict] = Field(default=None, description="filter (optional)")
    start: Optional[int] = Field(default=0, ge=0, le=10000, description="start index")
    size: Optional[int] = Field(default=10, ge=1, le=1000, description="page size")


class SSMSIDRequest(SSMSRBaseRequest):
    id: str = Field(default=None, description="ssms id")


@ssms.post(
    path="/",
    # remove dependencies here if you want the value in the handler
    status_code=status.HTTP_200_OK,
    description="Query the ssms metadata",
    summary="Query ssms metadata",
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
async def get_ssms(
    body: SSMSRequest,
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
    access_token: str | None = Cookie(default=None, alias="access_token"),
):
    fltr = body.filter
    expand = body.expand
    start = body.start
    size = body.size
    fields = body.fields
    gql_filter = parse_gql_filter(fltr)

    results = await ssms_query(
        gen3_graphql_client=gen3_graphql_client,
        fields=fields,
        expand=expand,
        filter=gql_filter,
        start=start,
        size=size,
        access_token=access_token,
    )
    return JSONResponse(status_code=status.HTTP_200_OK, content=results)


@ssms.get(
    path="/{ssm_id}",
    dependencies=[Depends(get_guppy_client)],
    status_code=status.HTTP_200_OK,
    description="Query the ssms metadata for the ssms id",
    summary="Get ssms metadata",
    responses={
        status.HTTP_200_OK: {"description": "Successfully processed the ssms query"},
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
    ssm_id: str = Path(..., description="SSM identifier"),
    fields: Optional[List[str]] = Query(default=None, description="fields (optional)"),
    expand: Optional[List[str]] = Query(default=None, description="expand (optional)"),
    access_token: str | None = Cookie(default=None, alias="access_token"),
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
):
    results = await ssms_id_query(
        gen3_graphql_client=gen3_graphql_client,
        id=ssm_id,
        fields=fields,
        expand=expand,
        access_token=access_token,
    )
    return JSONResponse(status_code=status.HTTP_200_OK, content=results)
