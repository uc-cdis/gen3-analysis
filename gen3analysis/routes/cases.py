from typing import Dict, Optional, Tuple, Any, List
from fastapi import APIRouter, Depends, Query, Path
from fastapi import Cookie
from pydantic import BaseModel, Field
from starlette import status
from starlette.responses import JSONResponse
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.filters.gen3GQLFilters import GQLFilter, parse_gql_filter
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.query_builders.cases.cases import cases_query, case_summary_query
from gen3analysis.settings import settings

cases = APIRouter()


class CasesRequest(BaseModel):
    filters: Optional[Dict] = Field(default=None, description="filter (optional)")
    fields: Optional[List[str]] = Field(
        default=["case_id"], description="fields (optional)"
    )
    size: Optional[int] = Field(
        default=10,
        ge=1,
        le=settings.MAX_CASES,
        description="number of cases to return (optional) default: 10",
    )
    offset: Optional[int] = Field(
        default=0,
        ge=0,
        le=settings.MAX_CASES,
        description="offset (optional) default: 0",
    )


class CaseSummaryRequest(BaseModel):
    id: str = Field(default=None, description="case id")
    fields: Optional[List[str]] = Field(
        default=["case_id"], description="fields (optional)"
    )


@cases.post(
    path="/",
    dependencies=[Depends(get_guppy_client)],
    status_code=status.HTTP_200_OK,
    description="Performs a cases query and returns case metadata for the matching cases.",
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
async def query_cases(
    body: CasesRequest,
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
    access_token: Optional[str] = Cookie(default=None, alias="access_token"),
):
    filters = body.filters
    size = body.size
    offset = body.offset

    gql_filters = parse_gql_filter(filters)

    results = await cases_query(
        gen3_graphql_client, gql_filters, body.fields, size, offset, access_token
    )
    return JSONResponse(status_code=status.HTTP_200_OK, content=results)


@cases.get(
    path="/{case_id}",
    dependencies=[Depends(get_guppy_client)],
    status_code=status.HTTP_200_OK,
    description="Performs a cases query and returns case metadata for case id",
    summary="Get case id metadata",
    responses={
        status.HTTP_200_OK: {"description": "Successfully processed the case query"},
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
async def get_case_by_id(
    case_id: str = Path(..., description="case id"),
    access_token: Optional[Tuple[Any]] = Cookie(None),
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
):
    results = await case_summary_query(gen3_graphql_client, case_id, access_token)
    return JSONResponse(status_code=status.HTTP_200_OK, content=results)
