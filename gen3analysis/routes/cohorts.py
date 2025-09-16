from typing import List, Dict, Optional

from fastapi import Cookie
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from starlette import status
from starlette.responses import JSONResponse

from gen3analysis.auth import Auth
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.routes import cases
from gen3analysis.config import logger


cohorts = APIRouter()


# Define a Pydantic model for the request body
class CohortQueryRequest(BaseModel):
    cohort_filters: Dict
    filters: Dict = {}
    query: str = ""
    case_index: str = ""
    cohort_item_field: str = ""
    limit: int = 10000


@cohorts.post(
    path="/query",
    dependencies=[Depends(get_guppy_client)],
    status_code=status.HTTP_200_OK,
    description="Performs a cohort query and return the query for all items matching the ids.",
    summary="Queries for cohort ids and uses those ids as the cohort in the second query",
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
async def cohort_query(
    body: CohortQueryRequest,
    access_token: Optional[str] = Cookie(None),
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
    auth: Auth = Depends(Auth),
) -> JSONResponse:
    cohort_filters = body.cohort_filters
    case_index = body.case_index
    cohort_item_field = body.cohort_item_field
    limit = body.limit
    query = body.query
    filters = body.filters

    if cohort_filters is None == 0:
        raise HTTPException(status_code=400, detail="Must have the cohort_query filter")

    if filters is None or len(filters) == 0:
        raise HTTPException(status_code=400, detail="Must have the query filter")

    if query is None or len(filters) == 0:
        raise HTTPException(status_code=400, detail="Must have the query")

    try:
        data = await cases.cohort_query(
            gen3_graphql_client=gen3_graphql_client,
            case_index=case_index,
            cohort_item_field=cohort_item_field,
            query=query,
            cohort_filters=cohort_filters,
            filters=filters,
            limit=limit,
            access_token=access_token,
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "results": data,
            },
        )
    except Exception as e:
        logger.error(f"Error while processing cohort query: {e}")
        raise HTTPException(status_code=500, detail="Error with cohort query")


@cohorts.get(
    path="/status",
)
def get_cohort_status() -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "results": "test",
        },
    )
