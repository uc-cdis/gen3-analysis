from typing import List, Dict, Optional
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import Response
from starlette import status
from starlette.responses import JSONResponse
from pydantic import BaseModel
from gen3analysis.filters.filters import FilterSet
import httpx
import json

CASE_BATCH_SIZE = 100000


def build_query(filters):
    query = """  query CohortComparison(
    $filters: FiltersArgument) {
    viewer {
      explore {
        cases {
         hits(filters: $filters, first:10000) {
      total
      edges {
        node {
          case_id
          submitter_id
          project {
            project_id
          }
          demographic {
            days_to_death
            vital_status
          }
          days_to_lost_to_followup
        }
      }
    }
    }
  }
    }
  }
    """


def get_curve(filters, req_opts):
    filters = [
        filters,
        {
            "op": "or",
            "content": [
                {
                    "op": "and",
                    "content": [
                        {
                            "op": ">",
                            "content": {
                                "field": "demographic.days_to_death",
                                "value": 0,
                            },
                        },
                    ],
                },
                {
                    "op": "and",
                    "content": [
                        {
                            "op": ">",
                            "content": {
                                "field": "diagnoses.days_to_last_follow_up",
                                "value": 0,
                            },
                        },
                    ],
                },
            ],
        },
        {"op": "not", "content": {"field": "demographic.vital_status"}},
    ]

    filters = [f for f in filters if "op" in f and "content" in f]

    data = cases_with_genes.search(
        {
            "fields": "diagnoses.days_to_last_follow_up,demographic.days_to_death,demographic.vital_status,case_id,submitter_id,project.project_id",
            "size": req_opts.get("size", CASE_BATCH_SIZE),
            "filters": {"op": "and", "content": filters},
        }
    )


# Define a Pydantic model for the request body
class PlotRequest(BaseModel):
    filters: Dict


survival = APIRouter()


@survival.post("/plot")
async def create_plot(
    request: Request,
    data: PlotRequest,
) -> JSONResponse:
    """Create a survival plot based on filtered data from Gen3.

    Args:
        request: The incoming request object
        data: The plot request containing filters

    Returns:
        JSONResponse containing the plot data

    Raises:
        HTTPException: If filters are invalid or GraphQL query fails
    """
    # check if data.filters is FilterSet
    if not isinstance(data.filters, FilterSet):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="filters must be a FilterSet",
        )

    try:
        # Construct GraphQL query
        variables = {"filter": data.filters}

        # Send GraphQL request
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "http://guppy-service/graphql",
                json={"query": query, "variables": variables},
            )
            response.raise_for_status()

        query_data = response.json()
        if "errors" in query_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"GraphQL query failed: {query_data['errors']}",
            )

        # Process and return results
        results = query_data.get("data", {}).get("subject", [])
        return JSONResponse(
            status_code=status.HTTP_200_OK, content={"results": results}
        )

    except httpx.RequestError as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to connect to GraphQL service: {str(e)}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )
