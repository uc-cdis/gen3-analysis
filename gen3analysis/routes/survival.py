from typing import Optional, Dict
from fastapi import APIRouter, Request
from starlette.responses import JSONResponse
from pydantic import BaseModel

from cdiserrors import InternalError, UserError

import json
import asyncio
from gen3analysis.gdc.graphqlQuery import GDCGQLClient

gdc_client = GDCGQLClient("https://portal.gdc.cancer.gov/auth/api/v0/graphql")

CASE_BATCH_SIZE = 10

GDCSurvivalQuery = f"""query GDCSurvivalCurve($filters: FiltersArgument) {{
  explore {{
    cases {{
      hits(filters: $filters, first: {CASE_BATCH_SIZE}) {{
        total
        edges {{
          node {{
            case_id
            submitter_id
            project {{
              project_id
            }}
            demographic {{
              days_to_death
              vital_status
            }}
            diagnoses {{
              hits(filters: $filters) {{
                edges {{
                  node {{
                    days_to_last_follow_up
                  }}
                }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""


async def get_curve(
    filters,
    req_opts: Optional[Dict] = None,
):
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
        {
            "op": "in",
            "content": {
                "field": "cases.project.project_id",
                "value": ["MMRF-COMMPASS"],
            },
        },
    ]

    filters = [f for f in filters if "op" in f and "content" in f]
    data = await gdc_client.execute(
        query=GDCSurvivalQuery, variables={"filters": {"op": "and", "content": filters}}
    )

    if data.get("error"):
        raise InternalError(data.get("error"))

    dataRoot = data.get("data", {}).get("explore", {}).get("cases", {}).get("hits", {})

    if dataRoot.get("total", 0) == 0:
        return []

    print(">>>>>> ", dataRoot)


# Define a Pydantic model for the request body
class PlotRequest(BaseModel):
    filters: Dict
    req_opts: Dict = {}


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
    try:
        filters = data.get("filters", [{}])
        if isinstance(filters, str):
            filters = json.loads(filters)
    except ValueError:
        raise UserError("filters must be valid json")

    curves = []
    non_empty_curves = []
    for f in filters:
        curve = get_curve(f, data.get("req_opts", {}))
        curves.append(curve)
        if curve:
            non_empty_curves.append(curve)


if __name__ == "__main__":
    print("Running in debug mode")
    asyncio.run(get_curve({}))
