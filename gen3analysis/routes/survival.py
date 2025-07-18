from typing import Optional, Dict, List
from fastapi import APIRouter, Request, Depends, HTTPException
from starlette.responses import JSONResponse
from starlette import status
from pydantic import BaseModel

from gen3analysis.dependencies.gdc_graphql_client import get_gdc_graphql_client
from gen3analysis.survivalpy.logrank import LogRankTest
from gen3analysis.survivalpy.survival import Analyzer, Datum
from gen3analysis.clients import gdc_graphql_client

from cdiserrors import InternalError, UserError

import json
import asyncio
from gen3analysis.gdc.graphqlQuery import GDCGQLClient


CASE_BATCH_SIZE = 1000

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


def make_datum(d, case):
    demographic = case.get("demographic", {})
    days_to_death = demographic.get("days_to_death")
    days = (
        days_to_death or d.get("days_to_last_follow_up")
        if d is not None
        else days_to_death
    )
    if days is None:
        return None

    meta = {"id": case["case_id"]}
    if "submitter_id" in case:
        meta["submitter_id"] = case["submitter_id"]

    if case.get("project", {}).get("project_id") is not None:
        meta["project_id"] = case["project"]["project_id"]

    return Datum(days, demographic.get("vital_status", "").lower() == "alive", meta)


def make_data(ds, case):
    return list(filter(bool, (make_datum(d["node"], case) for d in ds)))


def transform(data):
    r = [
        # default value is [{}] to ensure that if there is no diagnose but there is days_to_death and vital_status
        # make_data function still return the not None value
        make_data(
            c["node"].get("diagnoses", {}).get("hits", {}).get("edges", {}), c["node"]
        )
        for c in data["edges"]
        if "diagnoses" in c["node"] or "demographic" in c["node"]
    ]
    return [item for sublist in r for item in sublist]


def prepare_donor(donor, estimate):
    donor["survivalEstimate"] = estimate
    donor["id"] = donor["meta"]["id"]
    donor["submitter_id"] = donor["meta"]["submitter_id"]
    donor["project_id"] = donor["meta"]["project_id"]
    donor.pop("meta", None)
    return donor


def prepare_result(result):
    items = [item.to_json_dict() for item in result]

    return {
        "meta": {"id": id(result)},
        "donors": [
            prepare_donor(donor, interval.get("cumulativeSurvival"))
            for interval in items
            for donor in interval["donors"]
        ],
    }


def get_curve(
    filters,
    gdc_graphql_client,
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
    data = gdc_graphql_client.execute(
        query=GDCSurvivalQuery, variables={"filters": {"op": "and", "content": filters}}
    )

    if data.get("error"):
        raise InternalError(data.get("error"))

    dataRoot = data.get("data", {}).get("explore", {}).get("cases", {}).get("hits", {})

    if dataRoot.get("total", 0) == 0:
        return []

    results = Analyzer(transform(dataRoot)).compute()

    return results


# Define a Pydantic model for the request body
class PlotRequest(BaseModel):
    filters: str
    req_opts: Dict = {}


survival = APIRouter()


@survival.post("/plot")
def create_plot(
    request: Request,
    data: PlotRequest,
    gdc_graphql_client: GDCGQLClient = Depends(get_gdc_graphql_client),
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

    # return JSONResponse(status_code=status.HTTP_200_OK, content={"results" : data.filters})
    try:
        filters = data.filters
        if isinstance(filters, str):
            filters = json.loads(filters)
    except ValueError:
        raise UserError("filters must be valid json")

    curves = []
    non_empty_curves = []
    for f in filters:
        curve = get_curve(f, gdc_graphql_client, {})
        curves.append(
            curve,
        )
        if curve:
            non_empty_curves.append(curve)

    stats = {}
    if len(non_empty_curves) > 1:
        stats = LogRankTest(survival_results=non_empty_curves).compute()

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "results": [prepare_result(result) for result in curves],
            "overallStats": stats,
        },
    )


if __name__ == "__main__":
    print("Running in debug mode")
    asyncio.run(get_curve({}))
