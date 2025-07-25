from typing import Optional, Dict
from glom import glom
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Request, HTTPException
from starlette import status
from starlette.responses import JSONResponse

from gen3analysis.auth import Auth
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.survivalpy.logrank import LogRankTest
from gen3analysis.survivalpy.survival import Analyzer, Datum

MAX_CASES = 10000

survivalGen3 = APIRouter()

Gen3GraphQLQuery = f"""query ($filter: JSON) {{
    case(accessibility: accessible, offset: 0, first: {MAX_CASES}, filter: $filter) {{
        submitter_id
        _case_id
        project_id
        demographic {{
            days_to_death
            vital_status
        }}
        diagnoses {{
            days_to_last_follow_up
        }}
    }}
    _aggregation {{
        case(filter: $filter, accessibility: accessible) {{
            _totalCount
        }}
    }}
}}
"""


def make_datum(diagnoses, case):
    demographic = case.get("demographic", [])
    days_to_death = demographic[0].get("days_to_death", None)
    days = (
        days_to_death or diagnoses.get("days_to_last_follow_up")
        if diagnoses is not None
        else days_to_death
    )
    if days is None:
        return None

    meta = {"id": case["_case_id"]}
    if "submitter_id" in case:
        meta["submitter_id"] = case["submitter_id"]

    if case.get("project_id") is not None:
        meta["project_id"] = case["project_id"]

    vital_status = (
        demographic[0].get("vital_status", "").lower() == "alive"
        if len(demographic) > 0
        else False
    )

    return Datum(days, vital_status, meta)


def make_data(ds, case):
    return list(filter(bool, (make_datum(d, case) for d in ds)))


def transform(data):
    r = [
        # the default value is [{}] to ensure that if there is no diagnosis but there is days_to_death and vital_status
        # make_data function still return the not None value
        make_data(c.get("diagnoses", []), c)
        for c in data
        if "diagnoses" in c or "demographic" in c
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


async def get_curve(
    filters, gen3_graphql_client, access_token=None, req_opts: Optional[Dict] = None
):
    queryFilter = {
        "and": [
            filters,
            {
                "and": [
                    {
                        "or": [
                            {
                                "nested": {
                                    ">": {"days_to_death": 0},
                                    "path": "demographic",
                                }
                            },
                            {
                                "nested": {
                                    ">": {"days_to_last_follow_up": 0},
                                    "path": "diagnoses",
                                }
                            },
                        ]
                    }
                ]
            },
        ]
    }

    data = await gen3_graphql_client.execute(
        access_token=access_token,
        query=Gen3GraphQLQuery,
        variables={"filter": queryFilter},
    )

    if data.get("error"):
        raise ValueError(data.get("error"))

    # if no cases return
    if glom(data, "data._aggregation.case._totalCount", default=0) == 0:
        return []

    data_root = glom(data, "data.case", default={})
    results = Analyzer(transform(data_root)).compute()

    return results


filters = [
    {
        "and": [
            {
                "or": [
                    {"nested": {">": {"days_to_death": 0}, "path": "demographic"}},
                    {
                        "nested": {
                            ">": {"days_to_last_follow_up": 0},
                            "path": "diagnoses",
                        }
                    },
                ]
            }
        ]
    }
]


# Define a Pydantic model for the request body
class PlotRequest(BaseModel):
    filters: str
    req_opts: Dict = {}


@survivalGen3.post("/plot", include_in_schema=False, dependencies=[])
async def plot(
    request: PlotRequest,
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
    auth: Auth = Depends(Auth),
) -> JSONResponse:
    """
    Handles the plot request for survival analysis.

    This function processes a survival plot request using specific filters and
    returns computed survival curves and overall statistics. It integrates with
    a Gen3 Guppy GraphQL client to retrieve data and processes survival curves
    accordingly. Additionally, it includes functionality to calculate statistical
    significance using the Log Rank Test if multiple non-empty curves are
    retrieved.

    Args:
        request (PlotRequest): The incoming HTTP request containing the plotting details.
        gen3_graphql_client (GuppyGQLClient, optional): Dependency-injected instance
            of a configured Gen3 GraphQL client.

    Returns:
        JSONResponse: A structured JSON response containing survival curves
        ("results") and overall statistical test results ("overallStats").

    Raises:
        HTTPException: Raised with status code `500` if there is an issue with the
        data response or any unexpected exceptions occur.
    """
    try:
        curves = []
        non_empty_curves = []
        access_token = await auth.get_access_token()
        for f in filters:
            curve = await get_curve(f, gen3_graphql_client, access_token, {})
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

    except ValueError:
        raise HTTPException(status_code=500, detail="Issue data response")
    except Exception as e:
        # handle ValueError
        raise HTTPException(status_code=500)
