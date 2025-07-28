from lifelines import KaplanMeierFitter
from lifelines.statistics import logrank_test
from glom import glom
import pandas as pd
from typing import List, Dict, Optional
from fastapi import APIRouter, Depends, Request, HTTPException
from starlette.responses import JSONResponse
from starlette import status
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from pydantic import BaseModel

MAX_CASES = 10000

survival = APIRouter()

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


def transform(data) -> pd.DataFrame:
    """Transform the Gen3 data into a pandas DataFrame suitable for lifelines."""
    records = []

    for case in data:
        demographic = case.get("demographic", [])
        if not demographic:
            continue

        demo = demographic[0]
        days_to_death = demo.get("days_to_death")
        diagnoses = case.get("diagnoses", [{}])[0]
        days_to_follow_up = diagnoses.get("days_to_last_follow_up")

        # Use days_to_death if available, otherwise use days_to_follow_up
        duration = days_to_death if days_to_death is not None else days_to_follow_up

        if duration is not None:
            records.append(
                {
                    "duration": duration,
                    "event": int(
                        demo.get("vital_status", "").lower() != "alive"
                    ),  # 1 if dead, 0 if alive
                    "case_id": case.get("_case_id"),
                    "submitter_id": case.get("submitter_id"),
                    "project_id": case.get("project_id"),
                }
            )

    return pd.DataFrame(records)


async def get_curve(filters, gen3_graphql_client, req_opts: Optional[Dict] = None):
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
        query=Gen3GraphQLQuery, variables={"filter": queryFilter}
    )
    if data.get("error"):
        raise ValueError(data.get("error"))
    if glom(data, "data._aggregation.case._totalCount", default=0) == 0:
        return None
    data_root = glom(data, "data.case", default={})
    df = transform(data_root)
    if df.empty:
        return None

    # Ensure duration and event columns are numeric
    df["duration"] = pd.to_numeric(df["duration"], errors="coerce")
    df["event"] = pd.to_numeric(df["event"], errors="coerce").astype(int)

    # Remove rows with NaN values
    df = df.dropna(subset=["duration", "event"])

    if df.empty:
        return None

    # Create KaplanMeierFitter object
    kmf = KaplanMeierFitter()
    kmf.fit(
        durations=df["duration"], event_observed=df["event"], label="Survival Curve"
    )

    # Group donors by their exact duration time
    donors_by_time = (
        df.groupby("duration")
        .apply(
            lambda group: [
                {
                    "id": row["case_id"],
                    "submitter_id": row["submitter_id"],
                    "project_id": row["project_id"],
                    "event_type": "death" if row["event"] == 1 else "censored",
                }
                for _, row in group.iterrows()
            ]
        )
        .to_dict()
    )

    # Format results
    results = []
    survival_df = kmf.survival_function_
    confidence_df = kmf.confidence_interval_
    all_donors = []
    for time_point in kmf.timeline:
        survival_prob = survival_df.loc[time_point].iloc[0]
        ci_lower = confidence_df.loc[time_point].iloc[0]
        ci_upper = confidence_df.loc[time_point].iloc[1]

        # Only include donors who have events at this exact time point
        donors = []
        if time_point in donors_by_time:
            for donor in donors_by_time[time_point]:
                results.append(
                    {
                        "time": int(time_point),
                        "id": donor["id"],
                        "submitter_id": donor["submitter_id"],
                        "project_id": donor["project_id"],
                        "survivalEstimate": float(survival_prob),
                        "censored": (
                            True if donor["event_type"] == "censored" else False
                        ),
                    }
                )

    return {
        "meta": {"id": id(results)},
        "donors": results,
    }


# Define a Pydantic model for the request body
class PlotRequest(BaseModel):
    filters: List[Dict] = []
    req_opts: Dict = {}


@survival.post(
    path="/",
    dependencies=[Depends(get_guppy_client)],
    status_code=status.HTTP_200_OK,
    description="Retrieves the survival plot(s) for the given filters. An array of filters is provided and will return an array of survival plot data",
    summary="Survival plots for cohort represented as filters",
    responses={
        status.HTTP_200_OK: {"description": "Successfully processed the survival plot"},
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
@survival.post("/", include_in_schema=False, dependencies=[Depends(get_guppy_client)])
async def plot(
    request: PlotRequest,
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
) -> JSONResponse:
    filters = request.filters

    try:
        curves = []
        non_empty_curves = []
        for f in filters:
            curve = await get_curve(f, gen3_graphql_client, {})
            curves.append(curve)
            if curve:
                non_empty_curves.append(curve)

        stats = {}
        if len(non_empty_curves) > 1:
            # Perform logrank test using lifelines [[1]](https://lifelines.readthedocs.io/en/latest/Survival%20analysis%20with%20lifelines.html)
            durations = [curve[0]["time"] for curve in non_empty_curves]
            events = [
                1 if curve[0]["estimate"] < 1 else 0 for curve in non_empty_curves
            ]
            results = logrank_test(durations[0], durations[1], events[0], events[1])

            stats = {"pValue": results.p_value, "testStatistic": results.test_statistic}

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "results": curves,
                "overallStats": stats,
            },
        )

    except ValueError:
        raise HTTPException(status_code=500, detail="Issue with data response")
    except Exception as e:
        raise HTTPException(status_code=500)
