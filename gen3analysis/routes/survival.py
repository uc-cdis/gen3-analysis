from typing import List, Dict, Optional

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from glom import glom
from lifelines import KaplanMeierFitter
from lifelines.statistics import multivariate_logrank_test
from pydantic import BaseModel
from starlette import status
from starlette.responses import JSONResponse

from gen3analysis.auth import Auth
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.gen3.guppyQuery import GuppyGQLClient

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


async def get_curve(
    filters, gen3_graphql_client, auth, req_opts: Optional[Dict] = None
):
    query_filter = {
        "and": [
            filters,
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
            },
        ]
    }
    data = await gen3_graphql_client.execute(
        access_token=(await auth.get_access_token()),
        query=Gen3GraphQLQuery,
        variables={"filter": query_filter},
    )

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

    # return these for use in the statistics calculation
    durations = df["duration"]
    events = df["event"]
    kmf.fit(durations=durations, event_observed=events, label="Survival Curve")

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

    # Create a sorted list of time points
    timeline_sorted = sorted(kmf.timeline)

    for i, time_point in enumerate(timeline_sorted):
        survival_prob = survival_df.loc[time_point].iloc[0]

        # Only include donors who have events at this exact time point
        if time_point in donors_by_time:
            for donor in donors_by_time[time_point]:
                # For donors with events at this time point, use the survival probability
                # from BEFORE the event (i.e., the previous time point or current if censored)

                if donor["event_type"] == "death":
                    # For death events, use survival probability from the previous time point
                    if i > 0:
                        prev_time_point = timeline_sorted[i - 1]
                        donor_survival_prob = survival_df.loc[prev_time_point].iloc[0]
                    else:
                        # If this is the first time point, use 1.0
                        donor_survival_prob = 1.0
                else:
                    # For censored events, use the current survival probability
                    donor_survival_prob = survival_prob

                results.append(
                    {
                        "time": int(time_point),
                        "id": donor["id"],
                        "submitter_id": donor["submitter_id"],
                        "project_id": donor["project_id"],
                        "survivalEstimate": float(donor_survival_prob),
                        "censored": (
                            True if donor["event_type"] == "censored" else False
                        ),
                    }
                )

    return {
        "meta": {"id": id(results)},
        "donors": results,
        "durations": durations,
        "events": events,
    }


def calculate_survival_statistics(non_empty_curves: List[Dict]) -> Dict:
    """
    Calculate survival statistics for multiple curves using a log-rank test.

    Args:
        non_empty_curves: List of curve dictionaries containing durations, events, and donors

    Returns:
        Dictionary containing pValue and degreesFreedom, or empty dict if < 2 curves
    """
    statistics = {}
    if len(non_empty_curves) > 1:
        all_durations = []
        all_events = []
        for curve in non_empty_curves:
            all_durations.extend(curve["durations"])
            all_events.extend(curve["events"])

        groups = []
        for curve_index in range(len(non_empty_curves)):
            groups.extend([curve_index] * len(non_empty_curves[curve_index]["donors"]))

        log_rank_results = multivariate_logrank_test(all_durations, groups, all_events)
        statistics = {
            "pValue": log_rank_results.p_value,
            "degreesFreedom": len(non_empty_curves) - 1,
        }

    return statistics


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
async def plot(
    request: PlotRequest,
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
    auth: Auth = Depends(Auth),
) -> JSONResponse:
    filters = request.filters

    try:
        non_empty_curves = []
        for f in filters:
            curve = await get_curve(f, gen3_graphql_client, auth, request.req_opts)
            if curve:
                non_empty_curves.append(curve)

        statistics = calculate_survival_statistics(non_empty_curves)

        results = [
            {"meta": curve["meta"], "donors": curve["donors"]}
            for curve in non_empty_curves
        ]

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "results": results,
                "overallStats": statistics,
            },
        )

    except ValueError:
        raise HTTPException(status_code=500, detail="Error with survival calculation")
    except Exception:
        raise HTTPException(status_code=500)
