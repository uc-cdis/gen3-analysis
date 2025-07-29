import json
from pydantic import BaseModel
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException
from starlette.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR

from gen3analysis.auth import Auth
from gen3analysis.config import logger
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.gen3.guppyQuery import GuppyGQLClient


compare = APIRouter()


class FacetComparisonRequest(BaseModel):
    doc_type: str
    cohort1: dict
    cohort2: dict
    facets: list
    interval: Dict[str, int] = {}


def facet_name_to_props(facet_name) -> list:
    """
    Split a nested facet name into a list of properties.
    Example: input "abc.efg" => output ["abc", "efg"]

    Args:
        facet_name (str)

    Returns:
        list
    """
    # For now, just split by `.` - we may need to support property names with `.` later,
    # maybe by escaping them: `\.`
    return facet_name.split(".")


@compare.post("/facets", status_code=HTTP_200_OK)
async def compare_facets(
    body: FacetComparisonRequest,
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
    auth: Auth = Depends(Auth),
) -> dict:
    """
    Compare facets between two cohorts.

    Args:

        doc_type: the cohorts' ES document type

        cohort1: filter corresponding to the first cohort to compare

        cohort2: filter corresponding to the second cohort to compare

        facets: fields to compare

        interval: dictionary of intervals for numerical facets.

          Example: `facets=["numeric_field"]` and `interval={"numeric_field": 10}`

    Returns:
        dict - example:

            {
                "cohort1": {
                    "facets": {
                        "text_field": {
                            "buckets": [
                                {"key": "value1", "count": 99},
                                {"key": "value2", "count": 45},
                            ],
                        },
                        "numeric_field": {
                            "buckets": [
                                {"key": [20, 30], "count": 100},
                                {"key": [30, 40], "count": 44},
                            ],
                        },
                    }
                },
                "cohort2": {
                    "facets": { [...] }
                },
            }
    """
    # build the GraphQL query: query a histogram of values for each requested facet
    facets_query = ""
    for facet in body.facets:
        props = facet_name_to_props(facet)

        # query the fields
        facets_query += " ".join(f"{prop} {{" for prop in props) + " "

        # for numeric fields, add `rangeStep` parameter as specified in `interval` input
        params = (
            f"(rangeStep: {body.interval[facet]})" if facet in body.interval else ""
        )

        # query the histogram for this field
        facets_query += f"histogram{params} {{ key count }} "

        facets_query += " ".join("}" for _ in props) + " "

    # apply this query to each of the 2 cohorts
    query = f"""query ($cohort1: JSON, $cohort2: JSON){{
        cohort1: _aggregation {{
            {body.doc_type} (filter: $cohort1) {{ {facets_query} }}
        }}
        cohort2: _aggregation {{
            {body.doc_type} (filter: $cohort2) {{ {facets_query} }}
        }}
    }}"""

    data = await gen3_graphql_client.execute(
        access_token=(await auth.get_access_token()),
        query=query,
        variables={"cohort1": body.cohort1, "cohort2": body.cohort2},
    )

    # parse and transform the output
    try:
        res = {}
        for cohort in ["cohort1", "cohort2"]:
            res[cohort] = {"facets": {}}
            for facet in body.facets:
                _data = data["data"][cohort][body.doc_type]
                props = facet_name_to_props(facet)
                for prop in props:
                    _data = _data[prop]
                res[cohort]["facets"][facet] = {"buckets": _data["histogram"]}
    except KeyError as e:
        err_msg = f"Unable to parse GraphQL output: KeyError {e}"
        logger.error(f"{err_msg}. Output: {json.dumps(data)}")
        raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, err_msg)

    return res


class IntersectionRequest(BaseModel):
    doc_type: str
    cohort1: dict
    cohort2: dict
    # the default precision threshold is 3000 according to
    # https://www.elastic.co/docs/reference/aggregations/search-aggregations-metrics-cardinality-aggregation#_precision_control
    precision_threshold: int = 3000


@compare.post("/intersection", status_code=HTTP_200_OK)
async def get_cohort_intersection(
    body: IntersectionRequest,
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
    auth: Auth = Depends(Auth),
) -> dict:
    """
    Get the number of documents at the intersection between two cohorts, as well as the number
    of documents that only belong to either one of the cohorts. Useful to generate Venn diagrams.

    Args:

        doc_type: the cohorts' ES document type

        cohort1: filter corresponding to the first cohort to compare

        cohort2: filter corresponding to the second cohort to compare

        precision_threshold (default: 3000): option to trade memory for accuracy when querying cardinality in ES

    Returns:
        dict - example:

            {
                "cohort1": <number of documents that are in cohort1 and not in cohort2>,
                "cohort2": <number of documents that are in cohort2 and not in cohort1>,
                "intersection": <number of documents that are in both cohorts>,
            }
    """
    # Build the GraphQL query: query the cardinality count (number of unique values) of IDs. In
    # other words, query the number of documents in each cohort and in their intersection.
    # Note: this query assumes that there is a field named `_<doc_type>_id`, which should be the
    # case for data generated by the Gen3 Tube ETL.
    query = f"""query ($cohort1: JSON, $cohort2: JSON, $intersection: JSON) {{
        cohort1: _aggregation {{
            {body.doc_type} (filter: $cohort1) {{
                _{body.doc_type}_id {{
                    _cardinalityCount(precision_threshold: {body.precision_threshold})
                }}
            }}
        }}
        cohort2: _aggregation {{
            {body.doc_type} (filter: $cohort2) {{
                _{body.doc_type}_id {{
                    _cardinalityCount(precision_threshold: {body.precision_threshold})
                }}
            }}
        }}
        intersection: _aggregation {{
            {body.doc_type} (filter: $intersection) {{
                _{body.doc_type}_id {{
                    _cardinalityCount(precision_threshold: {body.precision_threshold})
                }}
            }}
        }}
    }}"""

    data = await gen3_graphql_client.execute(
        access_token=(await auth.get_access_token()),
        query=query,
        variables={
            "cohort1": body.cohort1,
            "cohort2": body.cohort2,
            "intersection": {"AND": [body.cohort1, body.cohort2]},
        },
    )

    # parse and transform the output
    try:
        n_intersection = data["data"]["intersection"][body.doc_type][
            f"_{body.doc_type}_id"
        ]["_cardinalityCount"]
        res = {
            cohort: data["data"][cohort][body.doc_type][f"_{body.doc_type}_id"][
                "_cardinalityCount"
            ]
            - n_intersection
            for cohort in ["cohort1", "cohort2"]
        }
        res["intersection"] = n_intersection
    except KeyError as e:
        err_msg = f"Unable to parse GraphQL output: KeyError {e}"
        logger.error(f"{err_msg}. Output: {json.dumps(data)}")
        raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, err_msg)

    return res
