import json
from pydantic import BaseModel

from fastapi import APIRouter, Depends, HTTPException
from starlette.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR

from gen3analysis.config import logger
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.gen3.guppyQuery import GuppyGQLClient


compare = APIRouter()


class FacetComparisonRequest(BaseModel):
    doc_type: str
    cohort1: dict
    cohort2: dict
    facets: list
    interval: float = 0


def facet_name_to_props(facet_name):
    # For now, just split by `.` - we may need to support property names with `.` later,
    # maybe by escaping them: `\.`
    return facet_name.split(".")


@compare.post("/facets", status_code=HTTP_200_OK)
async def compare_facet(
    body: FacetComparisonRequest,
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
) -> dict:
    """
    TODO
    """
    facets_query = ""
    for facet in body.facets:
        props = facet_name_to_props(facet)
        facets_query += " ".join(f"{prop} {{" for prop in props) + " "
        facets_query += "histogram { key count } "
        facets_query += " ".join("}" for _ in props) + " "

    query = f"""query ($cohort1: JSON, $cohort2: JSON){{
        cohort1: _aggregation {{
            {body.doc_type} (filter: $cohort1) {{ {facets_query} }}
        }}
        cohort2: _aggregation {{
            {body.doc_type} (filter: $cohort2) {{ {facets_query} }}
        }}
    }}"""
    # TODO age_at_diagnosis query

    # print(query)
    # print(json.dumps({"cohort1": body.cohort1, "cohort2": body.cohort2}, indent=2))
    data = await gen3_graphql_client.execute(
        query=query, variables={"cohort1": body.cohort1, "cohort2": body.cohort2}
    )

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


@compare.post("/venn", status_code=HTTP_200_OK)
async def compare_facet(
    body: FacetComparisonRequest,
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
) -> dict:
    """
    TODO
    """
    query = f"""query ($cohort1: JSON, $cohort2: JSON){{
        cohort1: _aggregation {{
            case (filter: $cohort1) {{ {facets_query} }}
        }}
        cohort2: _aggregation {{
            case (filter: $cohort2) {{ {facets_query} }}
        }}
    }}"""
    data = await gen3_graphql_client.execute(
        query=query, variables={"cohort1": body.cohort1, "cohort2": body.cohort2}
    )
    print("data =", data)
    res = data
    return res

"""
{
    "data": {
        "viewer": {
            "explore": {
                "intersection": {
                    "hits": {
                        "total": 0
                    }
                },
                "set1": {
                    "hits": {
                        "total": 429
                    }
                },
                "set2": {
                    "hits": {
                        "total": 84
                    }
                }
            }
        }
    }
}
"""