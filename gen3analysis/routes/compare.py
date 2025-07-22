import json
from pydantic import BaseModel

from fastapi import APIRouter, Depends, HTTPException
from starlette.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR

from gen3analysis.config import logger
from gen3analysis.dependencies.guppy_client import get_guppy_client
from gen3analysis.gen3.guppyQuery import GuppyGQLClient


compare = APIRouter()


class FacetComparisonRequest(BaseModel):
    cohort1: dict
    cohort2: dict
    facets: list
    interval: float = 0


@compare.post("/facet", status_code=HTTP_200_OK)
async def compare_facet(
    body: FacetComparisonRequest,
    gen3_graphql_client: GuppyGQLClient = Depends(get_guppy_client),
) -> dict:
    """TODO"""

    try:
        query_old = """query CohortComparison(
            $cohort1: FiltersArgument
            $cohort2: FiltersArgument
            $facets: [String]!
            $interval: Float
        ) {
            viewer {
                explore {
                    cohort1: cases {
                        hits(filters: $cohort1) {
                            total
                        }
                        facets(filters: $cohort1, facets: $facets)
                        aggregations(filters: $cohort1) {
                            diagnoses__age_at_diagnosis {
                                stats {
                                    min
                                    max
                                }
                                histogram(interval: $interval) {
                                    buckets {
                                        doc_count
                                        key
                                    }
                                }
                            }
                        }
                    }
                    cohort2: cases {
                        hits(filters: $cohort2) {
                            total
                        }
                        facets(filters: $cohort2, facets: $facets)
                        aggregations(filters: $cohort2) {
                            diagnoses__age_at_diagnosis {
                                stats {
                                    min
                                    max
                                }
                                histogram(interval: $interval) {
                                    buckets {
                                        doc_count
                                        key
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """

        facets_query = " ".join(
            f"{facet} {{ histogram {{ key count }} }}" for facet in body.facets
        )
        query = f"""query ($cohort1: JSON, $cohort2: JSON){{
            cohort1: _aggregation {{
                case (filter: $cohort1) {{ {facets_query} }}
            }}
            cohort2: _aggregation {{
                case (filter: $cohort2) {{ {facets_query} }}
            }}
        }}"""

        print("gen3_graphql_client", gen3_graphql_client)
        data = await gen3_graphql_client.execute(
            query=query, variables={"cohort1": body.cohort1, "cohort2": body.cohort2}
        )
        print("data =", data)
        err = data.get("error") or data.get("errors")
        if err:
            raise ValueError(err)

        # TODO query age_at_diagnosis

    except:
        import traceback

        traceback.print_exc()
        raise

    try:
        res = {
            cohort: {
                "facets": {
                    facet: {
                        "buckets": [data["data"]["cohort1"]["case"][facet]["histogram"]]
                    }
                    for facet in body.facets
                }
            }
            for cohort in ["cohort1", "cohort2"]
        }
    except KeyError:
        err_msg = "Unable to parse GraphQL output"
        logger.error(f"{err_msg}: {json.dumps(data)}")
        raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, err_msg)

    return res
