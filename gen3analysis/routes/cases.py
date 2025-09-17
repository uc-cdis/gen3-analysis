from typing import Dict, Optional, List
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.utils.filterEdit import (
    dot_notation_to_graphql,
    update_filters_with_object_ids,
)
from glom import glom
import json


def process_item_fields(fields):
    results = ""
    for field in fields:
        results += dot_notation_to_graphql(field)
    return results


async def get_item_ids(
    gen3_graphql_client: GuppyGQLClient,
    doc_type: str,
    item_fields: List[str],
    guppy_filter: Dict,
    limit=10000,
    access_token: Optional[str] = None,
):

    graphql_query = f"""query objectId ($filter: JSON) {{
            {doc_type}(first:{limit}, filter:$filter) {{
             {process_item_fields(item_fields)}
              }}
    }}"""

    print("executing query", graphql_query)
    print("executing variables", json.dumps(guppy_filter, indent=2))

    data = await gen3_graphql_client.execute(
        access_token=access_token,
        query=graphql_query,
        variables={"filter": guppy_filter},
    )

    return data


async def cohort_query(
    gen3_graphql_client: GuppyGQLClient,
    case_index: str,
    cohort_item_field: str,
    cohort_filters: Dict,
    filters: Dict,
    query: str,
    access_token: Optional[str] = None,
    limit=10000,
):
    # Get the cohort items by id
    cohort_query = f"""query objectIds ($cohort_filters: JSON) {{
            {case_index}(first:{limit}, filter:$cohort_filters) {{
                          {dot_notation_to_graphql(cohort_item_field)}
              }}
    }}"""
    data = await gen3_graphql_client.execute(
        access_token=access_token,
        query=cohort_query,
        variables={"cohort_filters": cohort_filters},
    )

    if (data.get("data") is None) or (data.get("data").get(case_index) is None):
        return {"hits": [], "total": 0}
    case_ids = [glom(x, cohort_item_field) for x in glom(data, f"data.{case_index}")]

    # build a filter containing the cohort ids and merge with the other filters
    ids = case_ids

    update_filters_with_object_ids(filters, "case_id", ids)

    return await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables=filters,
    )
