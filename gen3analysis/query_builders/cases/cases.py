from typing import Dict, Optional, List
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.utils.filterEdit import (
    dot_notation_to_graphql,
    update_filters_with_object_ids,
)
from gen3analysis.filters.gen3GQLFilters import (
    GQLFilter,
)
from gen3analysis.utils.group import group_paths
from gen3analysis.query_builders.cases.summary_fields import case_metadata_fields
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


def cases_query(
    gen3_graphql_client: GuppyGQLClient,
    filter: GQLFilter,
    fields=None,
    size=1,
    offset=0,
    access_token: Optional[str] = None,
):
    if fields is None:
        fields = ["case_id"]
    query = f"""
    query casesQuery($filter: JSON, $first: Int, $offset: Int, $accessibility: CaseCentric_Accessibility)) {{
    CaseCentric_case_centric(first: $first, offset:$offset, filter:$filter, accessibility:$accessibility ) {{
            {group_paths(fields)}
   }}"""

    return gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={
            "filter": filter,
            "size": size,
            "offset": offset,
            "accessibility": "accessible",
        },
    )


def case_summary_query(
    gen3_graphql_client: GuppyGQLClient,
    id: str,
    access_token: Optional[str] = None,
):
    query = f"""
     query casesQuery($filter: JSON) {{
     CaseCentric_case_centric(filter:$filter, first:1, offset:0, accessibility:"all" ) {{
             {group_paths(case_metadata_fields)}
    }}"""

    return gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={"filter": {"in": {"case_id": [id]}}, "accessibility": "accessible"},
    )
