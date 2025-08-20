from typing import Dict, Optional
from gen3analysis.gen3.guppyQuery import GuppyGQLClient


async def get_item_ids(
    gen3_graphql_client: GuppyGQLClient,
    doc_type,
    item_field,
    guppy_filter: Dict,
    limit=10000,
    access_token: Optional[str] = None,
):
    graphql_query = f"""query objectId ($filter: JSON) {{
            {doc_type}(first:{limit}, filter:$filter) {{
              {item_field}
              }}
    }}"""

    data = await gen3_graphql_client.execute(
        access_token=access_token,
        query=graphql_query,
        variables={"filter": guppy_filter},
    )

    return data


async def cohort_query(
    gen3_graphql_client: GuppyGQLClient,
    doc_type: str,
    item_field: str,
    cohort_filter: Dict,
    filters: Dict,
    query: str,
    access_token: Optional[str] = None,
    limit=10000,
):
    # Get the cohort items by id
    cohort_query = f"""query objectId ($filter: JSON) {{
            {doc_type}(first:{limit}, filter:$filter) {{
                          {item_field}
              }}
    }}"""
    data = await gen3_graphql_client.execute(
        access_token=access_token,
        query=cohort_query,
        variables={"filter": cohort_filter},
    )

    if (data.get("data") is None) or (data.get("data").get(doc_type) is None):
        return []

    # build a filter containing the cohort ids and merge with the other filters
    ids = data.get("data").get(doc_type)
    id_filters = f"{{ in: {{ {item_field} : {ids} }} }}"
    merged_filters = {"and": [filters, id_filters]}

    return await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={"filter": merged_filters},
    )
