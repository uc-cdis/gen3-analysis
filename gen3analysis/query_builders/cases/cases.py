import copy
from typing import Dict, Optional, List, Any
from glom import glom

from gen3analysis.filters.gen3GQLFilters import parse_gql_filter
from gen3analysis.settings import logger
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.query_builders.cases.summary_fields import case_metadata_fields
from gen3analysis.query_builders.ssm_occurrence.ssms_occurrence import DEFAULT_FIELDS
from gen3analysis.query_builders.genomic.queries import query_case_ids
from gen3analysis.settings import settings
from gen3analysis.utils.filterEdit import dot_notation_to_graphql
from gen3analysis.utils.group import build_fields_query_body

DEFAULT_FIELDS = ["case_id"]


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
    limit=settings.MAX_CASES,
    access_token: Optional[str] = None,
):
    graphql_query = f"""query objectId ($filter: JSON) {{
            {doc_type}(first:{limit}, filter:$filter) {{
             {process_item_fields(item_fields)}
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
    case_index: str,
    cohort_item_field: str,
    cohort_filter: Dict,
    filter: Dict,
    case_ids_filter_path: str,
    query: str,
    access_token: Optional[str] = None,
    limit=10000,
    sort: Optional[List[Dict]] = None,
):
    """
    Executes a cohort-oriented query using a Gen3 GraphQL client and retrieves results
    based on specified filters and cohort items.

    Parameters:
        gen3_graphql_client (GuppyGQLClient): The Gen3 GraphQL client used to execute
            the queries.
        case_index (str): The index of the case or entity to be queried.
        cohort_item_field (str): The field of the cohort items from which ids will
            be extracted.
        cohort_filter (Dict): Filters to apply when retrieving the initial cohort
            items.
        filters (Dict): Additional filters to merge with the cohort item-related
            ids for the final query.
        query (str): The GraphQL query to be executed after filtering for cohort
            item ids.
        access_token (Optional[str]): Optional access token to authorize the client
            for API execution. Defaults to None.
        limit (int): The maximum number of cohort items/entities to retrieve in the
            first query. Defaults to 10000.

    Returns:
        dict: A dictionary containing the results of the final query, including
            entities matching the filters.

    Raises:
        None
    """
    # Get the cohort items by id

    try:
        # Get the cohort ids using elastic search
        cohort_filter_gql = parse_gql_filter(cohort_filter)
        case_ids = query_case_ids(cohort_filter_gql)
        # Work on a copy so the input filter is not mutated.
        query_filter = copy.deepcopy(filter) if filter else {}

        if "and" in filter:
            query_filter["and"].append({"in": {case_ids_filter_path: case_ids}})
        else:
            query_filter["and"] = [{"in": {case_ids_filter_path: case_ids}}]

        variables = {"filter": filter}
        if sort is not None:
            variables["sort"] = sort

        return await gen3_graphql_client.execute(
            access_token=access_token,
            query=query,
            variables=variables,
        )
    except Exception as e:
        logger.error(f"Error while processing cohort query: {e}")
        raise e


async def cases_query(
    gen3_graphql_client: GuppyGQLClient,
    filter: Dict,
    fields=None,
    size=1,
    offset=0,
    access_token: Optional[str] = None,
):
    field_snippets: List[str] = []
    if not fields:
        # Ensure DEFAULT_FIELDS is defined/imported appropriately
        field_snippets.extend(DEFAULT_FIELDS)
    else:
        # Convert each requested field
        for f in fields:
            field_snippets.append(dot_notation_to_graphql(f))

    query = f"""
    query casesMetadataQuery($filter: JSON, $size: Int, $offset: Int, $accessibility: Accessibility) {{
    {settings.case_centric_gql}(first: $size, offset:$offset, filter:$filter, accessibility:$accessibility) {{
            {build_fields_query_body(fields)}
            }}
    {settings.case_centric_agg_gql} {{ {settings.CASE_CENTRIC_INDEX}(filter:$filter, accessibility:$accessibility) {{
            _totalCount
            }}
    }}
   }}"""

    data = await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={
            "filter": filter,
            "size": size,
            "offset": offset,
            "accessibility": "accessible",
        },
    )
    hits = glom(data, f"data.{settings.case_centric_gql}")
    total = glom(
        data,
        f"data.{settings.case_centric_agg_gql}.{settings.CASE_CENTRIC_INDEX}._totalCount",
    )
    return {
        "data": hits,
        "pagination": {"total": total, "size": size, "offset": offset},
    }


async def case_summary_query(
    gen3_graphql_client: GuppyGQLClient,
    id: str,
    access_token: Optional[str] = None,
) -> Dict[str, Any]:
    query = f"""
     query caseSummaryQuery($filter: JSON) {{
     {settings.case_centric_gql}(filter:$filter, first:1, offset:0, accessibility:accessible) {{
             {build_fields_query_body(case_metadata_fields)}
             }}
    }}"""

    data = await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={"filter": {"in": {"case_id": [id]}}},
    )
    return data
