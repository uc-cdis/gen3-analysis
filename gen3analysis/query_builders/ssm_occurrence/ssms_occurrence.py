from typing import Dict, Optional, List, Any
from functools import lru_cache
from glom import glom
from gen3analysis.gen3.es_client import get_nested_registry
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.settings import settings
from gen3analysis.utils.filterEdit import (
    dot_notation_to_graphql,
    get_subfields,
)

DEFAULT_FIELDS = [
    "ssm_occurrence_id",
]


@lru_cache
def get_expandable_fields():
    ssm_registry = get_nested_registry()[settings.ES_SSM_OCCURRENCE_INDEX]
    nested_fields = []
    # Access the internal _by_field dictionary which contains all field information
    for field_path, field_info in ssm_registry._by_field.items():
        # Check if this field is nested (has nested_paths in its ancestry)
        if len(field_info.nested_paths) > 0:
            # Add all fields that are within a nested path
            nested_fields.append(field_path)
    return nested_fields


def process_item_fields(fields):
    results = ""
    for field in fields:
        results += dot_notation_to_graphql(field)
    return results


async def ssms_occurrence_query(
    gen3_graphql_client: GuppyGQLClient,
    filter=None,
    fields=None,
    expand=None,
    size=1,
    offset=0,
    access_token: Optional[str] = None,
):
    if filter is None:
        filter = {}
    field_snippets: List[str] = []

    expandable_fields = get_expandable_fields()

    if not fields:
        field_snippets.extend(DEFAULT_FIELDS)

    if expand:
        for field in expand:
            expand_fields = get_subfields(expandable_fields, field)
            for f in expand_fields:
                field_snippets.append(dot_notation_to_graphql(f))

    seen = set()
    query_fields = " ".join(x for x in field_snippets if not (x in seen or seen.add(x)))
    query = f"""
    query ssmsOccurrenceQuery($filter: JSON, $first: Int, $offset: Int, $accessibility: Accessibility) {{
    {settings.ssm_occurrence_centric_gql}(first: $first, offset:$offset, filter:$filter, accessibility:$accessibility) {{
            {query_fields}
            }}
    {settings.ssm_occurrence_centric_agg_gql} {{ {settings.SSM_OCCURRENCE_CENTRIC_INDEX}(filter:$filter, accessibility:$accessibility) {{
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

    hits = glom(
        data,
        f"data.{settings.ssm_occurrence_centric_gql}",
    )
    total = glom(
        data,
        f"data.{settings.ssm_occurrence_centric_agg_gql}.{settings.SSM_OCCURRENCE_CENTRIC_INDEX}._totalCount",
    )
    return {
        "data": hits,
        "pagination": {"total": total, "size": size, "offset": offset},
    }


async def ssms_occurrence_id_query(
    gen3_graphql_client: GuppyGQLClient,
    id: str,
    fields: Optional[List[str]] = None,
    expand: Optional[List[str]] = None,
    access_token: Optional[str] = None,
) -> Dict[str, Any]:
    field_snippets: List[str] = []

    expandable_fields = get_expandable_fields()

    if not fields:
        # Ensure DEFAULT_FIELDS is defined/imported appropriately
        field_snippets.extend(DEFAULT_FIELDS)
    else:
        # Convert each requested field
        for f in fields:
            field_snippets.append(dot_notation_to_graphql(f))

    # Handle expand
    if expand:
        for field in expand:
            expand_fields = get_subfields(expandable_fields, field)
            for f in expand_fields:
                field_snippets.append(dot_notation_to_graphql(f))

    seen = set()
    query_fields = " ".join(x for x in field_snippets if not (x in seen or seen.add(x)))

    query = f"""
     query ssmsOccurrenceIdQuery($filter: JSON, $accessibility: Accessibility) {{
     {settings.ssm_occurrence_centric_gql}(filter:$filter, first:1, offset:0, accessibility:$accessibility) {{
             {query_fields}
             }}
    }}"""

    return await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={"filter": {"in": {"ssm_id": [id]}}},
    )
