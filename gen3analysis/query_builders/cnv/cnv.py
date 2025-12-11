from typing import Dict, Optional, List, Any
from glom import glom
from functools import lru_cache

from gen3analysis.gen3.es_client import get_nested_registry
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.settings import settings
from gen3analysis.utils.filterEdit import (
    dot_notation_to_graphql,
    get_subfields,
)

DEFAULT_FIELDS = [
    "start_position",
    "gene_level_cn",
    "cnv_change",
    "ncbi_build",
    "chromosome",
    "cnv_id",
    "cnv_change_5_category",
    "end_position",
]


@lru_cache
def get_expandable_fields():
    cnv_registry = get_nested_registry()[settings.CNV_CENTRIC_INDEX]
    nested_fields = []
    # Access the internal _by_field dictionary which contains all field information
    for field_path, field_info in cnv_registry._by_field.items():
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


async def cnv_query(
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
    query cnvQuery($filter: JSON, $first: Int, $offset: Int, $accessibility: Accessibility) {{
    {settings.cnv_centric_gql}(first: $first, offset:$offset, filter:$filter, accessibility:$accessibility) {{
            {query_fields}
            }}
    {settings.cnv_centric_agg_gql} {{ {settings.CNV_CENTRIC_INDEX}(filter:$filter, accessibility:$accessibility) {{
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

    hits = glom(data, f"data.{settings.cnv_centric_gql}")
    total = glom(
        data,
        f"data.{settings.cnv_centric_agg_gql}.{settings.cnv_CENTRIC_INDEX}._totalCount",
    )
    return {
        "data": hits,
        "pagination": {"total": total, "size": size, "offset": offset},
    }


async def cnv_id_query(
    gen3_graphql_client: GuppyGQLClient,
    id: str,
    fields: Optional[List[str]] = None,
    expand: Optional[List[str]] = None,
    access_token: Optional[str] = None,
) -> Dict[str, Any]:
    field_snippets: List[str] = []

    if not fields:
        # Ensure DEFAULT_FIELDS is defined/imported appropriately
        field_snippets.extend(DEFAULT_FIELDS)
    else:
        # Convert each requested field
        for f in fields:
            field_snippets.append(dot_notation_to_graphql(f))

    expandable_fields = get_expandable_fields()

    # Handle expand
    if expand:
        for field in expand:
            expand_fields = get_subfields(expandable_fields, field)
            for f in expand_fields:
                field_snippets.append(dot_notation_to_graphql(f))

    seen = set()
    query_fields = " ".join(x for x in field_snippets if not (x in seen or seen.add(x)))

    # Use the correct cnv index; adjust if your schema uses a different name
    query = f"""
    query cnvQuery($filter: JSON, $accessibility: Accessibility) {{
    {settings.cnv_centric_gql}(filter:$filter, first:1, offset:0, accessibility:$accessibility) {{
         {query_fields}
         }}
    }}"""

    return await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={"filter": {"in": {"cnv_id": [id]}}},
    )
