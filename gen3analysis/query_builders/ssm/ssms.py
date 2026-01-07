from typing import Dict, Optional, List, Any
from functools import lru_cache
from glom import glom

from gen3analysis.gen3.es_client import get_nested_registry
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.query_builders.utils.get_query_fields import get_query_fields
from gen3analysis.query_builders.utils.normalize_csv_or_list import (
    normalize_csv_or_list,
)
from gen3analysis.settings import settings
from gen3analysis.utils.filterEdit import (
    dot_notation_to_graphql,
)

DEFAULT_FIELDS = [
    "start_position",
    "gene_aa_change",
    "reference_allele",
    "ncbi_build",
    "cosmic_id",
    "mutation_subtype",
    "mutation_type",
    "chromosome",
    "genomic_dna_change",
    "tumor_allele",
    "end_position",
    "ssm_id",
]


@lru_cache
def get_expandable_fields():
    ssm_registry = get_nested_registry()[settings.ES_SSM_CENTRIC_INDEX]
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


async def ssms_query(
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

    expandable_fields = get_expandable_fields()
    fields = normalize_csv_or_list(fields)
    expand = normalize_csv_or_list(expand)
    query_fields = get_query_fields(fields, expand, expandable_fields, DEFAULT_FIELDS)
    query = f"""
    query ssmsQuery($filter: JSON, $size: Int, $offset: Int, $accessibility: Accessibility) {{
    hits: {settings.ssm_centric_gql}(first: $size, offset:$offset, filter:$filter, accessibility:$accessibility) {{
            {query_fields}
            }}
    {settings.ssm_centric_agg_gql} {{ totals: {settings.SSM_CENTRIC_INDEX}(filter:$filter, accessibility:$accessibility) {{
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

    hits = glom(data, f"data.{settings.ssm_centric_gql}")
    total = glom(
        data,
        f"data.{settings.ssm_centric_agg_gql}.{settings.SSM_CENTRIC_INDEX}._totalCount",
    )
    return {
        "data": hits,
        "pagination": {"total": total, "size": size, "offset": offset},
    }


async def ssms_id_query(
    gen3_graphql_client: GuppyGQLClient,
    id: str,
    fields: Optional[List[str]] = None,
    expand: Optional[List[str]] = None,
    access_token: Optional[str] = None,
) -> Dict[str, Any]:
    field_snippets: List[str] = []

    expandable_fields = get_expandable_fields()
    fields = normalize_csv_or_list(fields)
    expand = normalize_csv_or_list(expand)
    query_fields = get_query_fields(fields, expand, expandable_fields, DEFAULT_FIELDS)

    # Use the correct SSM index; adjust if your schema uses a different name
    index_name = settings.SSM_CENTRIC_INDEX

    query = f"""
    query ssmsQuery($filter: JSON, $accessibility: Accessibility) {{
    hits: {settings.ssm_centric_gql}(filter:$filter, first:1, offset:0, accessibility:$accessibility) {{
         {query_fields}
         }}
    }}"""

    return await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={"filter": {"in": {"ssm_id": [id]}}},
    )
