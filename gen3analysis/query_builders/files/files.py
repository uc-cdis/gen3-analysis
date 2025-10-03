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
from gen3analysis.query_builders.files.summary_fields import file_metadata_fields
from glom import glom
import json


def files_query(
    gen3_graphql_client: GuppyGQLClient,
    filter: GQLFilter,
    fields=None,
    size=1,
    offset=0,
    access_token: Optional[str] = None,
):
    if fields is None:
        fields = ["file_id"]
    query = f"""
    query filesQuery($filter: JSON, $first: Int, $offset: Int, $accessibility: File_Accessibility)) {{
    File_file(first: $first, offset:$offset, filter:$filter, accessibility:$accessibility ) {{
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


def file_summary_query(
    gen3_graphql_client: GuppyGQLClient,
    id: str,
    access_token: Optional[str] = None,
):
    query = f"""
     query filesQuery($filter: JSON) {{
     File_file(filter:$filter, first:1, offset:0, accessibility:"all" ) {{
             {group_paths(file_metadata_fields)}
    }}"""

    return gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={"filter": {"in": {"file_id": [id]}}, "accessibility": "accessible"},
    )
