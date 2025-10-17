from typing import Dict, Optional, List
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.settings import settings
from gen3analysis.filters.gen3GQLFilters import (
    GQLFilter,
)
from gen3analysis.utils.group import build_fields_query_body
from gen3analysis.query_builders.files.summary_fields import file_metadata_fields


async def files_query(
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
    {settings.FILE_INDEX}(first: $first, offset:$offset, filter:$filter, accessibility:$accessibility ) {{
            {build_fields_query_body(fields)}
   }}"""

    return await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={
            "filter": filter,
            "size": size,
            "offset": offset,
            "accessibility": "accessible",
        },
    )


async def file_summary_query(
    gen3_graphql_client: GuppyGQLClient,
    id: str,
    access_token: Optional[str] = None,
):
    query = f"""
     query filesQuery($filter: JSON) {{
     {settings.FILE_INDEX}(filter:$filter, first:1, offset:0, accessibility:accessible ) {{
             {build_fields_query_body(file_metadata_fields)}
             }}
    }}"""

    return await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={"filter": {"in": {"file_id": [id]}}},
    )
