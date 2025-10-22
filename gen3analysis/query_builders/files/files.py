from typing import Optional
from glom import glom
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
    query filesQuery($filter: JSON, $first: Int, $offset: Int, $accessibility: Accessibility) {{
    {settings.file_gql}(first: $first, offset:$offset, filter:$filter, accessibility:$accessibility ) {{
            {build_fields_query_body(fields)}
    }}
    {settings.file_agg_gql} {{ {settings.FILE_INDEX}(filter:$filter, accessibility:$accessibility) {{
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

    hits = glom(data, f"data.{settings.file_gql}")
    total = glom(
        data, f"data.{settings.file_agg_gql}.{settings.FILE_INDEX}._totalCount"
    )
    return {
        "data": hits,
        "pagination": {"total": total, "size": size, "offset": offset},
    }


async def file_summary_query(
    gen3_graphql_client: GuppyGQLClient,
    id: str,
    access_token: Optional[str] = None,
):
    query = f"""
     query filesQuery($filter: JSON) {{
     {settings.file_gql}(filter:$filter, first:1, offset:0, accessibility:accessible ) {{
             {build_fields_query_body(file_metadata_fields)}
             }}
    }}"""

    return await gen3_graphql_client.execute(
        access_token=access_token,
        query=query,
        variables={"filter": {"in": {"file_id": [id]}}},
    )
