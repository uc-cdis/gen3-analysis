from typing import Dict

from gen3analysis.auth import Auth
from gen3analysis.gen3.guppyQuery import GuppyGQLClient


async def get_item_ids(
    gen3_graphql_client: GuppyGQLClient,
    auth: Auth,
    doc_type,
    item_field,
    guppy_filter: Dict,
    limit=10000,
):
    graphql_query = f"""query objectId ($filter: JSON) {{
            {doc_type}(first:{limit}, filter:$filter) {{
              {item_field}
              }}
    }}"""

    data = await gen3_graphql_client.execute(
        access_token=(await auth.get_access_token()),
        query=graphql_query,
        variables={"filter": guppy_filter},
    )

    return data
