from typing import Dict, Optional, List, Any
from glom import glom
from gen3analysis.settings import logger

from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from gen3analysis.settings import settings
from gen3analysis.utils.filterEdit import dot_notation_to_graphql


async def download_query(
    gen3_graphql_client: GuppyGQLClient,
    cohort_filter: Dict,
    filter: Dict,
    case_ids_filter_path: str,
    index: str,
    cohort_item_field: str = "case_id",
    case_index: str = "CaseCentric_case_centric",
    fields: Optional[List[str]] = None,
    access_token: Optional[str] = None,
):
    try:

        # NOTE: Eventually have to switch this query to the download API or ES when number of cases > 10K
        cohort_query = f"""query objectIds ($cohort_filters: JSON) {{
                {case_index}(first:{settings.MAX_CASES}, filter:$cohort_filters) {{
                              {dot_notation_to_graphql(cohort_item_field)}
                  }}
        }}"""

        data = await gen3_graphql_client.execute(
            access_token=access_token,
            query=cohort_query,
            variables={"cohort_filters": cohort_filter},
        )

        if (data.get("data") is None) or (data.get("data").get(case_index) is None):
            return {"hits": [], "total": 0}
        case_ids = [
            glom(x, cohort_item_field) for x in glom(data, f"data.{case_index}")
        ]

        # build a filter containing the cohort ids and merge with the other filters
        ids = case_ids

        if "and" in filter:
            filter["and"].append({"in": {case_ids_filter_path: ids}})
        else:
            filter["and"] = [{"in": {case_ids_filter_path: ids}}]

        payload = {
            "type": index,
            "filter": filter,
            "fields": fields,
        }

        results = await gen3_graphql_client.download(
            access_token=access_token,
            payload=payload,
        )

        return results
    except Exception as e:
        logger.error(f"Error while processing cohort query: {e}")
        raise e
