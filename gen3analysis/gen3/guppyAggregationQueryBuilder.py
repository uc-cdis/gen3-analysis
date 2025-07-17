from typing import Dict, List, Any, Optional
from enum import Enum
from gen3analysis.filters.convertFiltersToGen3GQLFilters import (
    convert_filter_set_to_gql,
)
from gen3analysis.filters.filters import FilterSet


class Accessibility(Enum):
    ALL = "ALL"
    ACCESSIBLE = "ACCESSIBLE"
    UNACCESSIBLE = "UNACCESSIBLE"


class GraphQLQuery:
    def __init__(self, query: str, variables: Optional[Dict[str, Any]] = None):
        self.query = query
        self.variables = variables or {}


def is_filter_empty(filters: Dict[str, Any]) -> bool:
    """Check if filters are empty or None."""
    if not filters:
        return True
    # Add specific logic based on your FilterSet structure
    root = filters.get("root", {})
    return not root or not any(root.values())


def histogram_query_str_for_each_field(field: str) -> str:
    """Generate a histogram query string for a field."""
    # This function needs to be implemented based on your requirements
    return f"{field} {{ /* histogram query for {field} */ }}"


def build_get_aggregation_query(
    type_name: str,
    fields: List[str],
    filters: FilterSet,
    accessibility: Accessibility = Accessibility.ALL,
    filter_self: bool = False,
) -> GraphQLQuery:
    """
    Build GraphQL aggregation query from type, fields, and filters.

    Args:
        type_name: The GraphQL type name
        fields: List of field names to aggregate
        filters: FilterSet dictionary
        accessibility: Accessibility level (default: ALL)
        filter_self: Whether to filter self (default: False)

    Returns:
        GraphQLQuery object with query string and variables
    """
    if is_filter_empty(filters):
        query_start = f"""
              query getAggs {{
              _aggregation {{
              {type_name} (accessibility: {accessibility.value}) {{"""
    else:
        filter_self_str = "true" if filter_self else "false"
        query_start = f"""query getAggs ($filter: JSON) {{
               _aggregation {{
                      {type_name} (filter: $filter, filterSelf: {filter_self_str}, accessibility: {accessibility.value}) {{"""

    # Generate field queries
    field_queries = []
    for field in fields:
        field_queries.append(histogram_query_str_for_each_field(field))

    field_queries_str = "\n                  ".join(field_queries)

    query = f"""{query_start}
                  {field_queries_str}
                }}
              }}
            }}"""

    query_body = GraphQLQuery(
        query=query, variables={"filter": convert_filter_set_to_gql(filters)}
    )

    return query_body
