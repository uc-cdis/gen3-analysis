"""
This module provides functionality to build complex nested queries and aggregations
for genomic data
"""

import asyncio
from typing import List, Dict, Any, Optional

from elasticsearch_dsl import Search, Q, A

from gen3analysis.filters.es.convert_gql_to_elastic_search import (
    convert_gql_to_elastic_search,
)

from gen3analysis.filters.gen3GQLFilters import get_gql_filter_contents, GQLFilter
from gen3analysis.gen3.es_client import get_es
from gen3analysis.query_builders.genomic.queries import query_case_ids
from gen3analysis.query_builders.utils.combine_nested import (
    combine_nested_queries_simple,
)

from gen3analysis.settings import settings

aggregation_fields = [
    "consequence.transcript.consequence_type",
    "consequence.transcript.annotation.sift_impact",
    "consequence.transcript.annotation.polyphen_impact",
    "consequence.transcript.annotation.vep_impact",
]


def extract_aggregation_results(
    response: Dict[str, Any], aggregation_field: str
) -> List[Dict[str, Any]]:
    """
    Extract and format results from a specific aggregation field.

    Args:
        response: The Elasticsearch response dictionary
        aggregation_field: The field to extract results for

    Returns:
        List of dictionaries containing bucket information
    """
    agg_name = f"{aggregation_field}:filtered"

    try:
        # Try the filtered path first (when consequence filters are applied)
        try:
            agg_data = response["aggregations"]["consequence:global"][
                "consequence:filtered"
            ]["consequence"][agg_name][aggregation_field]
        except KeyError:
            # Try the unfiltered path (when no consequence filters)
            agg_data = response["aggregations"]["consequence:global"][
                "consequence:filtered"
            ]["consequence"][aggregation_field]

        buckets = agg_data.get("buckets", [])

        results = []
        for bucket in buckets:
            result = {
                "key": bucket["key"],
                "doc_count": bucket["doc_count"],
                "parent_count": bucket.get("rn", {}).get("doc_count", 0),
            }
            results.append(result)

        return results

    except KeyError as e:
        raise ValueError(
            f"Could not find aggregation data for field {aggregation_field}: {e}"
        )


def transform_to_graphql_response(
    response: Dict[str, Any],
    aggregation_fields: List[str],
) -> Dict[str, Any]:
    """
    Transform Elasticsearch aggregation response to GDC GraphQL format.

    This function converts the ES response structure into the hierarchical format
    used by GDC's GraphQL API, organizing aggregations by their field paths.

    Args:
        response: The Elasticsearch response dictionary from build_gdc_consequence_aggregation
        aggregation_fields: List of fields that were aggregated on
        entity_name: Name of the entity type (default: "ssm_centric")

    Returns:
        Dictionary in GDC GraphQL format with nested structure

    """
    # Get total count
    total_count = response.get("hits", {}).get("total", {}).get("value", 0)

    # Initialize the result structure
    result = {
        "data": {"_totalCount": total_count, "mutation_subtype": {"histogram": []}}
    }

    # Get shorthand reference to the entity
    entity = result["data"]

    # Add mutation_subtype aggregation (top-level, not nested)
    if "mutation_subtype" in response.get("aggregations", {}):
        for bucket in response["aggregations"]["mutation_subtype"]["buckets"]:
            entity["mutation_subtype"]["histogram"].append(
                {"key": bucket["key"], "count": bucket["doc_count"]}
            )

    # Process nested consequence aggregations
    consequence_aggs = {}

    for agg_field in aggregation_fields:
        # Extract the field data
        try:
            buckets_data = extract_aggregation_results(response, agg_field)
        except ValueError:
            # Skip fields that don't have data
            continue

        # Parse the field path to build nested structure
        # e.g., "consequence.transcript.annotation.vep_impact"
        # becomes nested dict structure
        parts = agg_field.split(".")

        # Skip the first part if it's 'consequence' (we know it's nested)
        if parts[0] == "consequence":
            parts = parts[1:]

        # Build the nested structure
        current = consequence_aggs
        for i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            current = current[part]

        # Add the histogram at the leaf level
        field_name = parts[-1]
        current[field_name] = {
            "histogram": [
                {
                    "key": bucket["key"],
                    "count": bucket["parent_count"],  # Use parent count (SSM count)
                }
                for bucket in buckets_data
            ]
        }

    # Add the consequence structure to the entity
    if consequence_aggs:
        entity["consequence"] = consequence_aggs

    return result


async def build_ssm_consequence_aggregation(
    case_ids: List[str],
    filters: Optional[List[dict]],
    size: int = 64,
) -> Dict[str, Any]:
    # Default aggregation fields

    # Initialize the search object
    s = Search(using=get_es(), index=settings.ES_SSM_CENTRIC_INDEX)

    # Set size to 0 since we only want aggregations
    s = s.extra(size=0)

    # Build the nested query for occurrence (case filtering)
    occurrence_query = Q(
        "nested",
        path="occurrence",
        ignore_unmapped=True,
        query=Q("bool", must=[Q("terms", occurrence__case__case_id=case_ids)]),
    )

    query_list = [occurrence_query]
    consequence_must_clauses = []
    if filters is not None and len(filters) > 0:
        # TODO Fix hardcoded path
        consequence_query = filters[0]  # all of these are nested
        consequence_must_clauses = consequence_query.query.to_dict()["bool"]["must"]
        query_list.append(consequence_query)

    # Build aggregations
    # We use a global aggregation approach similar to GDC

    # Global aggregation to get all documents
    global_agg = A("global")
    s.aggs.bucket("consequence:global", global_agg)

    # Filter aggregation within global to apply occurrence filter
    filtered_agg = A(
        "filter",
        Q(
            "nested",
            path="occurrence",
            ignore_unmapped=True,
            query=Q("bool", must=[Q("terms", occurrence__case__case_id=case_ids)]),
        ),
    )
    s.aggs["consequence:global"].bucket("consequence:filtered", filtered_agg)

    # Nested aggregation for a consequence
    nested_consequence_agg = A("nested", path="consequence")
    s.aggs["consequence:global"]["consequence:filtered"].bucket(
        "consequence", nested_consequence_agg
    )

    # Add aggregations for each requested field
    for agg_field in aggregation_fields:
        # Create a clean aggregation name
        agg_name = f"{agg_field}:filtered"
        field_name = agg_field.split(".")[-1]  # Get the last part of the field path

        # Apply the same filters within the aggregation
        filter_agg = A("filter", Q("bool", must=consequence_must_clauses))

        s.aggs["consequence:global"]["consequence:filtered"]["consequence"].bucket(
            agg_name, filter_agg
        )

        # Terms aggregation on the field
        terms_agg = A("terms", field=agg_field, size=size)
        s.aggs["consequence:global"]["consequence:filtered"]["consequence"][
            agg_name
        ].bucket(agg_field, terms_agg)

        # Reverse nested to count parent documents
        reverse_nested_agg = A("reverse_nested")
        s.aggs["consequence:global"]["consequence:filtered"]["consequence"][agg_name][
            agg_field
        ].bucket("rn", reverse_nested_agg)

    # Mutation subtype aggregation
    mutation_subtype_agg = A("terms", field="mutation_subtype", size=size)
    s.aggs.bucket("mutation_subtype", mutation_subtype_agg)
    # Combine both queries
    s = s.query("bool", must=query_list)

    # Execute the query
    response = await asyncio.to_thread(s.execute)

    # Return the full response as a dictionary
    return response.to_dict()


async def ssm_facet_query(case_filter: GQLFilter, filters):
    case_ids = await query_case_ids(case_filter)
    filter_contents = get_gql_filter_contents(filters)

    es_filters = [
        convert_gql_to_elastic_search(gf, index=settings.ES_SSM_CENTRIC_INDEX, boost=0)
        for gf in filter_contents
    ]

    # Combine nested queries to find a single gene that satisfies all filters.
    combined_filters = combine_nested_queries_simple(es_filters)

    if len(case_ids) == 0:
        return {"data": [], "total": 0}

    if len(combined_filters) == 0:
        combined_filters = None

    response = build_ssm_consequence_aggregation(
        case_ids=case_ids,
        filters=combined_filters,
    )

    graphql_response = transform_to_graphql_response(
        response,
        aggregation_fields,
    )

    return graphql_response
