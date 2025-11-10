"""
This module provides functionality to build complex nested queries and aggregations
for genomic data
"""

import json

from elasticsearch_dsl import Search, Q, A
from typing import List, Dict, Any, Optional

from gen3analysis.filters.es.convertGen3GQLToElasticSearch import (
    convert_gql_to_elastic_search,
)
from gen3analysis.filters.gen3GQLFilters import get_gql_filter_contents
from gen3analysis.gen3.es_client import get_es
from gen3analysis.query_builders.utils.combine_nested import (
    combine_nested_queries_simple,
)
from gen3analysis.settings import settings


def build_ssm_consequence_aggregation(
    case_ids: List[str],
    filters: Optional[List[dict]],
    size: int = 64,
) -> Dict[str, Any]:
    # Default aggregation fields
    aggregation_fields = [
        "consequence.transcript.consequence_type",
        "consequence.transcript.annotation.sift_impact",
        "consequence.transcript.annotation.polyphen_impact",
        "consequence.transcript.annotation.vep_impact",
    ]

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

    # # Build the combined nested query for consequence filters
    # # All filters must match on the SAME consequence object
    # consequence_must_clauses = []
    # for field, values in filters.items():
    #     consequence_must_clauses.append(Q('terms', **{field: values}))
    #
    # consequence_query = Q(
    #     'nested',
    #     path='consequence',
    #     ignore_unmapped=True,
    #     query=Q('bool', must=consequence_must_clauses)
    # )

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

    # Nested aggregation for consequence
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

    # write the results to a json file
    with open("./logs/build_ssm_consequence_aggregation2.json", "w") as f:
        json.dump(s.to_dict(), f, indent=4)

    # Execute the query
    response = s.execute()

    # Return the full response as a dictionary
    return response.to_dict()


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

    Example:
        >>> results = build_ssm_consequence_aggregation(...)
        >>> consequence_types = extract_aggregation_results(
        ...     results,
        ...     "consequence.transcript.consequence_type"
        ... )
        >>> for bucket in consequence_types:
        ...     print(f"{bucket['key']}: {bucket['doc_count']} consequences, "
        ...           f"{bucket['parent_count']} SSMs")
    """
    agg_name = f"{aggregation_field}:filtered"

    try:
        agg_data = response["aggregations"]["consequence:global"][
            "consequence:filtered"
        ]["consequence"][agg_name][aggregation_field]

        buckets = agg_data.get("buckets", [])

        results = []
        for bucket in buckets:
            result = {
                "key": bucket["key"],
                "count": bucket["doc_count"],
            }
            results.append(result)

        return results

    except KeyError as e:
        raise ValueError(
            f"Could not find aggregation data for field {aggregation_field}: {e}"
        )


def ssm_facet_query(case_ids: List[str], filters):

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

    results = build_ssm_consequence_aggregation(
        case_ids=case_ids,
        filters=combined_filters,
    )

    return results
