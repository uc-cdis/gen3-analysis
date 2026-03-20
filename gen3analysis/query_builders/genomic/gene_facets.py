import asyncio
from typing import List, Dict, Any, Optional

from elasticsearch_dsl import Search, Q, A

from gen3analysis.filters.es.convert_gql_to_elastic_search import (
    convert_gql_to_elastic_search,
)
from gen3analysis.filters.gen3GQLFilters import get_gql_filter_contents, GQLFilter
from gen3analysis.gen3.es_client import get_es, get_es_executor
from gen3analysis.query_builders.genomic.queries import query_case_ids
from gen3analysis.query_builders.utils.combine_nested import (
    combine_nested_queries_simple,
)
from gen3analysis.settings import settings


def extract_gene_filter_values(filters: List) -> Dict[str, List[str]]:
    """
    Extract gene-level filters (is_cancer_gene_census, biotype) from elasticsearch_dsl queries.

    Args:
        filters: Flat list of elasticsearch_dsl Query objects

    Returns:
        Dictionary with extracted filter values, e.g.:
        {
            "is_cancer_gene_census": ["true"],
            "biotype": ["protein_coding"]
        }
    """
    from elasticsearch_dsl.query import Query

    extracted = {}
    target_fields = ["is_cancer_gene_census", "biotype"]

    for query in filters:
        if not isinstance(query, Query):
            continue

        # Check if it's a terms query
        if (
            hasattr(query, "name")
            and query.name == "terms"
            and hasattr(query, "_params")
        ):
            for key, values in query._params.items():
                # Check if the key ends with our target field names
                for field in target_fields:
                    if key.endswith(field):
                        if field not in extracted:
                            extracted[field] = []
                        # Add values (ensure it's a list)
                        if isinstance(values, list):
                            extracted[field].extend(values)
                        else:
                            extracted[field].append(values)

    # Remove duplicates while preserving order
    for field in extracted:
        extracted[field] = list(dict.fromkeys(extracted[field]))

    return extracted


def extract_gene_filters(filters: List) -> tuple[List, List]:
    """
    Split filters into those that have is_cancer_gene_census or biotype fields and those that don't.

    Args:
        filters: Flat list of elasticsearch_dsl Query objects

    Returns:
        Tuple of (matching_filters, non_matching_filters)
        - matching_filters: List of Query objects that reference is_cancer_gene_census or biotype
        - non_matching_filters: List of Query objects that don't reference those fields
    """
    from elasticsearch_dsl.query import Query

    target_fields = ["is_cancer_gene_census", "biotype"]
    matching_filters = []
    non_matching_filters = []

    for query in filters:
        if not isinstance(query, Query):
            non_matching_filters.append(query)
            continue

        # Check if it's a terms query with _params
        is_match = False
        if (
            hasattr(query, "name")
            and query.name == "terms"
            and hasattr(query, "_params")
        ):
            # Check if any parameter key ends with our target fields
            for key in query._params.keys():
                if any(key.endswith(field) for field in target_fields):
                    is_match = True
                    break

        if is_match:
            matching_filters.append(query)
        else:
            non_matching_filters.append(query)

    return matching_filters, non_matching_filters


def extract_gene_aggregation_results(
    response: Dict[str, Any], aggregation_field: str
) -> List[Dict[str, Any]]:
    """
    Extract and format results from a specific gene aggregation field.

    Args:
        response: The Elasticsearch response dictionary
        aggregation_field: The field to extract results for

    Returns:
        List of dictionaries containing bucket information
    """
    try:
        agg_data = response["aggregations"][f"{aggregation_field}:global"][
            f"{aggregation_field}:filtered"
        ][aggregation_field]

        buckets = agg_data.get("buckets", [])

        results = []
        for bucket in buckets:
            result = {"key": bucket["key"], "doc_count": bucket["doc_count"]}
            results.append(result)

        return results

    except KeyError as e:
        raise ValueError(
            f"Could not find aggregation data for field {aggregation_field}: {e}"
        )


def transform_to_graphql_response(
    response: Dict[str, Any],
    aggregation_fields: List[str],
    max_histogram_items: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Transform Elasticsearch aggregation response to GDC GraphQL-style format.

    Args:
        response: The Elasticsearch response dictionary
        aggregation_fields: List of fields that were aggregated
        max_histogram_items: Optional limit on number of histogram items per field

    Returns:
        Dictionary in GraphQL format with nested structure
    """
    # Get total count from the first filtered aggregation
    total_count = 0
    if aggregation_fields and response.get("aggregations"):
        first_field = aggregation_fields[0]
        try:
            total_count = response["aggregations"][f"{first_field}:global"][
                f"{first_field}:filtered"
            ]["doc_count"]
        except KeyError:
            # Fallback to hits total if aggregation structure is different
            total_count = response.get("hits", {}).get("total", {}).get("value", 0)

    # Build gene_centric object
    gene_centric = {"_totalCount": total_count}

    # Process each aggregation field
    for field in aggregation_fields:
        try:
            buckets = extract_gene_aggregation_results(response, field)

            # Limit histogram items if specified
            if max_histogram_items is not None:
                buckets = buckets[:max_histogram_items]

            # Transform buckets to histogram format
            histogram = []
            for bucket in buckets:
                # Convert key to string, handling boolean values
                key = bucket["key"]
                if isinstance(key, bool):
                    key_str = "1" if key else "0"
                elif isinstance(key, int):
                    key_str = str(key)
                else:
                    key_str = str(key)

                histogram.append({"key": key_str, "count": bucket["doc_count"]})

            gene_centric[field] = {"histogram": histogram}

        except (KeyError, ValueError):
            # If aggregation field not found, include empty histogram
            gene_centric[field] = {"histogram": []}

    # Build final GraphQL response structure
    graphql_response = {"data": gene_centric}

    return graphql_response


async def build_gene_aggregation(
    case_ids: List[str],
    filters: Optional[List[dict]] = [],
    size: int = 200,
) -> Dict[str, Any]:
    """
    Build and execute a GDC-style aggregation query for gene data.

    This function creates a query that:
    1. Filters documents by case IDs (nested in case path)
    2. Filters by consequence criteria (nested in case.ssm.consequence path)
    3. Filters by gene-level criteria (non-nested fields like biotype, is_cancer_gene_census)
    4. Aggregates on specified gene fields

    Args:

        case_ids: List of case IDs to filter by
       filters: Dictionary of field filters, e.g.:
            {
                "case.ssm.consequence.transcript.annotation.vep_impact": ["modifier"]
            }
        size: Number of buckets to return in each aggregation (default: 200)

    Returns:
        Dictionary containing the Elasticsearch response with aggregations

    """
    aggregation_fields = ["biotype", "is_cancer_gene_census"]

    # Initialize the search object
    s = Search(using=get_es(), index=settings.ES_GENE_CENTRIC_INDEX)

    # Set size to 0 since we only want aggregations, disable source
    s = s.extra(size=0, _source=False)

    # Build the nested query for case filtering
    case_query_clauses = [Q("terms", **{"case.case_id": case_ids})]

    # Build the complete nested case query
    case_query = Q(
        "nested",
        path="case",
        ignore_unmapped=True,
        query=Q("bool", must=case_query_clauses),
    )

    # Build the main query combining case query and gene filters
    all_filters = [case_query]
    all_filters.extend(filters)
    main_query_clauses = combine_nested_queries_simple(all_filters)
    gene_filter_q, case_and_ssm_filters_q = extract_gene_filters(main_query_clauses)
    gene_filters = extract_gene_filter_values(gene_filter_q)

    # Apply the main query
    s = s.query("bool", must=main_query_clauses)

    # Build global aggregations for each field
    for agg_field in aggregation_fields:
        # Global aggregation
        global_agg = A("global")
        s.aggs.bucket(f"{agg_field}:global", global_agg)

        # Build filter for this aggregation (case + ssm + consequence + other gene filters)
        filter_clauses = case_and_ssm_filters_q

        # Add all gene filters except the current one being aggregated
        for field, values in gene_filters.items():
            if field != agg_field:
                filter_clauses.append(Q("terms", **{field: values}))

        # Filter aggregation within global
        filtered_agg = A("filter", Q("bool", must=filter_clauses))
        s.aggs[f"{agg_field}:global"].bucket(f"{agg_field}:filtered", filtered_agg)

        # Terms aggregation on the field
        terms_agg = A("terms", field=agg_field, size=size)
        s.aggs[f"{agg_field}:global"][f"{agg_field}:filtered"].bucket(
            agg_field, terms_agg
        )

    # Execute the query in dedicated ES thread pool
    loop = asyncio.get_event_loop()
    executor = get_es_executor()
    response = await loop.run_in_executor(executor, s.execute)

    # Return the full response as a dictionary
    return response.to_dict()


async def gene_facet_query(case_filter: GQLFilter, filters):

    case_ids = await query_case_ids(case_filter)

    filter_contents = get_gql_filter_contents(filters)

    es_filters = [
        convert_gql_to_elastic_search(gf, index=settings.ES_GENE_CENTRIC_INDEX, boost=0)
        for gf in filter_contents
    ]

    # Combine nested queries to find a single gene that satisfies all filters.
    combined_filters = combine_nested_queries_simple(es_filters)

    response = await build_gene_aggregation(
        case_ids=case_ids,
        filters=combined_filters,
    )

    aggregation_fields = ["biotype", "is_cancer_gene_census"]

    graphql_response = transform_to_graphql_response(
        response,
        aggregation_fields,
        max_histogram_items=10,  # Limit to top 10 for each field
    )

    return graphql_response
