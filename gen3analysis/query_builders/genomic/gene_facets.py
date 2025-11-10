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


def build_gene_aggregation(
    case_ids: List[str],
    consequence_filters: Optional[Dict[str, List[str]]] = None,
    gene_filters: Optional[Dict[str, List[str]]] = None,
    filters2: Optional[List[dict]] = [],
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
        consequence_filters: Dictionary of consequence field filters, e.g.:
            {
                "case.ssm.consequence.transcript.annotation.vep_impact": ["modifier"]
            }
        gene_filters: Dictionary of gene-level field filters, e.g.:
            {
                "is_cancer_gene_census": ["true"],
                "biotype": ["protein_coding"]
            }
        size: Number of buckets to return in each aggregation (default: 200)

    Returns:
        Dictionary containing the Elasticsearch response with aggregations

    """
    aggregation_fields = ["biotype", "is_cancer_gene_census"]

    # Default consequence filters (modifier impact)
    if consequence_filters is None:
        consequence_filters = {
            "case.ssm.consequence.transcript.annotation.vep_impact": ["modifier"]
        }

    # Default gene filters (cancer genes, protein coding)
    if gene_filters is None:
        gene_filters = {
            "is_cancer_gene_census": ["true"],
            "biotype": ["protein_coding"],
        }

    # Initialize the search object
    s = Search(using=get_es(), index=settings.ES_GENE_CENTRIC_INDEX)

    # Set size to 0 since we only want aggregations, disable source
    s = s.extra(size=0, _source=False)

    # Build the nested query for case filtering
    case_query_clauses = [Q("terms", **{"case.case_id": case_ids})]

    # Add nested consequence filters within the case.ssm.consequence path
    if consequence_filters:
        consequence_must_clauses = []
        for field, values in consequence_filters.items():
            consequence_must_clauses.append(Q("terms", **{field: values}))

        consequence_query = Q(
            "nested",
            path="case.ssm.consequence",
            ignore_unmapped=True,
            query=Q("bool", must=consequence_must_clauses),
        )

        # Wrap consequence query in case.ssm nested query
        ssm_query = Q(
            "nested",
            path="case.ssm",
            ignore_unmapped=True,
            query=Q("bool", must=[consequence_query]),
        )

        case_query_clauses.append(ssm_query)

    # Build the complete nested case query
    case_query = Q(
        "nested",
        path="case",
        ignore_unmapped=True,
        query=Q("bool", must=case_query_clauses),
    )

    # Build the main query combining case query and gene filters
    main_query_clauses = [case_query]

    # Add gene-level filters (non-nested)
    for field, values in gene_filters.items():
        main_query_clauses.append(Q("terms", **{field: values}))

    all_filters = [case_query]
    all_filters.extend(filters2)
    main_query_clauses2 = combine_nested_queries_simple(all_filters)
    gene_filter_q, case_and_ssm_filters_q = extract_gene_filters(all_filters)
    gene_filter_values = extract_gene_filter_values(gene_filter_q)

    # Apply the main query
    s = s.query("bool", must=main_query_clauses)

    # Build global aggregations for each field
    for agg_field in aggregation_fields:
        # Global aggregation
        global_agg = A("global")
        s.aggs.bucket(f"{agg_field}:global", global_agg)

        # Build filter for this aggregation (case + ssm + consequence + other gene filters)
        filter_clauses = [case_query]

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

    # write the results to a json file
    with open("./logs/build_gene_facets.json", "w") as f:
        json.dump(s.to_dict(), f, indent=4)

    # Execute the query
    response = s.execute()

    # Return the full response as a dictionary
    return response.to_dict()


def gene_facet_query(case_ids: List[str], filters):

    filter_contents = get_gql_filter_contents(filters)

    es_filters = [
        convert_gql_to_elastic_search(gf, index=settings.ES_GENE_CENTRIC_INDEX, boost=0)
        for gf in filter_contents
    ]

    # Combine nested queries to find a single gene that satisfies all filters.
    combined_filters = combine_nested_queries_simple(es_filters)

    results = build_gene_aggregation(
        case_ids=case_ids,
        filters2=combined_filters,
    )

    return results
