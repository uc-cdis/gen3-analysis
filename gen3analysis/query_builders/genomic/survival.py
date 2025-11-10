import json
from dataclasses import field
from typing import Optional, List, Dict, Any, Iterable

from elasticsearch_dsl import Q, A, Search
from glom import glom, Path

from gen3analysis.filters.es.convertGen3GQLToElasticSearch import (
    convert_gql_to_elastic_search,
)
from gen3analysis.filters.es.query_builder import ESQueryBuilder
from gen3analysis.filters.gen3GQLFilters import (
    GQLFilter,
    get_gql_filter_contents,
    GQLIncludes,
)
from gen3analysis.gen3.es_client import get_es
from gen3analysis.query_builders.utils.combine_nested import (
    combine_nested_queries_simple,
)
from gen3analysis.query_builders.utils.extract_ids import extract_ids_from_hit
from gen3analysis.settings import settings


def build_gene_survival_query(
    genomic_filters, gene_id, exclude_gene, case_ids: List[str]
):
    genomic_es_filters = [
        convert_gql_to_elastic_search(gf, index=settings.ES_CASE_CENTRIC_INDEX, boost=0)
        for gf in genomic_filters
    ]

    if not exclude_gene:
        genomic_es_filters.append(
            Q(
                "nested",
                path="gene",
                ignore_unmapped=True,
                query=Q(
                    "bool",
                    must=[Q("term", gene__symbol={"value": gene_id, "boost": 0})],
                ),
            )
        )

    # Combine nested queries to find a single gene that satisfies all filters.
    combined_filters = combine_nested_queries_simple(genomic_es_filters)

    q = Q(
        "bool",
        must=[
            Q("exists", field="demographic.vital_status", boost=0),
            Q(
                "terms",
                case_id=case_ids,
                boost=0,
            ),
            *combined_filters,
            Q(
                "bool",
                should=[
                    Q(
                        "bool",
                        must=[
                            Q(
                                "range",
                                demographic__days_to_death={"gt": 0, "boost": 0},
                            ),
                        ],
                    ),
                    Q(
                        "bool",
                        must=[
                            Q(
                                "nested",
                                path="diagnoses",
                                ignore_unmapped=True,
                                query=Q(
                                    (
                                        Q(
                                            "bool",
                                            must=[
                                                Q(
                                                    "range",
                                                    diagnoses__days_to_last_follow_up={
                                                        "gt": 0,
                                                        "boost": 0,
                                                    },
                                                ),
                                            ],
                                        )
                                    )
                                ),
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )
    if exclude_gene:
        q.must.append(Q("terms", available_variation_data=["ssm", "cnv"], boost=0))
        q.must_not = [
            Q(
                "nested",
                path="gene",
                ignore_unmapped=True,
                query=Q(
                    "bool",
                    must=[Q("term", gene__symbol={"value": gene_id, "boost": 0})],
                ),
            )
        ]

    return q


def genomic_survival_comparison_query(
    case_ids: List[str], gene_id: str, genomic_filter: GQLFilter
):
    genomic_filter_contents = get_gql_filter_contents(genomic_filter)

    s = Search(using=get_es(), index=settings.ES_CASE_CENTRIC_INDEX)
    s = s.extra(track_total_hits=True)
    s = s.source(["_id"])
    s = s[0 : settings.MAX_CASES]
    excluded_query = s.query(
        build_gene_survival_query(genomic_filter_contents, gene_id, True, case_ids)
    )
    included_query = s.query(
        build_gene_survival_query(genomic_filter_contents, gene_id, False, case_ids)
    )

    included_results = included_query.execute()
    excluded_results = excluded_query.execute()
    included_case_ids = extract_ids_from_hit(included_results)
    excluded_case_ids = extract_ids_from_hit(excluded_results)
    return [included_case_ids, excluded_case_ids]
