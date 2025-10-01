from typing import Optional, List, Dict, Any, Iterable
from elasticsearch_dsl import Q, A, Search

from gen3analysis.filters.es.convertGen3GQLToElasticSearch import (
    convert_gql_to_elastic_search,
)
from gen3analysis.filters.es.es_nested_path import build_wrapped_query_Q
from gen3analysis.filters.es.query_builder import ESQueryBuilder
from gen3analysis.filters.gen3GQLFilters import GQLFilter, get_gql_filter_contents
from gen3analysis.gen3.es_client import get_es, get_nested_registry
from gen3analysis.routes.survival import MAX_CASES
import json
from glom import glom


def build_gen3_es_query(
    case_filter: GQLFilter,
    size: Optional[int] = 20,
) -> Dict[str, Any]:
    """
    Builds the GraphQL query for retrieving case IDs.

    This function constructs a GraphQL query with the specified filters, optional
    fields to include, and the maximum size of results. The query is intended to
    retrieve case IDs based on the provided parameters.

    Parameters:
    filters (GQLFilter): The filter conditions for the GraphQL query.
    include (Optional[Union[str, List[str]]]): The fields or list of fields to
        include in the query output. Defaults to None.
    size (Optional[int]): The maximum number of results to retrieve. Defaults
        to 20.

    Returns:
    Dict[str, Any]: A dictionary representation of the constructed GraphQL query.
    """
    qb = ESQueryBuilder().size(size)
    # filter cases
    if case_filter:
        qb.filter(case_filter)
    else:
        qb.filter(Q("match_all"))

    return qb.to_dict()


def build_top_genes_by_case_count(
    gene_path: str = "consequence.transcript",  # nested path
    gene_field: str = "consequence.transcript.gene_symbol.keyword",
    case_nested_path: str = "occurrence.case",
    case_id_field: str = "occurrence.case.case_id",
    case_filter: Optional[Q] = None,
    size: int = 20,
) -> Dict[str, Any]:
    """
    Returns an aggregation that:
      - scopes into gene_path (nested)
      - buckets by gene symbol
      - for each gene, counts unique case_ids from a *different* nested path
    NOTE: In ES 7.x, mixing nested scopes requires care. We compute the case
    cardinality by a *separate* nested agg chained under the gene bucket.
    """

    qb = ESQueryBuilder().size(0)

    # Optional global filter on cases or other fields
    if case_filter:
        qb.filter(case_filter)

    # Nested into genes -> terms by gene -> nested into cases -> cardinality of case_id
    qb.agg_nested("genes", gene_path).subagg_terms(
        "genes", "by_gene", field=gene_field, size=size, order={"_count": "desc"}
    )

    # Under each gene bucket, scope into cases and compute unique case count
    qb.s.aggs["genes"]["by_gene"].bucket("cases", A("nested", path=case_nested_path))
    qb.s.aggs["genes"]["by_gene"]["cases"].metric(
        "unique_cases", A("cardinality", field=case_id_field, precision_threshold=40000)
    )

    return qb.to_dict()


def build_cases_by_gene_terms_query(
    gene_path: str,
    gene_id_field: str,
    gene_ids: Iterable[str],
    return_size: int = 0,
) -> Dict[str, Any]:
    """
    Example: filter to a list of gene_ids in a nested path (terms) and return hits or aggs.
    """
    qb = ESQueryBuilder().size(return_size)
    qb.filter(ESQueryBuilder.nested_terms(gene_path, gene_id_field, gene_ids))
    return qb.to_dict()


def build_composite_by_ssm(
    size: int = 200,
    after_key: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Composite over ssm_id with a nested cardinality of unique cases.
    Use after_key from the previous response to get the next page.
    """
    qb = ESQueryBuilder().size(0)

    qb.agg_composite(
        "ssm_idAggs",
        sources=[{"ssm_id": {"terms": {"field": "ssm_id"}}}],
        size=size,
        after_key=after_key,
    )

    # For each bucket: nested into cases -> unique case count
    qb.s.aggs["ssm_idAggs"].bucket("occ", A("nested", path="occurrence.case"))
    qb.s.aggs["ssm_idAggs"]["occ"].metric(
        "unique_cases",
        A("cardinality", field="occurrence.case.case_id", precision_threshold=40000),
    )

    return qb.to_dict()


def build_total_case_count_for_gene_filters(gene_id: str):

    ssm_query = convert_gql_to_elastic_search
    q = Q(
        "bool",
        must=[
            Q("ids", values=[gene_id], boost=0),
            Q(
                "nested",
                path="case",
                ignore_unmapped=True,
                query=Q(
                    "bool",
                    must=[
                        Q("terms", case__available_variation_data=["ssm"]),
                        Q(
                            "nested",
                            path="case.ssm",
                            ignore_unmapped=True,
                            query=Q(
                                "bool",
                                must=[
                                    Q(
                                        "nested",
                                        path="case.ssm.observation",
                                        ignore_unmapped=True,
                                        query=Q(
                                            "bool",
                                            must=[
                                                Q(
                                                    "exists",
                                                    field="case.ssm.observation.observation_id",
                                                    boost=0,
                                                )
                                            ],
                                        ),
                                    )
                                ],
                            ),
                        ),
                    ],
                ),
                inner_hits={
                    "size": 0,
                    "from": 0,
                    "_source": {"includes": ["case.case_id"]},
                },
            ),
        ],
    )
    return q


def build_gene_query(
    gene_es_filters: List[GQLFilter],
    ssm_es_filters: List[GQLFilter],
    case_ids: List[str],
):
    # Build must-clauses conditionally
    must_clauses = [
        Q(
            "nested",
            path="case",
            ignore_unmapped=True,
            query=Q(
                "bool",
                must=[Q("terms", case__case_id=case_ids, boost=0), *ssm_es_filters],
            ),
        )
    ]
    if len(gene_es_filters) > 0:
        must_clauses.extend(gene_es_filters)

    top_gene_query = Q(
        "bool",
        must=must_clauses,
        should=[
            Q(
                "bool",
                must=[
                    Q(
                        "nested",
                        path="case",
                        score_mode="sum",
                        query=Q(
                            "constant_score",
                            boost=1.0,
                            filter=Q(
                                "bool",
                                must=[
                                    Q(
                                        "bool",
                                        must=[
                                            Q(
                                                "bool",
                                                must=[
                                                    Q(
                                                        "terms",
                                                        case__case_id=case_ids,
                                                        boost=0,
                                                    ),
                                                    *ssm_es_filters,
                                                ],
                                            )
                                        ],
                                    ),
                                    Q(
                                        "bool",
                                        must=[
                                            Q(
                                                "nested",
                                                path="case.ssm",
                                                query=Q(
                                                    "exists", field="case.ssm.ssm_id"
                                                ),
                                            )
                                        ],
                                    ),
                                    Q("exists", field="case.project.project_id"),
                                ],
                                must_not=Q("term", case__project__project_id=""),
                            ),
                        ),
                    ),
                ],
            ),
            Q("bool", boost=0, must=Q("match_all")),
        ],
    )

    return top_gene_query


def query_case_ids(case_filter: GQLFilter) -> List[str]:
    s = Search(using=get_es(), index="case_centric")
    if case_filter:
        filters = convert_gql_to_elastic_search(case_filter)
    else:
        filters = Q("match_all")
    s = s[0:MAX_CASES]  # Get all cases
    s = s.source(False)
    s = s.query(filters)

    results = s.execute()

    case_ids = [x._id for x in results["hits"]["hits"]]
    return case_ids


def query_top_genes(
    case_filter: GQLFilter,
    gene_filter: GQLFilter,
    ssm_filter: GQLFilter,
    size: int = 20,
    offset: int = 0,
) -> Dict[str, Any]:
    s = Search(using=get_es(), index="case_centric")
    if case_filter:
        filters = convert_gql_to_elastic_search(case_filter)
    else:
        filters = Q("match_all")
    s = s[0:MAX_CASES]  # Get all cases
    s = s.source(False)
    s = s.query(filters)
    results = s.execute()

    case_ids = [x._id for x in results["hits"]["hits"]]

    if len(case_ids) == 0:
        return {"data": [], "total": 0}

    gene_filter_contents = get_gql_filter_contents(gene_filter)
    gene_es_filters = []
    for x in gene_filter_contents:
        gene_query = convert_gql_to_elastic_search(x, index="gene_centric", boost=0)
        gene_es_filters.append(gene_query)

    ssm_filter_contents = get_gql_filter_contents(ssm_filter)
    ssm_es_filters = []
    for x in ssm_filter_contents:
        ssm_query = convert_gql_to_elastic_search(
            x, index="gene_centric", start_path_index=1, boost=0
        )
        ssm_es_filters.append(ssm_query)

    # Build must-clauses conditionally
    must_clauses = [
        Q(
            "nested",
            path="case",
            ignore_unmapped=True,
            query=Q(
                "bool",
                must=[Q("terms", case__case_id=case_ids, boost=0), *ssm_es_filters],
            ),
        )
    ]
    if len(gene_es_filters) > 0:
        must_clauses.extend(gene_es_filters)

    top_gene_query = Q(
        "bool",
        must=must_clauses,
        should=[
            Q(
                "bool",
                must=[
                    Q(
                        "nested",
                        path="case",
                        score_mode="sum",
                        query=Q(
                            "constant_score",
                            boost=1.0,
                            filter=Q(
                                "bool",
                                must=[
                                    Q(
                                        "bool",
                                        must=[
                                            Q(
                                                "bool",
                                                must=[
                                                    Q(
                                                        "terms",
                                                        case__case_id=case_ids,
                                                        boost=0,
                                                    ),
                                                    *ssm_es_filters,
                                                ],
                                            )
                                        ],
                                    ),
                                    Q(
                                        "bool",
                                        must=[
                                            Q(
                                                "nested",
                                                path="case.ssm",
                                                query=Q(
                                                    "exists", field="case.ssm.ssm_id"
                                                ),
                                            )
                                        ],
                                    ),
                                    Q("exists", field="case.project.project_id"),
                                ],
                                must_not=Q("term", case__project__project_id=""),
                            ),
                        ),
                    ),
                ],
            ),
            Q("bool", boost=0, must=Q("match_all")),
        ],
    )

    # given case ids, get 20 top genes

    gene_s = Search(using=get_es(), index="gene_centric")
    gene_s = gene_s.source(
        ["symbol", "name", "biotype", "gene_id", "is_cancer_gene_census"]
    )
    gene_s = gene_s.query(top_gene_query)
    gene_s = gene_s[offset:size]
    gene_s = gene_s.extra(track_scores=True)
    results = gene_s.execute()
    return {
        "data": results["hits"]["hits"]._l_,
        "total": results["hits"]["total"]["value"],
    }


def gene_table_query(
    case_filter: GQLFilter,
    gene_filter: GQLFilter,
    ssm_filter: GQLFilter,
    size: int = 20,
    offset: int = 0,
) -> Dict[str, Any]:
    # first get the case using the cohort filter

    nested_registry = get_nested_registry()
    gene_nested_registry = nested_registry.get("gene_centric")

    case_ids = query_case_ids(case_filter)

    gene_filter_contents = get_gql_filter_contents(gene_filter)
    gene_es_filters = []
    for x in gene_filter_contents:
        gene_query = convert_gql_to_elastic_search(x, index="gene_centric", boost=0)
        gene_es_filters.append(gene_query)

    ssm_filter_contents = get_gql_filter_contents(ssm_filter)
    ssm_es_filters = []
    for x in ssm_filter_contents:
        ssm_query = convert_gql_to_elastic_search(
            x, index="gene_centric", start_path_index=1, boost=0
        )
        ssm_es_filters.append(ssm_query)

    # then get the top genes

    gene_cases_s = Search(using=get_es(), index="gene_centric")
    gene_cases_s = gene_cases_s.source(
        [
            "score",
            "symbol",
            "name",
            "cytoband",
            "biotype",
            "gene_id",
            "is_cancer_gene_census",
        ]
    )

    genes_by_cases_query = build_gene_query(gene_es_filters, ssm_es_filters, case_ids)
    gene_cases_s = gene_cases_s.query(genes_by_cases_query)
    gene_cases_s = gene_cases_s[offset:size]
    gene_cases_s = gene_cases_s.extra(track_scores=True)

    results = gene_cases_s.execute()

    # now we have the list of genes, create multiple queries for each gene for cnv and ssm counts
    gene_ids = [x._id for x in results["hits"]["hits"]]

    gene_information = {}
    for id in gene_ids:
        gene_information[id] = {"gene_id": id}

    for gene_id in gene_ids:
        gene_all_cases_s = Search(using=get_es(), index="gene_centric")
        gene_all_cases_s = gene_all_cases_s[:1]
        gene_all_cases_s = gene_all_cases_s.extra(track_scores=False)
        gene_all_cases_s = gene_all_cases_s.source(False)
        gene_count_all_cases_query = build_total_case_count_for_gene_filters(gene_id)
        gene_all_cases_s = gene_all_cases_s.query(gene_count_all_cases_query)
        print(json.dumps(gene_all_cases_s.to_dict()))
        results = gene_all_cases_s.execute()
        base = glom(results, "hits.hits", default={"_l_": [None]})[0]
        total = glom(base, "inner_hits.case.hits.total.value", default=0)
        gene_information[gene_id]["case_count"] = total

    return gene_information
