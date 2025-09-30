import json
from dataclasses import asdict

from gen3analysis.filters.convertFiltersToGen3GQLFilters import (
    convert_operation_to_gql,
)
from gen3analysis.filters.convertGDCGQLFiltersToFilters import (
    convert_gql_operation_to_operation,
)
from elasticsearch import Elasticsearch
from es.query_builder import ESQueryBuilder
from elasticsearch_dsl import Q
from gen3analysis.filters.es.es_fieldpath_query import (
    make_wrapped_clause,
    make_top_level_query,
)

from gen3analysis.filters.es.convertGen3GQLToElasticSearch import (
    convert_gql_to_elastic_search,
)

GDCFilters = [
    {
        "op": "and",
        "content": [
            {
                "op": "or",
                "content": [
                    {
                        "op": ">",
                        "content": {
                            "field": "demographic.days_to_death",
                            "value": 0,
                        },
                    },
                    {
                        "op": ">",
                        "content": {
                            "field": "diagnoses.days_to_last_follow_up",
                            "value": 0,
                        },
                    },
                ],
            }
        ],
    },
    {"op": "not", "content": {"field": "demographic.vital_status"}},
]

if __name__ == "__main__":
    user_dict = convert_gql_operation_to_operation(GDCFilters[0])
    user_json = json.dumps(asdict(user_dict), indent=2)
    print("GDC Filter:")
    print(user_json)

    guppy_filters = convert_operation_to_gql(user_dict)
    # Use the dataclasses_json to_json method and then parse it back
    guppy_dict = guppy_filters.to_dict()
    # guppy_dict = json.loads(guppy_json_str)

    guppy_json = json.dumps(guppy_dict, indent=2)
    print("\nGen3 GQL Filter:")
    print(guppy_json)

    # test elastic search filter
    qb = ESQueryBuilder().size(0)
    project_filter = ESQueryBuilder.nested_term(
        path="occurrence.case.project",
        field="occurrence.case.project.project_id",
        value="MMRF-COMMPASS",
    )

    query = qb.nested_of(
        "occurrence.case.project", Q("term", **{"project_id": "MMRF-COMMPASS"})
    )

    print("\nElastic Search Filter:", query)
    es_json = json.dumps(query.to_dict(), indent=2)
    print("\n    json:", es_json)

    es_from_guppy_filters = convert_gql_to_elastic_search(guppy_filters)
    guppy_json = json.dumps(es_from_guppy_filters.to_dict(), indent=2)
    print("convert_gql_to_elastic_search:")
    print(guppy_json)

    es = Elasticsearch(["http://localhost:9200"])
    index = "mmrf_ssm_centric"

    clause = make_wrapped_clause(
        es, index, "consequence.transcript.gene.gene_id", "term", "ENSG00000153944"
    )
    print(clause)
    body = make_top_level_query(clause)

    print(body)

    g = (
        Q(
            "nested",
            path="case",
            query=Q("terms", field="occurrence.case.case_id", case_id=["a", "b"]),
        ),
    )
    print(g)
