from typing import Optional
from gen3analysis.filters.es.es_nested_path import build_wrapped_query_Q
from gen3analysis.filters.gen3GQLFilters import (
    GQLFilter,
    is_gql_intersection,
    is_gql_greater_than,
    is_gql_equal,
    is_gql_not_equal,
    is_gql_less_than,
    is_gql_less_than_or_equals,
    is_gql_greater_than_or_equals,
    is_gql_includes,
    is_gql_excludes,
    is_gql_exclude_if_any,
    is_gql_union,
    is_gql_nested_filter,
)
from elasticsearch_dsl import Q

from gen3analysis.gen3.es_client import get_nested_registry


def convert_gql_to_elastic_search(
    fltr: GQLFilter,
    index=Optional[str],
    start_path_index: int = 0,
    boost: Optional[float] = 1.0,
) -> Q:

    nesting = None
    if index is not None:
        nesting = get_nested_registry().get(index)
    q = None
    field = None
    if is_gql_intersection(fltr):
        return Q(
            "bool",
            must=[
                convert_gql_to_elastic_search(x, index, start_path_index, boost)
                for x in fltr.and_op
            ],
        )

    if is_gql_union(fltr):
        return Q(
            "bool",
            should=[
                convert_gql_to_elastic_search(x, index, start_path_index, boost)
                for x in fltr.or_op
            ],
        )

    if is_gql_equal(fltr):
        field, value = next(iter(fltr.equal_op.items()))
        q = Q("term", **{field: value}, boost=boost)

    if is_gql_not_equal(fltr):
        field, value = next(iter(fltr.not_equal_op.items()))
        q = Q("bool", must_not=Q("term", **{field: value}, boost=boost))

    if is_gql_greater_than(fltr):
        field, value = next(iter(fltr.greater_than_op.items()))
        q = Q("range", **{field: {"gt": value}})

    if is_gql_greater_than_or_equals(fltr):
        field, value = next(iter(fltr.greater_than_or_equals_op.items()))
        q = Q("range", **{field: {"gte": value}})

    if is_gql_less_than(fltr):
        field, value = next(iter(fltr.less_than_op.items()))
        q = Q("range", **{field: {"lt": value}})

    if is_gql_less_than_or_equals(fltr):
        field, value = next(iter(fltr.less_than_or_equals_op.items()))
        q = Q("range", **{field: {"lte": value}})

    if is_gql_includes(fltr):
        field, values = next(iter(fltr.in_op.items()))
        q = Q("terms", **{field: values})

    if is_gql_excludes(fltr):
        field, values = next(iter(fltr.exclude_op.items()))
        q = Q("bool", must_not=Q("terms", **{field: values}, boost=boost))

    if is_gql_exclude_if_any(fltr):
        field, values = next(iter(fltr.exclude_if_any_op.items()))
        q = Q("bool", must_not=Q("terms", **{field: values}, boost=boost))

    if is_gql_nested_filter(fltr):
        path = fltr.nested_op.path
        nested_query = convert_gql_to_elastic_search(fltr.nested_op.filter_content)
        return Q("nested", path=path, query=nested_query)

    if nesting is not None:
        ne = nesting.get(field)
        if ne is None:
            return q
        paths = ne.nested_paths
        if paths is not None and (len(paths) - start_path_index) >= 1:
            return build_wrapped_query_Q(q, paths[start_path_index:])

    return q
