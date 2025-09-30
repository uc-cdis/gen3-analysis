from typing import Any, Dict, Iterable, List, Optional, Union
from elasticsearch_dsl import Q, A, Search


class ESQueryBuilder:
    """
    Thin wrapper around elasticsearch-dsl that:
      - makes nested queries/filters easy
      - provides composite aggregation helpers (for 7.x pagination)
      - returns both DSL objects and plain dicts
    """

    def __init__(self, index: Optional[Union[str, List[str]]] = None):
        self.s = Search(index=index)

    # -------- Core WHERE / FILTER --------

    def must(self, *queries: Q) -> "ESQueryBuilder":
        for q in queries:
            self.s = self.s.query("bool", must=[q])
        return self

    def filter(self, *filters: Q) -> "ESQueryBuilder":
        for f in filters:
            self.s = self.s.query("bool", filter=[f])
        return self

    def should(
        self, *queries: Q, minimum_should_match: Optional[int] = None
    ) -> "ESQueryBuilder":
        kwargs = {"should": list(queries)}
        if minimum_should_match is not None:
            kwargs["minimum_should_match"] = minimum_should_match
        self.s = self.s.query("bool", **kwargs)
        return self

    # -------- Nested sugar --------

    @staticmethod
    def nested_term(path: str, field: str, value: Any) -> Q:
        return Q("nested", path=path, query=Q("term", **{field: value}))

    @staticmethod
    def nested_terms(path: str, field: str, values: Iterable[Any]) -> Q:
        return Q("nested", path=path, query=Q("terms", **{field: list(values)}))

    @staticmethod
    def nested_exists(path: str, field: str) -> Q:
        return Q("nested", path=path, query=Q("exists", field=field))

    @staticmethod
    def nested_range(path: str, field: str, **range_kwargs) -> Q:
        return Q("nested", path=path, query=Q("range", **{field: range_kwargs}))

    # Deeply-nested (nested within nested) by stacking:
    @staticmethod
    def nested_of(path: str, inner: Q) -> Q:
        """Wrap any Q inside a nested container at `path`."""
        return Q("nested", path=path, query=inner)

    # -------- Aggregations --------

    def agg_terms(
        self,
        name: str,
        field: str,
        size: int = 10,
        order: Optional[Dict[str, str]] = None,
        include: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
    ) -> "ESQueryBuilder":
        a = A("terms", field=field, size=size)
        if order:
            a = A("terms", field=field, size=size, order=order)
        if include is not None:
            a.params["include"] = include
        if exclude is not None:
            a.params["exclude"] = exclude
        self.s.aggs.bucket(name, a)
        return self

    def agg_nested(self, name: str, path: str) -> "ESQueryBuilder":
        self.s.aggs.bucket(name, A("nested", path=path))
        return self

    def subagg_terms(
        self,
        parent: str,
        name: str,
        field: str,
        size: int = 10,
        order: Optional[Dict[str, str]] = None,
    ) -> "ESQueryBuilder":
        a = A("terms", field=field, size=size)
        if order:
            a = A("terms", field=field, size=size, order=order)
        self.s.aggs[parent].bucket(name, a)
        return self

    def subagg_cardinality(
        self, parent: str, name: str, field: str, precision_threshold: int = 40000
    ) -> "ESQueryBuilder":
        self.s.aggs[parent].metric(
            name, A("cardinality", field=field, precision_threshold=precision_threshold)
        )
        return self

    def agg_composite(
        self,
        name: str,
        sources: List[Dict[str, Dict[str, Dict[str, str]]]],
        size: int = 200,
        after_key: Optional[Dict[str, Any]] = None,
    ) -> "ESQueryBuilder":
        """
        Composite aggregation for 7.x pagination by 'after_key'.
        sources example:
          [
            {"ssm_id": {"terms": {"field": "ssm_id"}}},
            {"project": {"terms": {"field": "occurrence.case.project.project_id"}}},
          ]
        """
        a = A("composite", sources=sources, size=size)
        if after_key:
            a.params["after"] = after_key
        self.s.aggs.bucket(name, a)
        return self

    def agg_nested_path_then_terms(
        self,
        nested_name: str,
        nested_path: str,
        terms_name: str,
        field: str,
        size: int = 10,
        order: Optional[Dict[str, str]] = None,
    ) -> "ESQueryBuilder":
        self.agg_nested(nested_name, nested_path)
        # add a terms sub-agg under the nested scope
        a = A("terms", field=field, size=size)
        if order:
            a = A("terms", field=field, size=size, order=order)
        self.s.aggs[nested_name].bucket(terms_name, a)
        return self

    # -------- Misc --------

    def size(self, n: int) -> "ESQueryBuilder":
        self.s = self.s.extra(size=n)
        return self

    def from_(self, n: int) -> "ESQueryBuilder":
        self.s = self.s.extra(from_=n)
        return self

    def sort(self, *fields: Union[str, Dict[str, Any]]) -> "ESQueryBuilder":
        self.s = self.s.sort(*fields)
        return self

    def to_dict(self) -> Dict[str, Any]:
        return self.s.to_dict()

    def dsl(self) -> Search:
        return self.s
