# Elasticsearch 7.x friendly helpers for auto-wrapping nested queries.
from typing import Any, Dict, Iterable, List, Optional, Tuple, Literal
from elasticsearch import Elasticsearch

Kind = Literal["term", "terms", "range", "exists", "neq"]


# -----------------------------
# Mapping / field-path analysis
# -----------------------------
def get_index_properties(es: Elasticsearch, index: str) -> Dict:
    """
    Return the top-level 'properties' dict for an index (or alias pattern).
    If multiple indices match, we just read the first one to learn structure.
    """
    m = es.indices.get_mapping(index=index)
    first_idx = next(iter(m))
    return m[first_idx]["mappings"].get("properties", {})


def _descend(node: Dict, segment: str) -> Tuple[Dict, str]:
    """
    Descend one step into the mapping tree by segment name.
    Handles both 'properties' (objects/nested) and 'fields' (multi-fields).
    Returns (child_node, route_taken: 'properties'|'fields').
    """
    if "properties" in node and segment in node["properties"]:
        return node["properties"][segment], "properties"
    if "fields" in node and segment in node["fields"]:
        return node["fields"][segment], "fields"
    raise KeyError(
        f"Path segment '{segment}' not found. "
        f"Node has keys={list(node.keys())} and may have properties={list(node.get('properties', {}).keys())} "
        f"or fields={list(node.get('fields', {}).keys())}."
    )


def analyze_field_path(
    es: Elasticsearch,
    index: str,
    field_path: str,
) -> Dict:
    """
    Walks the mapping to figure out which ancestors are 'nested'.
    Returns:
      {
        'nested_paths': ['gene', 'gene.ssm', ...],  # ordered shallow -> deep
        'leaf_type': 'keyword' | 'long' | ... | None,
        'exists': bool
      }
    """
    props = get_index_properties(es, index)
    parts = field_path.split(".")
    nested_paths: List[str] = []
    cur = {"properties": props}
    cur_path: List[str] = []

    for seg in parts:
        child, via = _descend(cur, seg)
        cur_path.append(seg)
        if child.get("type") == "nested":
            nested_paths.append(".".join(cur_path))
        cur = child

    return {
        "nested_paths": nested_paths,
        "leaf_type": cur.get("type"),
        "exists": cur.get("type") is not None,
    }


# -----------------------------
# Field caps (optional checks)
# -----------------------------
def get_field_caps(es: Elasticsearch, index: str, field_path: str) -> Dict:
    """
    Calls _field_caps to learn capabilities (searchable/aggregatable/type).
    Note: For multi-fields, pass the exact leaf (e.g., 'foo.keyword').
    """
    caps = es.field_caps(index=index, fields=field_path)
    return caps.get("fields", {}).get(field_path, {})


def assert_searchable_for(
    es: Elasticsearch,
    index: str,
    field_path: str,
    kind: Kind,
    allow_missing_caps: bool = True,
) -> None:
    """
    Raises if the field likely isn't usable for the requested kind.
    (Best-effort; skip if your cluster restricts _field_caps.)
    """
    caps = get_field_caps(es, index, field_path)
    if not caps:
        if allow_missing_caps:
            return
        raise ValueError(f"No field caps for '{field_path}' on index '{index}'")

    # caps might contain multiple types (if indices disagree). Be permissive:
    if kind in ("term", "terms", "neq", "exists", "range"):
        # For query usage, 'searchable' should be True in at least one mapping variant.
        if not any(v.get("searchable", False) for v in caps.values()):
            raise ValueError(
                f"Field '{field_path}' is not searchable for '{kind}' queries."
            )


# -----------------------------
# Clause builders (inner leafs)
# -----------------------------
def _leaf_clause(kind: Kind, field_path: str, value: Any = None, **kwargs) -> Dict:
    """
    Build the innermost clause for a single field:
      - term:   value=scalar
      - terms:  value=iterable
      - range:  supply operators via kwargs, e.g. gt=0, lte=100
      - exists: no value required
      - neq:    value=scalar  (implemented as bool must_not term)
    """
    if kind == "term":
        if value is None:
            raise ValueError("term requires a scalar value")
        return {"term": {field_path: value}}

    if kind == "terms":
        if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
            raise ValueError("terms requires an iterable of values")
        return {"terms": {field_path: list(value)}}

    if kind == "range":
        ops = {k: v for k, v in kwargs.items() if k in ("gt", "gte", "lt", "lte")}
        if not ops:
            raise ValueError("range requires one of gt/gte/lt/lte as kwargs")
        return {"range": {field_path: ops}}

    if kind == "exists":
        return {"exists": {"field": field_path}}

    if kind == "neq":
        if value is None:
            raise ValueError("neq requires a scalar value")
        # 'Not equal' as NOT term. Note: this includes docs where the field is missing.
        return {"bool": {"must_not": [{"term": {field_path: value}}]}}

    raise ValueError(f"Unsupported kind '{kind}'")


# -----------------------------
# Wrapping with nested scopes
# -----------------------------
def wrap_with_nested(inner: Dict, nested_paths: List[str]) -> Dict:
    """
    Wrap the inner clause with nested blocks from deepest -> shallowest.
    """
    wrapped = inner
    for p in reversed(nested_paths):
        wrapped = {"nested": {"path": p, "query": wrapped}}
    return wrapped


def make_wrapped_clause(
    es: Elasticsearch,
    index: str,
    field_path: str,
    kind: Kind,
    value: Any = None,
    *,
    require_exists_for_neq: bool = False,
    validate_caps: bool = False,
    **range_ops,
) -> Dict:
    """
    High-level convenience:
      - discovers nested ancestors from mapping
      - builds the leaf clause
      - wraps with nested scopes
    Returns a CLAUSE (not a top-level { "query": ... }).
    """
    info = analyze_field_path(es, index, field_path)
    if validate_caps:
        assert_searchable_for(es, index, field_path, kind)

    inner = _leaf_clause(kind, field_path, value, **range_ops)

    # Special handling: for 'neq' you often want to exclude docs where field is missing
    # (ES 'must_not term' matches docs with missing field). Use require_exists_for_neq=True to add exists.
    if kind == "neq" and require_exists_for_neq:
        inner = {
            "bool": {
                "must": [{"exists": {"field": field_path}}],
                "must_not": inner["bool"]["must_not"],
            }
        }

    return wrap_with_nested(inner, info["nested_paths"])


def make_top_level_query(clause: Dict) -> Dict:
    """
    Wrap a clause into {"query": ...} for direct search() calls.
    """
    return {"query": clause}


# -----------------------------
# (Optional) Simple aggs helper
# -----------------------------
def make_terms_agg_under_nested(
    es: Elasticsearch,
    index: str,
    field_path: str,
    agg_name: str = "by_field",
    size: int = 10,
    order: Optional[Dict[str, str]] = None,
) -> Dict:
    """
    Build { "size":0, "aggs": { ... } } with the correct nested scope for a terms agg.
    Useful when the field is itself inside nested ancestry.
    """
    info = analyze_field_path(es, index, field_path)
    body: Dict[str, Any] = {"size": 0, "aggs": {}}

    # Build nested buckets step-by-step
    cursor = body["aggs"]
    for i, p in enumerate(info["nested_paths"]):
        bucket_name = f"nested_{i}"
        cursor[bucket_name] = {"nested": {"path": p}, "aggs": {}}
        cursor = cursor[bucket_name]["aggs"]

    # Terms aggregation at leaf
    terms_agg: Dict[str, Any] = {"terms": {"field": field_path, "size": size}}
    if order:
        terms_agg["terms"]["order"] = order

    cursor[agg_name] = terms_agg
    return body
