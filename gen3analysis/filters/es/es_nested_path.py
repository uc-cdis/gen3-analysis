from typing import Dict, List, Tuple
from elasticsearch import Elasticsearch

try:
    from elasticsearch_dsl import Q  # optional
except Exception:
    Q = None


def get_index_properties(es: Elasticsearch, index: str) -> Dict:
    m = es.indices.get_mapping(index=index)
    # ES 7.x: m = { "index_name": {"mappings": {"properties": {...}}}}
    # If wildcard indices, just pick the first one for structure:
    idx = next(iter(m))
    return m[idx]["mappings"].get("properties", {})


def _descend(node: Dict, segment: str) -> Dict:
    # Prefer properties (objects/nested), else multi-fields (fields)
    props = node.get("properties", {})
    if segment in props:
        return props[segment]
    fields = node.get("fields", {})
    if segment in fields:
        return fields[segment]
    raise KeyError(
        f"Path segment '{segment}' not found at node. "
        f"Available properties={list(props.keys())}, fields={list(fields.keys())}"
    )


def _get_all_field_paths(root_props: Dict, prefix: str = "") -> List[str]:
    """
    Recursively extract all field paths from the mapping properties.
    Returns a list of dot-notation field paths.
    """
    paths: List[str] = []
    for key, value in root_props.items():
        current_path = f"{prefix}.{key}" if prefix else key

        # If it has properties, it's an object or nested type - recurse
        if "properties" in value:
            paths.extend(_get_all_field_paths(value["properties"], current_path))

        # If it has fields (multi-fields), recurse into those too
        if "fields" in value:
            for field_key in value["fields"].keys():
                paths.append(f"{current_path}.{field_key}")

        # Always add the current path if it's a leaf or has a type
        if "type" in value:
            paths.append(current_path)

    return paths


def analyze_field_path(es: Elasticsearch, index: str, field_path: str) -> Dict:
    """
    Returns:
      {
        "nested_paths": ["gene", "gene.ssm", ...],  # ancestors that are type 'nested'
        "leaf_type": "keyword"/"text"/"long"/...,
        "exists": True/False
      }
    """
    props = get_index_properties(es, index)
    parts = field_path.split(".")
    nested_paths: List[str] = []
    cur = {"properties": props}
    cur_path: List[str] = []

    for i, seg in enumerate(parts):
        # step into next segment
        child, via = _descend(cur, seg)
        cur_path.append(seg)

        # If this node is nested/object, record if nested
        node_type = child.get("type")
        if node_type == "nested":
            nested_paths.append(".".join(cur_path))

        # Move cursor for next iteration:
        # If we went through "fields", the field is a leaf variant (e.g., .keyword)
        # and there will be no "properties" below it.
        cur = child

    leaf_type = cur.get("type")
    return {
        "nested_paths": nested_paths,  # from root→deepest
        "leaf_type": leaf_type,
        "exists": leaf_type is not None,
    }


def build_wrapped_term_query(field_path: str, value, nested_paths: List[str]) -> Dict:
    """
    Build the innermost term and wrap with nested blocks from deepest→shallowest.
    """
    inner = {"term": {field_path: value}}
    for p in reversed(nested_paths):
        inner = {"nested": {"path": p, "query": inner}}
    return {"query": inner}


def build_wrapped_term_Q(field_path: str, value, nested_paths: List[str]):
    """
    Same as above but returns an elasticsearch-dsl Q (if installed).
    """
    if Q is None:
        raise RuntimeError("elasticsearch-dsl is not installed")
    q = Q("term", **{field_path: value})
    for p in reversed(nested_paths):
        q = Q("nested", path=p, query=q)
    return q


def build_wrapped_query_Q(value, nested_paths: List[str]):
    """
    Same as above but returns an elasticsearch-dsl Q (if installed).
    """
    if Q is None:
        raise RuntimeError("elasticsearch-dsl is not installed")
    q = value
    for p in reversed(nested_paths):
        q = Q("nested", path=p, query=Q("bool", must=[q]), ignore_unmapped=True)
    return q
