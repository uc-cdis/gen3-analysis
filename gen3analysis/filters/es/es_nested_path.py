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


def _descend(node: Dict, segment: str) -> Tuple[Dict, str]:
    """
    Move one step down the mapping. Handles multi-fields:
    e.g., if previous node was {"type": "text", "fields": {"keyword": {"type": "keyword"}}}
    and the segment is "keyword", descend into node["fields"]["keyword"].
    Returns (child_node, how_we_got_there) where second is "properties" or "fields".
    """
    if "properties" in node and segment in node["properties"]:
        return node["properties"][segment], "properties"
    # multi-field branch
    if "fields" in node and segment in node["fields"]:
        return node["fields"][segment], "fields"
    raise KeyError(
        f"Path segment '{segment}' not found in mapping; node keys={list(node.keys())}"
    )


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
