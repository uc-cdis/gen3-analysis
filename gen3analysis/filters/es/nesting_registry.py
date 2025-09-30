from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Literal
import json
import threading
from elasticsearch import Elasticsearch

Kind = Literal["term", "terms", "range", "exists", "neq"]


# ---------- Data model ----------
@dataclass(frozen=True)
class FieldInfo:
    field_path: str
    nested_paths: Tuple[str, ...]  # ordered shallow -> deep
    leaf_type: Optional[str]  # keyword, text, long, date, ...
    searchable: Optional[bool] = None
    aggregatable: Optional[bool] = None


# ---------- Mapping walkers (no network) ----------
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


def _analyze_from_props(root_props: Dict, field_path: str) -> FieldInfo:
    parts = field_path.split(".")
    cur = {"properties": root_props}
    nested_paths: List[str] = []
    cur_path: List[str] = []
    for seg in parts:
        cur = _descend(cur, seg)
        cur_path.append(seg)
        if cur.get("type") == "nested":
            nested_paths.append(".".join(cur_path))
    return FieldInfo(
        field_path=field_path,
        nested_paths=tuple(nested_paths),
        leaf_type=cur.get("type"),
    )


# ---------- ES fetch helpers ----------
def _get_root_props(es: Elasticsearch, index: str) -> Dict:
    m = es.indices.get_mapping(index=index)
    idx = next(iter(m))  # first matching index/alias
    return m[idx]["mappings"].get("properties", {})


def _field_caps(es: Elasticsearch, index: str, field: str) -> Dict:
    caps = es.field_caps(index=index, fields=field)
    return caps.get("fields", {}).get(field, {})


def _stable_mapping_hash(mappings: Dict) -> str:
    return json.dumps(mappings, sort_keys=True, separators=(",", ":"))


# ---------- Registry ----------
class NestingRegistry:
    """
    Caches nested ancestry for a subset of fields so handlers can build wrapped
    clauses without hitting ES mappings per request. Supports auto-refresh on
    mapping changes via hash comparison.
    """

    def __init__(self, index: str):
        self.index = index
        self._lock = threading.Lock()
        self._root_props: Optional[Dict] = None
        self._mapping_hash: Optional[str] = None
        self._by_field: Dict[str, FieldInfo] = {}

    @classmethod
    def build(
        cls,
        es: Elasticsearch,
        index: str,
        fields: Iterable[str],
        *,
        include_caps: bool = True,
    ) -> "NestingRegistry":
        reg = cls(index)
        reg._refresh_from_es(es)
        reg._prime(es, fields, include_caps=include_caps)
        return reg

    def _refresh_from_es(self, es: Elasticsearch) -> None:
        with self._lock:
            m = es.indices.get_mapping(index=self.index)
            idx = next(iter(m))
            self._root_props = m[idx]["mappings"].get("properties", {})
            self._mapping_hash = _stable_mapping_hash(m[idx]["mappings"])

    def _prime(
        self, es: Elasticsearch, fields: Iterable[str], *, include_caps: bool
    ) -> None:
        assert self._root_props is not None
        with self._lock:
            for f in fields:
                info = _analyze_from_props(self._root_props, f)
                if include_caps:
                    caps = _field_caps(es, self.index, f)
                    # caps is a dict keyed by type across indices; be permissive
                    searchable = (
                        any(v.get("searchable", False) for v in caps.values())
                        if caps
                        else None
                    )
                    aggregatable = (
                        any(v.get("aggregatable", False) for v in caps.values())
                        if caps
                        else None
                    )
                    info = FieldInfo(
                        field_path=info.field_path,
                        nested_paths=info.nested_paths,
                        leaf_type=info.leaf_type,
                        searchable=searchable,
                        aggregatable=aggregatable,
                    )
                self._by_field[f] = info

    def refresh_if_mapping_changed(self, es: Elasticsearch) -> bool:
        """
        Re-fetch mapping if hash changed. Returns True if refresh occurred.
        Call this on a schedule (e.g., every few minutes) or expose an admin endpoint.
        """
        m = es.indices.get_mapping(index=self.index)
        idx = next(iter(m))
        new_hash = _stable_mapping_hash(m[idx]["mappings"])
        with self._lock:
            if new_hash != self._mapping_hash:
                self._root_props = m[idx]["mappings"].get("properties", {})
                self._mapping_hash = new_hash
                self._by_field.clear()
                return True
        return False

    def get(self, field: str) -> Optional[FieldInfo]:
        with self._lock:
            if field not in self._by_field:
                return None
            return self._by_field[field]

    def ensure(
        self, es: Elasticsearch, field: str, *, include_caps: bool = True
    ) -> FieldInfo:
        """
        Lazily add a field not in the original subset (useful when queries evolve).
        """
        with self._lock:
            if field in self._by_field:
                return self._by_field[field]
            assert self._root_props is not None
            info = _analyze_from_props(self._root_props, field)
            if include_caps:
                caps = _field_caps(es, self.index, field)
                searchable = (
                    any(v.get("searchable", False) for v in caps.values())
                    if caps
                    else None
                )
                aggregatable = (
                    any(v.get("aggregatable", False) for v in caps.values())
                    if caps
                    else None
                )
                info = FieldInfo(
                    field_path=info.field_path,
                    nested_paths=info.nested_paths,
                    leaf_type=info.leaf_type,
                    searchable=searchable,
                    aggregatable=aggregatable,
                )
            self._by_field[field] = info
            return info

    # ---------- Query helpers using the cache ----------
    @staticmethod
    def _leaf(kind: Kind, field: str, value: Any = None, **ops) -> Dict:
        if kind == "term":
            return {"term": {field: value}}
        if kind == "terms":
            return {"terms": {field: list(value)}}
        if kind == "range":
            rops = {k: v for k, v in ops.items() if k in ("gt", "gte", "lt", "lte")}
            if not rops:
                raise ValueError("range requires one of gt/gte/lt/lte")
            return {"range": {field: rops}}
        if kind == "exists":
            return {"exists": {"field": field}}
        if kind == "neq":
            return {"bool": {"must_not": [{"term": {field: value}}]}}
        raise ValueError(f"Unsupported kind {kind}")

    @staticmethod
    def _wrap(inner: Dict, nested_paths: Tuple[str, ...]) -> Dict:
        wrapped = inner
        for p in reversed(nested_paths):  # deepest → shallowest
            wrapped = {"nested": {"path": p, "query": wrapped}}
        return wrapped

    def clause(
        self,
        field: str,
        kind: Kind,
        value: Any = None,
        *,
        require_exists_for_neq: bool = False,
        **range_ops,
    ) -> Dict:
        info = self.get(field)
        inner = self._leaf(kind, field, value, **range_ops)
        if kind == "neq" and require_exists_for_neq:
            inner = {
                "bool": {
                    "must": [{"exists": {"field": field}}],
                    "must_not": inner["bool"]["must_not"],
                }
            }
        return self._wrap(inner, info.nested_paths)

    def terms_agg(
        self,
        field: str,
        agg_name: str = "by_field",
        size: int = 10,
        order: Optional[Dict[str, str]] = None,
    ) -> Dict:
        """
        Returns a full body: { "size": 0, "aggs": {...} } with correct nested buckets.
        """
        info = self.get(field)
        body: Dict[str, Any] = {"size": 0, "aggs": {}}
        cursor = body["aggs"]
        for i, p in enumerate(info.nested_paths):
            bucket = f"nested_{i}"
            cursor[bucket] = {"nested": {"path": p}, "aggs": {}}
            cursor = cursor[bucket]["aggs"]
        cursor[agg_name] = {"terms": {"field": field, "size": size}}
        if order:
            cursor[agg_name]["terms"]["order"] = order
        return body
