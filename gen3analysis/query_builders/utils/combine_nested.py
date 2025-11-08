from typing import List
from elasticsearch_dsl import Q
from elasticsearch_dsl.query import Query


def build_nested_hierarchy(root: str, nested_filters: List) -> Query:
    """
    Build a nested query hierarchy from a list of nested filters.
    Each nested path should appear only once, properly nested within its parent.

    Example:
    - gene.is_cancer_gene_census -> direct child of gene
    - gene.ssm.mutation_type -> nested within gene
    - gene.ssm.consequence.sift_impact -> nested within gene.ssm
    """

    # Group filters by their exact path
    filters_by_path = {}
    for nf in nested_filters:
        path = nf.path
        if path not in filters_by_path:
            filters_by_path[path] = []
        filters_by_path[path].append(nf.query)

    # Build the hierarchy from deepest to shallowest
    def build_for_path(current_path: str, all_paths: dict) -> Query:
        """
        Recursively build the nested query for a given path and all its children.
        """
        # Get queries directly at this path level
        current_queries = []
        if current_path in all_paths:
            current_queries = all_paths[current_path].copy()

        # Find all immediate child paths (one level deeper)
        child_paths = set()
        for path in all_paths.keys():
            if path.startswith(current_path + "."):
                # Check if it's an immediate child (not a grandchild)
                remainder = path[len(current_path) + 1 :]
                if "." not in remainder:
                    child_paths.add(path)

        # Recursively build nested queries for each child path
        for child_path in sorted(child_paths):
            child_nested_query = build_for_path(child_path, all_paths)
            current_queries.append(child_nested_query)

        # Combine all queries at this level
        if not current_queries:
            return None

        combined_query = (
            Q("bool", must=current_queries)
            if len(current_queries) > 1
            else current_queries[0]
        )

        # Wrap in nested query (unless this is the root being returned)
        return Q(
            "nested", path=current_path, ignore_unmapped=True, query=combined_query
        )

    # Start building from the root
    result = build_for_path(root, filters_by_path)
    return result if result else Q("match_all")


def combine_nested_queries_simple(filters: List) -> List:
    """
    Simpler version: Group all nested queries by their root path and combine them.
    For gene.ssm.consequence and gene.is_cancer_gene_census, both use root "gene"
    """
    from elasticsearch_dsl.query import Query

    nested_by_root = {}
    other_filters = []

    def get_root_path(path: str) -> str:
        """Get the root nested path (first segment)"""
        return path.split(".")[0]

    for f in filters:
        # Check if it's a Query object and specifically a Nested query
        if isinstance(f, Query) and hasattr(f, "name") and f.name == "nested":
            root = get_root_path(f.path)
            if root not in nested_by_root:
                nested_by_root[root] = []
            nested_by_root[root].append(f)
        else:
            other_filters.append(f)

    # For each root path, build the properly nested structure
    for root, nested_filters in nested_by_root.items():
        combined = build_nested_hierarchy(root, nested_filters)
        if combined:
            other_filters.append(combined)

    return other_filters
