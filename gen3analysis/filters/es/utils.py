from typing import List, Dict, Any
import json


def inspect_index_mapping(es_client, index_name: str) -> Dict[str, Any]:
    """
    Retrieve and display the mapping for an index to help debug nested field issues.

    Args:
        es_client: Elasticsearch client instance
        index_name: Name of the Elasticsearch index

    Returns:
        Dictionary containing the index mapping

    Example:
        >>> from elasticsearch import Elasticsearch
        >>> es = Elasticsearch(['localhost:9200'])
        >>> mapping = inspect_index_mapping(es, "ssm_centric")
        >>> print(json.dumps(mapping, indent=2))
    """
    try:
        mapping = es_client.indices.get_mapping(index=index_name)
        return mapping
    except Exception as e:
        print(f"Error retrieving mapping: {e}")
        return {}


def find_nested_paths(mapping: Dict[str, Any], prefix: str = "") -> List[str]:
    """
    Recursively find all nested paths in an Elasticsearch mapping.

    Args:
        mapping: The mapping dictionary or properties section
        prefix: Current path prefix (used in recursion)

    Returns:
        List of nested field paths

    Example:
        >>> mapping = inspect_index_mapping(es, "ssm_centric")
        >>> nested_paths = find_nested_paths(mapping)
        >>> print("Nested paths:", nested_paths)
    """
    nested_paths = []

    if isinstance(mapping, dict):
        # Check if this is a properties section
        if "properties" in mapping:
            mapping = mapping["properties"]

        # Check each index in the mapping
        for index_name, index_data in mapping.items():
            if isinstance(index_data, dict) and "mappings" in index_data:
                return find_nested_paths(index_data["mappings"], prefix)

        # Iterate through fields
        for field_name, field_data in mapping.items():
            if isinstance(field_data, dict):
                current_path = f"{prefix}.{field_name}" if prefix else field_name

                # Check if this field is nested
                if field_data.get("type") == "nested":
                    nested_paths.append(current_path)

                # Recurse into properties
                if "properties" in field_data:
                    nested_paths.extend(
                        find_nested_paths(field_data["properties"], current_path)
                    )

    return nested_paths
