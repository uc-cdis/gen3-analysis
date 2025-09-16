from typing import Dict, Optional, List
from gen3analysis.gen3.guppyQuery import GuppyGQLClient
from glom import glom


def update_filters_with_object_ids(
    filter: Dict, field_name: str, ids: List[str]
) -> None:
    """
    Look for a field_name in a nested dictionary and replace its contents with ids if it's a list.

    Args:
        filter (Dict): The nested dictionary to search and modify
        field_name (str): The name of the field to find and update
        ids (List[str]): The list of IDs to replace the field's contents with
    """

    def _recursive_update(obj, target_field, new_ids):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == target_field and isinstance(value, list):
                    # Found the target field and it's a list, replace its contents
                    obj[key] = new_ids[:]  # Create a copy of the ids list
                elif isinstance(value, (dict, list)):
                    # Continue searching recursively
                    _recursive_update(value, target_field, new_ids)
        elif isinstance(obj, list):
            # If the current object is a list, check each item
            for item in obj:
                if isinstance(item, (dict, list)):
                    _recursive_update(item, target_field, new_ids)

    _recursive_update(filter, field_name, ids)


def dot_notation_to_graphql(dot_string: str) -> str:
    """
    Convert a dot notation string into a GraphQL nested query structure.

    Args:
        dot_string (str): A dot-separated string like "user.profile.name"

    Returns:
        str: A GraphQL nested query string like "user { profile { name } }"

    Example:
        >>> dot_notation_to_graphql("user.profile.name")
        "user { profile { name } }"

        >>> dot_notation_to_graphql("cases.samples.aliquots.submitter_id")
        "cases { samples { aliquots { submitter_id } } }"
    """
    if not dot_string or not isinstance(dot_string, str):
        return ""

    # Split the dot notation into parts
    parts = dot_string.split(".")

    if len(parts) == 1:
        # Single field, no nesting needed
        return parts[0]

    # Build nested structure from the inside out
    result = parts[-1]  # Start with the innermost field

    # Work backwards through the parts, wrapping each level
    for part in reversed(parts[:-1]):
        result = f"{part} {{ {result} }}"

    return result
