from typing import Optional, Dict, Any

from .filters import (
    Operation,
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEquals,
    GreaterThan,
    GreaterThanOrEquals,
    Includes,
    Excludes,
    ExcludeIfAny,
    Intersection,
    UnionOr,
    Exists,
    Missing,
)
from .gdcGQLFilters import (
    GqlOperation,
    is_gql_missing,
    is_gql_exists,
    is_gql_exclude_if_any,
    is_gql_equals,
    is_gql_not_equals,
    is_gql_less_than,
    is_gql_greater_than,
    is_gql_less_than_or_equals,
    is_gql_excludes,
    is_gql_union,
    is_gql_includes,
    is_gql_intersection,
)


def convert_gql_operation_to_operation(
    gql_operation: GqlOperation,
) -> Optional[Operation]:
    """
    Convert a GqlOperation to its corresponding Operation.

    Args:
        gql_operation: The GqlOperation to convert

    Returns:
        The converted Operation or None if conversion is not possible
    """
    if is_gql_equals(gql_operation):
        return Equals(
            field=gql_operation["content"]["field"],
            operand=gql_operation["content"]["value"],
        )

    elif is_gql_not_equals(gql_operation):
        return NotEquals(
            field=gql_operation["content"]["field"],
            operand=gql_operation["content"]["value"],
        )

    elif is_gql_less_than(gql_operation):
        return LessThan(
            field=gql_operation["content"]["field"],
            operand=gql_operation["content"]["value"],
        )

    elif is_gql_less_than_or_equals(gql_operation):
        return LessThanOrEquals(
            field=gql_operation["content"]["field"],
            operand=gql_operation["content"]["value"],
        )

    elif is_gql_greater_than(gql_operation):
        return GreaterThan(
            field=gql_operation["content"]["field"],
            operand=gql_operation["content"]["value"],
        )

    elif is_gql_less_than_or_equals(gql_operation):
        return GreaterThanOrEquals(
            field=gql_operation["content"]["field"],
            operand=gql_operation["content"]["value"],
        )

    elif is_gql_includes(gql_operation):
        return Includes(
            field=gql_operation["content"]["field"],
            operands=gql_operation["content"]["value"],
        )

    elif is_gql_excludes(gql_operation):
        return Excludes(
            field=gql_operation["content"]["field"],
            operands=gql_operation["content"]["value"],
        )

    elif is_gql_exclude_if_any(gql_operation):
        # Handle the case where value might be a single item or a list
        operands = gql_operation["content"]["value"]
        if not isinstance(operands, list):
            operands = [operands]

        return ExcludeIfAny(field=gql_operation["content"]["field"], operands=operands)

    elif is_gql_missing(gql_operation):
        return Missing(field=gql_operation["content"]["field"])

    elif is_gql_exists(gql_operation):
        return Exists(field=gql_operation["content"]["field"])

    elif is_gql_intersection(gql_operation):
        converted_operations = []
        for op in gql_operation["content"]:
            converted = convert_gql_operation_to_operation(op)
            if converted:
                converted_operations.append(converted)

        return Intersection(operands=converted_operations)

    elif is_gql_union(gql_operation):
        converted_operations = []
        for op in gql_operation["content"]:
            converted = convert_gql_operation_to_operation(op)
            if converted:
                converted_operations.append(converted)

        return UnionOr(operands=converted_operations)

    return None


def convert_gql_filter_to_filter_set(gql_operation: GqlOperation) -> Dict[str, Any]:
    """
    Convert a GqlOperation to a FilterSet representation.

    Args:
        gql_operation: The GqlOperation to convert

    Returns:
        A dictionary representing a FilterSet with the converted operation
    """
    operation = convert_gql_operation_to_operation(gql_operation)
    if not operation:
        return {"root": {}, "mode": "and"}

    # Determine mode based on the top-level operation
    mode = "and"
    root = {}

    # If the top-level operation is already an Intersection or Union,
    # extract its operands and set the appropriate mode
    if isinstance(operation, Intersection):
        mode = "and"
        for i, op in enumerate(operation.operands):
            root[f"filter{i + 1}"] = op
    elif isinstance(operation, UnionOr):
        mode = "or"
        for i, op in enumerate(operation.operands):
            root[f"filter{i + 1}"] = op
    else:
        # Single operation, add it directly to the root
        root["filter1"] = operation

    return {"root": root, "mode": mode}
