from typing import Optional

from filters import (
    FilterSet,
    Operation,
    LessThanOrEquals,
    GreaterThan,
    GreaterThanOrEquals,
    is_greaterThan,
    is_intersection,
    is_excludes,
    is_excludeifany,
    is_nested,
    is_union,
    is_lessThan,
    is_lessThanOrEquals,
    is_greaterThanOrEquals,
    is_equals,
    is_notequals,
    is_includes,
)
from gen3GQLFilters import (
    GQLFilter,
    GQLEqual,
    GQLNotEqual,
    GQLLessThan,
    GQLLessThanOrEquals,
    GQLGreaterThan,
    GQLGreaterThanOrEquals,
    GQLIncludes,
    GQLExcludes,
    GQLExcludeIfAny,
    GQLIntersection,
    GQLUnion,
    GQLNestedFilter,
    NestedContents,
)


def convert_operation_to_gql(operation: Operation) -> Optional[GQLFilter]:
    """Convert a single Operation to its corresponding GQLFilter."""

    def _create_simple_filter(gql_class, field_key, value_key):
        """Create a simple filter with field and operand."""
        return gql_class(
            **{field_key: {operation.field: getattr(operation, value_key)}}
        )

    def _create_list_filter(gql_class, field_key):
        """Create a filter with field and operands list."""
        return gql_class(**{field_key: {operation.field: operation.operands}})

    def _create_composite_filter(gql_class, field_key):
        """Create a composite filter (intersection/union)."""
        converted_operands = [convert_operation_to_gql(op) for op in operation.operands]
        filtered_operands = [op for op in converted_operands if op is not None]
        return gql_class(**{field_key: filtered_operands})

    def _create_nested_filter():
        """Create a nested filter."""
        if operation.operand:
            converted_filter = convert_operation_to_gql(operation.operand)
            if converted_filter:
                nested_contents = NestedContents(
                    path=operation.path, filter_content=converted_filter
                )
                return GQLNestedFilter(nested_op=nested_contents)
        return None

    # Mapping of operation checkers to their corresponding filter creators
    operation_mapping = {
        is_equals: lambda: _create_simple_filter(GQLEqual, "equal_op", "operand"),
        is_notequals: lambda: _create_simple_filter(
            GQLNotEqual, "not_equal_op", "operand"
        ),
        is_lessThan: lambda: _create_simple_filter(
            GQLLessThan, "less_than_op", "operand"
        ),
        is_lessThanOrEquals: lambda: _create_simple_filter(
            GQLLessThanOrEquals, "less_than_or_equals_op", "operand"
        ),
        is_greaterThan: lambda: _create_simple_filter(
            GQLGreaterThan, "greater_than_op", "operand"
        ),
        is_greaterThanOrEquals: lambda: _create_simple_filter(
            GQLGreaterThanOrEquals, "greater_than_or_equals_op", "operand"
        ),
        is_includes: lambda: _create_list_filter(GQLIncludes, "in_op"),
        is_excludes: lambda: _create_list_filter(GQLExcludes, "exclude_op"),
        is_excludeifany: lambda: _create_list_filter(
            GQLExcludeIfAny, "exclude_if_any_op"
        ),
        is_intersection: lambda: _create_composite_filter(GQLIntersection, "and_op"),
        is_union: lambda: _create_composite_filter(GQLUnion, "or_op"),
        is_nested: _create_nested_filter,
    }

    # Find and execute the appropriate converter
    for checker, converter in operation_mapping.items():
        if checker(operation):
            return converter()

    return None


def convert_filter_set_to_gql(filter_set: FilterSet) -> Optional[GQLFilter]:
    """Convert a FilterSet to GQLFilter representation."""
    if not filter_set.root:
        return None

    # Convert all operations in the root
    converted_filters = []
    for operation in filter_set.root.values():
        converted = convert_operation_to_gql(operation)
        if converted:
            converted_filters.append(converted)

    if not converted_filters:
        return None

    # If there's only one filter, return it directly
    if len(converted_filters) == 1:
        return converted_filters[0]

    # Otherwise, wrap in appropriate intersection/union based on mode
    if filter_set.mode == "and":
        return GQLIntersection(and_op=converted_filters)
    else:  # mode == 'or'
        return GQLUnion(or_op=converted_filters)
