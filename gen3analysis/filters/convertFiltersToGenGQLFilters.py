from typing import Optional, Dict, Any, Union
from filters import (
    FilterSet,
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
    NestedFilter,
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
    if isinstance(operation, Equals):
        return GQLEqual(equal_op={operation.field: operation.operand})

    elif isinstance(operation, NotEquals):
        return GQLNotEqual(not_equal_op={operation.field: operation.operand})

    elif isinstance(operation, LessThan):
        return GQLLessThan(less_than_op={operation.field: operation.operand})

    elif isinstance(operation, LessThanOrEquals):
        return GQLLessThanOrEquals(
            less_than_or_equals_op={operation.field: operation.operand}
        )

    elif isinstance(operation, GreaterThan):
        return GQLGreaterThan(greater_than_op={operation.field: operation.operand})

    elif isinstance(operation, GreaterThanOrEquals):
        return GQLGreaterThanOrEquals(
            greater_than_or_equals_op={operation.field: operation.operand}
        )

    elif isinstance(operation, Includes):
        return GQLIncludes(in_op={operation.field: operation.operands})

    elif isinstance(operation, Excludes):
        return GQLExcludes(exclude_op={operation.field: operation.operands})

    elif isinstance(operation, ExcludeIfAny):
        return GQLExcludeIfAny(exclude_if_any_op={operation.field: operation.operands})

    elif isinstance(operation, Intersection):
        converted_operands = [convert_operation_to_gql(op) for op in operation.operands]
        return GQLIntersection(
            and_op=[op for op in converted_operands if op is not None]
        )

    elif isinstance(operation, UnionOr):
        converted_operands = [convert_operation_to_gql(op) for op in operation.operands]
        return GQLUnion(or_op=[op for op in converted_operands if op is not None])

    elif isinstance(operation, NestedFilter):
        if operation.operand:
            converted_filter = convert_operation_to_gql(operation.operand)
            if converted_filter:
                nested_contents = NestedContents(
                    path=operation.path, filter_content=converted_filter
                )
                return GQLNestedFilter(nested_op=nested_contents)

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
