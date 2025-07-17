from dataclasses import dataclass, field
from typing import List, Union, Dict, Any, Optional
from dataclasses_json import dataclass_json, LetterCase, config


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLEqual:
    """GraphQL Equal filter: {'=': {'field': 'value'}}"""

    equal_op: Dict[str, Union[str, int]] = field(
        default_factory=dict, metadata=config(field_name="!=")
    )

    def to_dict(self) -> Dict[str, Any]:
        return {"=": self.equal_op}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLEqual":
        return cls(equal_op=data.get("=", {}))


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLNotEqual:
    """GraphQL Not Equal filter: {'!=': {'field': 'value'}}"""

    not_equal_op: Dict[str, Union[str, int]] = field(
        default_factory=dict, metadata=config(field_name="!=")
    )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLNotEqual":
        return cls(not_equal_op=data.get("!=", {}))


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLLessThan:
    """GraphQL Less Than filter: {'<': {'field': 'value'}}"""

    less_than_op: Dict[str, Union[str, int]] = field(
        default_factory=dict, metadata=config(field_name="<")
    )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLLessThan":
        return cls(less_than_op=data.get("<", {}))


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLLessThanOrEquals:
    """GraphQL Less Than Or Equals filter: {'<=': {'field': 'value'}}"""

    less_than_or_equals_op: Dict[str, Union[str, int]] = field(
        default_factory=dict, metadata=config(field_name="<=")
    )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLLessThanOrEquals":
        return cls(less_than_or_equals_op=data.get("<=", {}))


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLGreaterThan:
    """GraphQL Greater Than filter: {'>': {'field': 'value'}}"""

    greater_than_op: Dict[str, Union[str, int]] = field(
        default_factory=dict, metadata=config(field_name=">")
    )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLGreaterThan":
        return cls(greater_than_op=data.get(">", {}))


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLGreaterThanOrEquals:
    """GraphQL Greater Than Or Equals filter: {'>=': {'field': 'value'}}"""

    greater_than_or_equals_op: Dict[str, Union[str, int]] = field(
        default_factory=dict, metadata=config(field_name=">=")
    )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLGreaterThanOrEquals":
        return cls(greater_than_or_equals_op=data.get(">=", {}))


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLIncludes:
    """GraphQL Includes filter: {'in': {'field': ['value1', 'value2']}}"""

    in_op: Dict[str, List[Union[str, int]]] = field(
        default_factory=dict, metadata=config(field_name="in")
    )

    def to_dict(self) -> Dict[str, Any]:
        return {"in": self.in_op}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLIncludes":
        return cls(in_op=data.get("in", {}))


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLExcludes:
    """GraphQL Excludes filter: {'exclude': {'field': ['value1', 'value2']}}"""

    exclude_op: Dict[str, List[Union[str, int]]] = field(
        default_factory=dict, metadata=config(field_name="exclude")
    )

    def to_dict(self) -> Dict[str, Any]:
        return {"exclude": self.exclude_op}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLExcludes":
        return cls(exclude_op=data.get("exclude", {}))


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLExcludeIfAny:
    """GraphQL Exclude If Any filter: {'excludeifany': {'field': ['value1', 'value2']}}"""

    exclude_if_any_op: Dict[str, List[Union[str, int]]] = field(
        default_factory=dict, metadata=config(field_name="excludeifany")
    )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLExcludeIfAny":
        return cls(exclude_if_any_op=data.get("excludeifany", {}))


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLIntersection:
    """GraphQL Intersection filter: {'and': [filter1, filter2, ...]}"""

    and_op: List["GQLFilter"] = field(
        default_factory=list, metadata=config(field_name="and")
    )


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class GQLUnion:
    """GraphQL Union filter: {'or': [filter1, filter2, ...]}"""

    or_op: List["GQLFilter"] = field(
        default_factory=list, metadata=config(field_name="or")
    )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLUnion":
        return cls(or_op=data.get("or", []))


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class NestedContents:
    """Contents of a nested filter including path"""

    path: str = ""
    # This will contain the actual filter content
    filter_content: "GQLFilter" = None

    def to_dict(self) -> Dict[str, Any]:
        result = {"path": self.path}
        if self.filter_content and hasattr(self.filter_content, "to_dict"):
            result.update(self.filter_content.to_dict())
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NestedContents":
        path = data.get("path", "")
        # Extract filter content (everything except path)
        filter_data = {k: v for k, v in data.items() if k != "path"}
        filter_content = parse_gql_filter(filter_data) if filter_data else None
        return cls(path=path, filter_content=filter_content)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLNestedFilter:
    """GraphQL Nested filter: {'nested': {'path': 'field.subfield', ...filter}}"""

    nested_op: NestedContents = field(
        default_factory=NestedContents, metadata=config(field_name="nested")
    )


# Union type for all GQL filters
GQLFilter = Union[
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
]


def is_gql_intersection(value: Any) -> bool:
    """
    Type guard to check if an object is a GQLIntersection

    Args:
        value: The value to check

    Returns:
        True if the value is a GQLIntersection
    """
    return (
        isinstance(value, dict)
        and "and" in value
        and isinstance(value.get("and"), list)
    )


def is_gql_union(value: Any) -> bool:
    """
    Type guard to check if an object is a GQLUnion

    Args:
        value: The value to check

    Returns:
        True if the value is a GQLUnion
    """
    return (
        isinstance(value, dict) and "or" in value and isinstance(value.get("or"), list)
    )


def is_gql_nested_filter(value: Any) -> bool:
    """
    Type guard to check if an object is a GQLNestedFilter

    Args:
        value: The value to check

    Returns:
        True if the value is a GQLNestedFilter
    """
    return (
        isinstance(value, dict)
        and "nested" in value
        and isinstance(value.get("nested"), dict)
        and "path" in value.get("nested", {})
    )


def is_gql_equal(value: Any) -> bool:
    """Check if value is a GQLEqual filter"""
    return isinstance(value, dict) and "=" in value


def is_gql_not_equal(value: Any) -> bool:
    """Check if value is a GQLNotEqual filter"""
    return isinstance(value, dict) and "!=" in value


def is_gql_less_than(value: Any) -> bool:
    """Check if value is a GQLLessThan filter"""
    return isinstance(value, dict) and "<" in value


def is_gql_less_than_or_equals(value: Any) -> bool:
    """Check if value is a GQLLessThanOrEquals filter"""
    return isinstance(value, dict) and "<=" in value


def is_gql_greater_than(value: Any) -> bool:
    """Check if value is a GQLGreaterThan filter"""
    return isinstance(value, dict) and ">" in value


def is_gql_greater_than_or_equals(value: Any) -> bool:
    """Check if value is a GQLGreaterThanOrEquals filter"""
    return isinstance(value, dict) and ">=" in value


def is_gql_includes(value: Any) -> bool:
    """Check if value is a GQLIncludes filter"""
    return isinstance(value, dict) and "in" in value


def is_gql_excludes(value: Any) -> bool:
    """Check if value is a GQLExcludes filter"""
    return isinstance(value, dict) and "exclude" in value


def is_gql_exclude_if_any(value: Any) -> bool:
    """Check if value is a GQLExcludeIfAny filter"""
    return isinstance(value, dict) and "excludeifany" in value


# Helper function to parse GQL filters from dict
def parse_gql_filter(data: Dict[str, Any]) -> Optional["GQLFilter"]:
    """Parse a dictionary into the appropriate GQLFilter type"""
    if not data:
        return None

    if "=" in data:
        return GQLEqual.from_dict(data)
    elif "!=" in data:
        return GQLNotEqual.from_dict(data)
    elif "<" in data:
        return GQLLessThan.from_dict(data)
    elif "<=" in data:
        return GQLLessThanOrEquals.from_dict(data)
    elif ">" in data:
        return GQLGreaterThan.from_dict(data)
    elif ">=" in data:
        return GQLGreaterThanOrEquals.from_dict(data)
    elif "in" in data:
        return GQLIncludes.from_dict(data)
    elif "exclude" in data:
        return GQLExcludes.from_dict(data)
    elif "excludeifany" in data:
        return GQLExcludeIfAny.from_dict(data)
    elif "and" in data:
        return GQLIntersection.from_dict(data)
    elif "or" in data:
        return GQLUnion.from_dict(data)
    elif "nested" in data:
        return GQLNestedFilter.from_dict(data)
    else:
        return None
