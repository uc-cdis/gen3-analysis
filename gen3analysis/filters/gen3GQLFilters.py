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

    def get_field(self):
        return list(self.in_op.keys())[0]

    def get_values(self) -> List[Union[str, int]]:
        return self.in_op.get(self.get_field(), [])

    def search(self, s: str) -> bool:
        if s in self.get_field():
            return True
        return False


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

    @classmethod
    def from_payload(cls, data: Dict[str, Any]) -> "GQLIntersection":
        """
        Custom constructor that parses a raw payload like:
          {"and": [ {...}, {...} ]}
        into a GQLIntersection with parsed operand filters.
        """
        items = data.get("and", [])
        parsed: List["GQLFilter"] = []
        for item in items:
            if isinstance(item, dict):
                f = parse_gql_filter(item)
                if f is not None:
                    parsed.append(f)
        return cls(and_op=parsed)

    @classmethod
    def from_operands(cls, operands: List["GQLFilter"]) -> "GQLIntersection":
        """
        Custom constructor that accepts already-built GQLFilter operands.
        """
        return cls(and_op=[op for op in operands if op is not None])

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLIntersection":
        return GQLIntersection.from_payload(data)

    def to_dict(self) -> Dict[str, Any]:
        return {"and": [x.to_dict() for x in self.and_op]}

    def search(self, s: str) -> bool:
        return False


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class GQLUnion:
    """GraphQL Union filter: {'or': [filter1, filter2, ...]}"""

    or_op: List["GQLFilter"] = field(
        default_factory=list, metadata=config(field_name="or")
    )

    @classmethod
    def from_payload(cls, data: Dict[str, Any]) -> "GQLUnion":
        # convert each member of "and" to its concrete GQLFilter using parse_gql_filter
        items = data.get("or", [])
        parsed = []
        for item in items:
            f = parse_gql_filter(item) if isinstance(item, dict) else None
            if f is not None:
                parsed.append(f)
        return cls(or_op=parsed)

    def to_dict(self) -> Dict[str, Any]:
        return {"or": [x.to_dict() for x in self.or_op]}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLUnion":
        return GQLUnion.from_payload(data)

    def search(self, s: str) -> bool:
        return False


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class NestedContents:
    """Contents of a nested filter including path"""

    path: str = ""
    # This will contain the actual filter content
    filter_content: "GQLFilter" = None

    @classmethod
    def from_payload(cls, data: Dict[str, Any]) -> "NestedContents":
        path = data.get("path", "")
        # Extract filter content (everything except path)
        filter_data = {k: v for k, v in data.items() if k != "path"}
        filter_content = parse_gql_filter(filter_data) if filter_data else None
        return cls(path=path, filter_content=filter_content)

    def to_dict(self) -> Dict[str, Any]:
        result = {"path": self.path}
        if self.filter_content and hasattr(self.filter_content, "to_dict"):
            result.update(self.filter_content.to_dict())
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NestedContents":
        return NestedContents.from_payload(data)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GQLNestedFilter:
    """GraphQL Nested filter: {'nested': {'path': 'field.subfield', ...filter}}"""

    nested_op: NestedContents = field(
        default_factory=NestedContents, metadata=config(field_name="nested")
    )

    @classmethod
    def from_payload(cls, data: Dict[str, Any]) -> "GQLNestedFilter":
        return cls(nested_op=NestedContents.from_payload(data.get("nested", {})))

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GQLNestedFilter":
        return GQLNestedFilter.from_payload(data)


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
    if isinstance(value, GQLIntersection):
        return True
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
    if isinstance(value, GQLUnion):
        return True
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
    if isinstance(value, GQLNestedFilter):
        return True
    if (
        isinstance(value, dict)
        and "nested" in value
        and isinstance(value.get("nested"), dict)
        and "path" in value.get("nested", {})
    ):
        return True
    return False


def is_gql_equal(value: Any) -> bool:
    """Check if value is a GQLEqual filter"""

    if isinstance(value, GQLEqual):
        return True

    if isinstance(value, dict) and "=" in value:
        return True
    return False


def is_gql_not_equal(value: Any) -> bool:
    """Check if value is a GQLNotEqual filter"""
    if isinstance(value, GQLNotEqual):
        return True
    return isinstance(value, dict) and "!=" in value


def is_gql_less_than(value: Any) -> bool:
    """Check if value is a GQLLessThan filter"""
    if isinstance(value, GQLLessThan):
        return True
    return isinstance(value, dict) and "<" in value


def is_gql_less_than_or_equals(value: Any) -> bool:
    """Check if value is a GQLLessThanOrEquals filter"""
    if isinstance(value, GQLLessThanOrEquals):
        return True
    return isinstance(value, dict) and "<=" in value


def is_gql_greater_than(value: Any) -> bool:
    """Check if value is a GQLGreaterThan filter"""
    if isinstance(value, GQLGreaterThan):
        return True
    return isinstance(value, dict) and ">" in value


def is_gql_greater_than_or_equals(value: Any) -> bool:
    """Check if value is a GQLGreaterThanOrEquals filter"""
    if isinstance(value, GQLGreaterThanOrEquals):
        return True
    return isinstance(value, dict) and ">=" in value


def is_gql_includes(value: Any) -> bool:
    """Check if value is a GQLIncludes filter"""
    if isinstance(value, GQLIncludes):
        return True
    return isinstance(value, dict) and "in" in value


def is_gql_excludes(value: Any) -> bool:
    """Check if value is a GQLExcludes filter"""
    if isinstance(value, GQLExcludes):
        return True
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
        return GQLIntersection.from_payload(data)
    elif "or" in data:
        return GQLUnion.from_payload(data)
    elif "nested" in data:
        return GQLNestedFilter.from_payload(data)
    else:
        return None


def get_gql_filter_contents(f: GQLFilter) -> Optional[List[GQLFilter]]:
    if f is None:
        return []

    if is_gql_intersection(f):
        return f.and_op

    if is_gql_union(f):
        return f.or_op

    return []
