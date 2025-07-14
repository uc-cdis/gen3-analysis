from dataclasses import dataclass, field
from typing import List, Union, Optional, Any, Dict, Literal
from dataclasses_json import dataclass_json, LetterCase


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlEqualsContent:
    """Content for GQL equals operation"""

    field: str = ""
    value: Union[str, int] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlEquals:
    """GQL Equals operation: {'op': '=', 'content': {'field': 'name', 'value': 'John'}}"""

    op: Literal["="] = "="
    content: Union[str, int] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlNotEqualsContent:
    """Content for GQL not equals operation"""

    field: str = ""
    value: Union[str, int] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlNotEquals:
    """GQL Not Equals operation: {'op': '!=', 'content': {'field': 'name', 'value': 'John'}}"""

    op: Literal["!="] = "!="
    content: Union[str, int] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlLessThanContent:
    """Content for GQL less than operation"""

    field: str = ""
    value: Union[str, int] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlLessThan:
    """GQL Less Than operation: {'op': '<', 'content': {'field': 'age', 'value': 30}}"""

    op: Literal["<"] = "<"
    content: Union[str, int] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlLessThanOrEqualsContent:
    """Content for GQL less than or equals operation"""

    field: str = ""
    value: Union[str, int] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlLessThanOrEquals:
    """GQL Less Than Or Equals operation: {'op': '<=', 'content': {'field': 'age', 'value': 30}}"""

    op: Literal["<="] = "<="
    content: GqlLessThanOrEqualsContent = field(
        default_factory=GqlLessThanOrEqualsContent
    )


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlGreaterThanContent:
    """Content for GQL greater than operation"""

    field: str = ""
    value: Union[str, int] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlGreaterThan:
    """GQL Greater Than operation: {'op': '>', 'content': {'field': 'score', 'value': 85}}"""

    op: Literal[">"] = ">"
    content: GqlGreaterThanContent = field(default_factory=GqlGreaterThanContent)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlGreaterThanOrEqualsContent:
    """Content for GQL greater than or equals operation"""

    field: str = ""
    value: Union[str, int] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlGreaterThanOrEquals:
    """GQL Greater Than Or Equals operation: {'op': '>=', 'content': {'field': 'score', 'value': 85}}"""

    op: Literal[">="] = ">="
    content: GqlGreaterThanOrEqualsContent = field(
        default_factory=GqlGreaterThanOrEqualsContent
    )


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlMissingContent:
    """Content for GQL missing operation"""

    field: str = ""
    value: Literal["MISSING"] = "MISSING"


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlMissing:
    """GQL Missing operation: {'op': 'is', 'content': {'field': 'email', 'value': 'MISSING'}}"""

    op: Literal["is"] = "is"
    content: GqlMissingContent = field(default_factory=GqlMissingContent)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlExistsContent:
    """Content for GQL exists operation"""

    field: str = ""
    value: Optional[str] = None


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlExists:
    """GQL Exists operation: {'op': 'not', 'content': {'field': 'email', 'value': null}}"""

    op: Literal["not"] = "not"
    content: GqlExistsContent = field(default_factory=GqlExistsContent)


# @dataclass_json(letter_case=LetterCase.CAMEL)
# @dataclass(frozen=True)
# class GqlIncludesContent:
#     """Content for GQL includes operation"""
#     field: str = ''
#     value: List[Union[str, int]] = field(default_factory=list)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlIncludes:
    """GQL Includes operation: {'op': 'in', 'content': {'field': 'category', 'value': ['tech', 'science']}}"""

    op: Literal["in"] = "in"
    content: List[Union[str, int]] = field(default_factory=list)


# @dataclass_json(letter_case=LetterCase.CAMEL)
# @dataclass(frozen=True)
# class GqlExcludesContent:
#     """Content for GQL excludes operation"""
#     field: str = ''
#     value: List[Union[str, int]] = field(default_factory=list)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlExcludes:
    """GQL Excludes operation: {'op': 'exclude', 'content': {'field': 'category', 'value': ['spam', 'test']}}"""

    op: Literal["exclude"] = "exclude"
    content: List[Union[str, int]] = field(default_factory=list)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlExcludeIfAnyContent:
    """Content for GQL exclude if any operation"""

    field: str = ""
    value: Union[str, List[Union[str, int]]] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlExcludeIfAny:
    """GQL Exclude If Any operation: {'op': 'excludeifany', 'content': {'field': 'tags', 'value': ['sensitive', 'private']}}"""

    op: Literal["excludeifany"] = "excludeifany"
    content: GqlExcludeIfAnyContent = field(default_factory=GqlExcludeIfAnyContent)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlIntersection:
    """GQL Intersection operation: {'op': 'and', 'content': [operation1, operation2, ...]}"""

    op: Literal["and"] = "and"
    content: List["GqlOperation"] = field(default_factory=list)
    is_logged_in: Optional[bool] = None


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GqlUnion:
    """GQL Union operation: {'op': 'or', 'content': [operation1, operation2, ...]}"""

    op: Literal["or"] = "or"
    content: List["GqlOperation"] = field(default_factory=list)
    is_logged_in: Optional[bool] = None


# Union type for all GQL operations
GqlOperation = Union[
    GqlEquals,
    GqlNotEquals,
    GqlLessThan,
    GqlLessThanOrEquals,
    GqlGreaterThan,
    GqlGreaterThanOrEquals,
    GqlMissing,
    GqlExists,
    GqlIncludes,
    GqlExcludes,
    GqlExcludeIfAny,
    GqlIntersection,
    GqlUnion,
]


# Helper functions for type checking
def is_gql_equals(operation: Any) -> bool:
    """Check if operation is a GQL equals operation"""
    return isinstance(operation, dict) and operation.get("op") == "="


def is_gql_not_equals(operation: Any) -> bool:
    """Check if operation is a GQL not equals operation"""
    return isinstance(operation, dict) and operation.get("op") == "!="


def is_gql_less_than(operation: Any) -> bool:
    """Check if operation is a GQL less than operation"""
    return isinstance(operation, dict) and operation.get("op") == "<"


def is_gql_less_than_or_equals(operation: Any) -> bool:
    """Check if operation is a GQL less than or equals operation"""
    return isinstance(operation, dict) and operation.get("op") == "<="


def is_gql_greater_than(operation: Any) -> bool:
    """Check if operation is a GQL greater than operation"""
    return isinstance(operation, dict) and operation.get("op") == ">"


def is_gql_greater_than_or_equals(operation: Any) -> bool:
    """Check if operation is a GQL greater than or equals operation"""
    return isinstance(operation, dict) and operation.get("op") == ">="


def is_gql_missing(operation: Any) -> bool:
    """Check if operation is a GQL missing operation"""
    return isinstance(operation, dict) and operation.get("op") == "is"


def is_gql_exists(operation: Any) -> bool:
    """Check if operation is a GQL exists operation"""
    return isinstance(operation, dict) and operation.get("op") == "not"


def is_gql_includes(operation: Any) -> bool:
    """Check if operation is a GQL includes operation"""
    return isinstance(operation, dict) and operation.get("op") == "in"


def is_gql_excludes(operation: Any) -> bool:
    """Check if operation is a GQL excludes operation"""
    return isinstance(operation, dict) and operation.get("op") == "exclude"


def is_gql_exclude_if_any(operation: Any) -> bool:
    """Check if operation is a GQL exclude if any operation"""
    return isinstance(operation, dict) and operation.get("op") == "excludeifany"


def is_gql_intersection(operation: Any) -> bool:
    """Check if operation is a GQL intersection operation"""
    return isinstance(operation, dict) and operation.get("op") == "and"


def is_gql_union(operation: Any) -> bool:
    """Check if operation is a GQL union operation"""
    return isinstance(operation, dict) and operation.get("op") == "or"


def parse_gql_operation(data: Dict[str, Any]) -> Optional[GqlOperation]:
    """Parse a dictionary into the appropriate GqlOperation type"""
    if not isinstance(data, dict) or "op" not in data:
        return None

    op = data.get("op")

    if op == "=":
        return GqlEquals.from_dict(data)
    elif op == "!=":
        return GqlNotEquals.from_dict(data)
    elif op == "<":
        return GqlLessThan.from_dict(data)
    elif op == "<=":
        return GqlLessThanOrEquals.from_dict(data)
    elif op == ">":
        return GqlGreaterThan.from_dict(data)
    elif op == ">=":
        return GqlGreaterThanOrEquals.from_dict(data)
    elif op == "is":
        return GqlMissing.from_dict(data)
    elif op == "not":
        return GqlExists.from_dict(data)
    elif op == "in":
        return GqlIncludes.from_dict(data)
    elif op == "exclude":
        return GqlExcludes.from_dict(data)
    elif op == "excludeifany":
        return GqlExcludeIfAny.from_dict(data)
    elif op == "and":
        return GqlIntersection.from_dict(data)
    elif op == "or":
        return GqlUnion.from_dict(data)
    else:
        return None
