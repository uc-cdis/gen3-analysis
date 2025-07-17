from dataclasses import dataclass
from typing import List, Union, Dict, Any, Protocol, TypeVar, Literal

from dataclasses_json import dataclass_json, LetterCase


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class Intersection:
    operator: Literal["and"] = "and"
    operands: List["Operation"] = None

    def __post_init__(self):
        if self.operands is None:
            object.__setattr__(self, "operands", [])


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class UnionOr:
    operator: Literal["or"] = "or"
    operands: List["Operation"] = None

    def __post_init__(self):
        if self.operands is None:
            object.__setattr__(self, "operands", [])


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class Equals:
    operator: Literal["="] = "="
    field: str = ""
    operand: Union[int, str] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class NotEquals:
    operator: Literal["!="] = "!="
    field: str = ""
    operand: Union[int, str] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class Includes:
    operator: Literal["in", "includes"] = "in"
    field: str = ""
    operands: List[Union[str, int]] = None

    def __post_init__(self):
        if self.operands is None:
            object.__setattr__(self, "operands", [])


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class Comparison:
    field: str = ""
    operand: Union[str, int] = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class LessThan(Comparison):
    operator: Literal["<"] = "<"


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class LessThanOrEquals(Comparison):
    operator: Literal["<="] = "<="


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GreaterThan(Comparison):
    operator: Literal[">"] = ">"


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class GreaterThanOrEquals(Comparison):
    operator: Literal[">="] = ">="


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class Exists:
    operator: Literal["exists"] = "exists"
    field: str = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class Missing:
    operator: Literal["missing"] = "missing"
    field: str = ""


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class ExcludeIfAny:
    operator: Literal["excludeifany"] = "excludeifany"
    field: str = ""
    operands: List[Union[str, int]] = None

    def __post_init__(self):
        if self.operands is None:
            object.__setattr__(self, "operands", [])


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class Excludes:
    operator: Literal["excludes"] = "excludes"
    field: str = ""
    operands: List[Union[str, int]] = None

    def __post_init__(self):
        if self.operands is None:
            object.__setattr__(self, "operands", [])


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class NestedFilter:
    operator: Literal["nested"] = "nested"
    path: str = ""
    operand: "Operation" = None


# Union type for all operations
Operation = Union[
    Intersection,
    UnionOr,
    Includes,
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEquals,
    GreaterThan,
    GreaterThanOrEquals,
    NestedFilter,
    ExcludeIfAny,
    Excludes,
    Exists,
    Missing,
]

# Operations that have a field attribute
OperationWithField = Union[
    Includes,
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEquals,
    GreaterThan,
    GreaterThanOrEquals,
    ExcludeIfAny,
    Excludes,
]

# Operations that have operands
OperandsType = Union[Includes, Excludes, ExcludeIfAny, Intersection, UnionOr]


def is_union(value: Any) -> bool:
    """Check if the value is a UnionOr operation."""
    return value.operator == "or"


def is_intersection(value: Any) -> bool:
    return value.operator == "and"


def is_includes(value: Any) -> bool:
    return value.operator == "in"


def is_equals(value: Any) -> bool:
    return value.operator == "="


def is_notequals(value: Any) -> bool:
    return value.operator == "!="


def is_greaterThan(value: Any) -> bool:
    return value.operator == ">"


def is_greaterThanOrEquals(value: Any) -> bool:
    return (
        value.operator == ">="
        and isinstance(value.operand, Union[str, int])
        and isinstance(value.field, str)
    )


def is_lessThan(value: Any) -> bool:
    return (
        value.operator == "<"
        and isinstance(value.operand, Union[str, int])
        and isinstance(value.field, str)
    )


def is_lessThanOrEquals(value: Any) -> bool:
    return (
        value.operator == "<="
        and isinstance(value.operand, Union[str, int])
        and isinstance(value.field, str)
    )


def is_missing(value: Any) -> bool:
    """Check if the value is a Missing operation."""
    return value.operator == "missing" and isinstance(value.field, str)


def is_exists(value: Any) -> bool:
    """Check if the value is an Exists operation."""
    return value.operator == "exists" and isinstance(value.field, str)


def is_excludeifany(value: Any) -> bool:
    """Check if the value is an ExcludeIfAny operation."""
    return value.operator == "excludeifany"


def is_excludes(value: Any) -> bool:
    """Check if the value is an Excludes operation."""
    return value.operator == "excludes"


def is_nested(value: Any) -> bool:
    """Check if the value is a NestedFilter operation."""
    return value.operator == "nested" and isinstance(value.path, str)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass(frozen=True)
class FilterSet:
    root: Dict[str, Operation] = None
    mode: Literal["and", "or"] = "and"

    def __post_init__(self):
        if self.root is None:
            object.__setattr__(self, "root", {})


T = TypeVar("T")


class OperationHandler(Protocol[T]):
    def handle_equals(self, op: Equals) -> T: ...

    def handle_not_equals(self, op: NotEquals) -> T: ...

    def handle_less_than(self, op: LessThan) -> T: ...

    def handle_less_than_or_equals(self, op: LessThanOrEquals) -> T: ...

    def handle_greater_than(self, op: GreaterThan) -> T: ...

    def handle_greater_than_or_equals(self, op: GreaterThanOrEquals) -> T: ...

    def handle_includes(self, op: Includes) -> T: ...

    def handle_excludes(self, op: Excludes) -> T: ...

    def handle_exclude_if_any(self, op: ExcludeIfAny) -> T: ...

    def handle_intersection(self, op: Intersection) -> T: ...

    def handle_union(self, op: Union) -> T: ...

    def handle_nested_filter(self, op: NestedFilter) -> T: ...


# Helper functions
def is_filter_set(input_obj: Any) -> bool:
    """Check if the input is a FilterSet."""
    if not isinstance(input_obj, dict):
        return False

    root = input_obj.get("root")
    mode = input_obj.get("mode")

    if not isinstance(root, dict):
        return False

    if mode not in ["and", "or"]:
        return False

    return True


def is_operands_type(operation: Operation) -> bool:
    """Check if operation has operand attribute."""
    return hasattr(operation, "operands")
