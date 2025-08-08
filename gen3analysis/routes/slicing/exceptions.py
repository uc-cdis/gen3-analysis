class BaseUserError(Exception):
    pass


class BasePermissionError(Exception):
    pass


class BaseLookupError(Exception):
    pass


class BaseRetrievalError(Exception):
    pass


class BaseSlicingError(Exception):
    pass


class CoordinateParsingError(BaseUserError):
    pass


class CoordinateRangeError(BaseUserError):
    pass


class TruncatedError(BaseSlicingError):
    pass


class FormatError(BaseSlicingError):
    pass


class NotFoundError(BaseRetrievalError):
    pass
