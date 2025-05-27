class AppException(Exception):
    """Base application exception."""
    pass


class ValidationError(AppException):
    """Validation error exception."""
    pass


class NotFoundError(AppException):
    """Resource not found exception."""
    pass


class DatabaseError(AppException):
    """Database operation error."""
    pass