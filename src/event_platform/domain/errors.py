"""Errors raised when domain invariants are violated."""


class DomainError(Exception):
    """Base class for every business-rule violation."""


class InvalidEventError(DomainError):
    """An event does not satisfy the platform's invariants."""
