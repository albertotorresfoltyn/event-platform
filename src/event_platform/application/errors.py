"""Errors raised by application services and the ports they depend on."""


class ApplicationError(Exception):
    """Base class for use-case level failures."""


class QueueFullError(ApplicationError):
    """The ingestion queue reached its capacity; producers should back off and retry."""
