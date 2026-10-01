"""Errors raised by application services and the ports they depend on."""


class ApplicationError(Exception):
    """Base class for use-case level failures."""


class QueueFullError(ApplicationError):
    """The ingestion queue reached its capacity; producers should back off and retry."""


class InvalidQueryError(ApplicationError):
    """A read request has inconsistent or out-of-range parameters."""


class DependencyUnavailableError(ApplicationError):
    """A backing service (MongoDB, Elasticsearch, Redis) cannot be reached."""

    def __init__(self, dependency: str) -> None:
        super().__init__(f"{dependency} is unavailable")
        self.dependency = dependency
