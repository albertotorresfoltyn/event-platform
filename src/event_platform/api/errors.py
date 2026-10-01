"""Translate domain/application exceptions into HTTP responses in a single place."""

import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from event_platform.application.errors import (
    DependencyUnavailableError,
    InvalidQueryError,
    QueueFullError,
)
from event_platform.domain.errors import InvalidEventError

logger = logging.getLogger(__name__)

QUEUE_FULL_RETRY_AFTER_SECONDS = 1
DEPENDENCY_RETRY_AFTER_SECONDS = 5


async def _unprocessable(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, content={"detail": str(exc)}
    )


async def _queue_full(_: Request, exc: Exception) -> JSONResponse:
    logger.warning("Rejecting request, ingestion queue is full: %s", exc)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Ingestion queue is at capacity, retry later"},
        headers={"Retry-After": str(QUEUE_FULL_RETRY_AFTER_SECONDS)},
    )


async def _dependency_unavailable(_: Request, exc: Exception) -> JSONResponse:
    logger.error("Dependency unavailable: %s", exc, exc_info=exc.__cause__)
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": f"{exc}, retry later"},
        headers={"Retry-After": str(DEPENDENCY_RETRY_AFTER_SECONDS)},
    )


async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled error on %s %s", request.method, request.url.path, exc_info=exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"},
    )


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(InvalidEventError, _unprocessable)
    app.add_exception_handler(InvalidQueryError, _unprocessable)
    app.add_exception_handler(QueueFullError, _queue_full)
    app.add_exception_handler(DependencyUnavailableError, _dependency_unavailable)
    app.add_exception_handler(Exception, _unhandled)
