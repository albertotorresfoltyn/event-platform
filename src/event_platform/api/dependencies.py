"""FastAPI dependency providers resolving services from the app container."""

from typing import Annotated, cast

from fastapi import Depends, Request

from event_platform.application.ingestion import EventIngestionService
from event_platform.container import Container


def get_container(request: Request) -> Container:
    return cast(Container, request.app.state.container)


def get_ingestion_service(
    container: Annotated[Container, Depends(get_container)],
) -> EventIngestionService:
    return container.ingestion_service


IngestionServiceDep = Annotated[EventIngestionService, Depends(get_ingestion_service)]
