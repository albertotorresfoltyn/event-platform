"""FastAPI dependency providers resolving services from the app container."""

from datetime import datetime
from typing import Annotated, cast

from fastapi import Depends, Query, Request

from event_platform.application.ingestion import EventIngestionService
from event_platform.application.queries import EventFilter
from event_platform.application.querying import EventQueryService
from event_platform.application.search import EventSearchService
from event_platform.container import Container


def get_container(request: Request) -> Container:
    return cast(Container, request.app.state.container)


def get_ingestion_service(
    container: Annotated[Container, Depends(get_container)],
) -> EventIngestionService:
    return container.ingestion_service


def get_query_service(
    container: Annotated[Container, Depends(get_container)],
) -> EventQueryService:
    return container.query_service


def get_search_service(
    container: Annotated[Container, Depends(get_container)],
) -> EventSearchService:
    return container.search_service


def get_event_filter(
    event_type: Annotated[str | None, Query(description="Exact event type")] = None,
    user_id: Annotated[str | None, Query(description="Exact user id")] = None,
    source_url: Annotated[str | None, Query(description="Exact source URL")] = None,
    start: Annotated[
        datetime | None, Query(description="Inclusive lower bound (ISO 8601, UTC if no offset)")
    ] = None,
    end: Annotated[
        datetime | None, Query(description="Exclusive upper bound (ISO 8601, UTC if no offset)")
    ] = None,
) -> EventFilter:
    return EventFilter(
        event_type=event_type, user_id=user_id, source_url=source_url, start=start, end=end
    )


IngestionServiceDep = Annotated[EventIngestionService, Depends(get_ingestion_service)]
QueryServiceDep = Annotated[EventQueryService, Depends(get_query_service)]
EventFilterDep = Annotated[EventFilter, Depends(get_event_filter)]
SearchServiceDep = Annotated[EventSearchService, Depends(get_search_service)]
