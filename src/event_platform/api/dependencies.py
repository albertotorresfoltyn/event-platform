"""FastAPI dependency providers resolving services from the app container."""

import secrets
from datetime import datetime
from typing import Annotated, cast

from fastapi import Depends, Header, HTTPException, Query, Request, status

from event_platform.application.health import ReadinessService
from event_platform.application.ingestion import EventIngestionService
from event_platform.application.ports import DeadLetterQueue
from event_platform.application.queries import EventFilter
from event_platform.application.querying import EventQueryService
from event_platform.application.realtime_stats import RealtimeStatsService
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


def get_realtime_stats_service(
    container: Annotated[Container, Depends(get_container)],
) -> RealtimeStatsService:
    return container.realtime_stats_service


def get_readiness_service(
    container: Annotated[Container, Depends(get_container)],
) -> ReadinessService:
    return container.readiness_service


def get_dead_letter_queue(
    container: Annotated[Container, Depends(get_container)],
) -> DeadLetterQueue:
    return container.queue


def require_admin(
    container: Annotated[Container, Depends(get_container)],
    x_admin_key: Annotated[str | None, Header()] = None,
) -> None:
    configured = container.settings.admin_api_key
    if configured is None:
        # Hide the admin surface entirely when no key is configured.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    if x_admin_key is None or not secrets.compare_digest(
        x_admin_key, configured.get_secret_value()
    ):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid admin key")


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
RealtimeStatsServiceDep = Annotated[RealtimeStatsService, Depends(get_realtime_stats_service)]
ReadinessServiceDep = Annotated[ReadinessService, Depends(get_readiness_service)]
DeadLetterQueueDep = Annotated[DeadLetterQueue, Depends(get_dead_letter_queue)]
