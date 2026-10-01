"""Elasticsearch adapter implementing the EventIndexer and EventSearcher ports."""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from elasticsearch import AsyncElasticsearch, BadRequestError, NotFoundError
from elasticsearch import ConnectionError as EsConnectionError

from event_platform.application.errors import DependencyUnavailableError
from event_platform.application.queries import EventFilter, SearchHit, SearchResult
from event_platform.domain.events import Event
from event_platform.infrastructure.elasticsearch.documents import from_source, to_document
from event_platform.infrastructure.elasticsearch.mapping import MAPPINGS, index_settings
from event_platform.infrastructure.elasticsearch.queries import (
    BEST_MATCH_THEN_NEWEST,
    build_search_query,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _translate_errors() -> AsyncIterator[None]:
    try:
        yield
    except EsConnectionError as exc:  # also covers ConnectionTimeout
        raise DependencyUnavailableError("Elasticsearch") from exc


class ElasticsearchEventIndex:
    def __init__(
        self,
        client: AsyncElasticsearch,
        index_name: str,
        *,
        number_of_shards: int = 1,
        number_of_replicas: int = 0,
    ) -> None:
        self._client = client
        self._index_name = index_name
        self._settings = index_settings(number_of_shards, number_of_replicas)
        self._index_ready = False

    async def ensure_index(self) -> None:
        """Create the index with the explicit mapping if it does not exist yet.

        Called at startup and lazily before the first write, so an event can never
        reach Elasticsearch first and trigger an auto-created, dynamically-mapped index.
        """
        if self._index_ready:
            return
        async with _translate_errors():
            if not await self._client.indices.exists(index=self._index_name):
                await self._create_index()
        self._index_ready = True

    async def _create_index(self) -> None:
        try:
            await self._client.indices.create(
                index=self._index_name, settings=self._settings, mappings=MAPPINGS
            )
            logger.info("Created Elasticsearch index %s", self._index_name)
        except BadRequestError as exc:  # another worker created it concurrently
            if exc.error != "resource_already_exists_exception":
                raise

    async def index(self, event: Event) -> None:
        await self.ensure_index()
        async with _translate_errors():
            # Explicit id makes re-indexing a redelivered event an overwrite, not a duplicate.
            await self._client.index(
                index=self._index_name, id=event.event_id, document=to_document(event)
            )

    async def search(self, text: str, event_filter: EventFilter, limit: int) -> SearchResult:
        async with _translate_errors():
            try:
                response = await self._client.search(
                    index=self._index_name,
                    query=build_search_query(text, event_filter),
                    sort=BEST_MATCH_THEN_NEWEST,
                    size=limit,
                    track_scores=True,
                )
            except NotFoundError:  # nothing has been indexed yet
                return SearchResult(total=0, hits=[])
        hits = response["hits"]
        return SearchResult(
            total=hits["total"]["value"],
            hits=[
                SearchHit(event=from_source(hit["_source"]), score=hit["_score"] or 0.0)
                for hit in hits["hits"]
            ],
        )
