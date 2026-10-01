"""Full-text search use case over event metadata."""

from event_platform.application.errors import InvalidQueryError
from event_platform.application.ports import EventSearcher
from event_platform.application.queries import EventFilter, SearchResult

MAX_SEARCH_TEXT_LENGTH = 256


class EventSearchService:
    def __init__(self, searcher: EventSearcher) -> None:
        self._searcher = searcher

    async def search(self, text: str, event_filter: EventFilter, limit: int) -> SearchResult:
        text = text.strip()
        if not text:
            raise InvalidQueryError("search text must not be blank")
        if len(text) > MAX_SEARCH_TEXT_LENGTH:
            raise InvalidQueryError(f"search text must be at most {MAX_SEARCH_TEXT_LENGTH} chars")
        return await self._searcher.search(text, event_filter, limit)
