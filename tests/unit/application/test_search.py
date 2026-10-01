import pytest

from event_platform.application.errors import InvalidQueryError
from event_platform.application.queries import EventFilter
from event_platform.application.search import MAX_SEARCH_TEXT_LENGTH, EventSearchService
from tests.fakes import StubEventSearcher


async def test_search_trims_text_and_delegates() -> None:
    searcher = StubEventSearcher()
    event_filter = EventFilter(user_id="u-1")

    await EventSearchService(searcher).search("  firefox  ", event_filter, limit=5)

    assert searcher.calls == [("firefox", event_filter, 5)]


@pytest.mark.parametrize("text", ["   ", "x" * (MAX_SEARCH_TEXT_LENGTH + 1)])
async def test_search_rejects_blank_or_oversized_text(text: str) -> None:
    with pytest.raises(InvalidQueryError):
        await EventSearchService(StubEventSearcher()).search(text, EventFilter(), limit=5)
