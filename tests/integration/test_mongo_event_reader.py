from datetime import UTC, datetime, timedelta

import pytest
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

from event_platform.application.queries import Cursor, EventFilter, TimeBucket
from event_platform.domain.events import Event
from event_platform.infrastructure.mongo.documents import Document
from event_platform.infrastructure.mongo.event_reader import MongoEventReader
from event_platform.infrastructure.mongo.event_repository import MongoEventRepository
from event_platform.infrastructure.mongo.indexes import EVENT_INDEXES, ensure_indexes
from event_platform.infrastructure.mongo.queries import NEWEST_FIRST, build_find_query
from tests.factories import make_event

pytestmark = pytest.mark.integration

# 2026-09-28 is a Monday, so weekly buckets start there.
MONDAY = datetime(2026, 9, 28, tzinfo=UTC)


@pytest.fixture
def collection(mongo_database: AsyncDatabase[Document]) -> AsyncCollection[Document]:
    return mongo_database["events"]


@pytest.fixture
def reader(collection: AsyncCollection[Document]) -> MongoEventReader:
    return MongoEventReader(collection)


async def seed(collection: AsyncCollection[Document], *events: Event) -> None:
    repository = MongoEventRepository(collection)
    for event in events:
        await repository.save(event)


async def test_find_applies_filters_newest_first(
    reader: MongoEventReader, collection: AsyncCollection[Document]
) -> None:
    old_click = make_event(event_type="click", user_id="u-1", timestamp=MONDAY)
    new_click = make_event(event_type="click", user_id="u-1", timestamp=MONDAY + timedelta(hours=1))
    other_user = make_event(event_type="click", user_id="u-2", timestamp=MONDAY)
    pageview = make_event(event_type="pageview", user_id="u-1", timestamp=MONDAY)
    await seed(collection, old_click, new_click, other_user, pageview)

    found = await reader.find(EventFilter(event_type="click", user_id="u-1"), 10, None)

    assert [e.event_id for e in found] == [new_click.event_id, old_click.event_id]


async def test_find_applies_half_open_date_range(
    reader: MongoEventReader, collection: AsyncCollection[Document]
) -> None:
    events = [make_event(timestamp=MONDAY + timedelta(hours=h)) for h in range(4)]
    await seed(collection, *events)

    found = await reader.find(
        EventFilter(start=MONDAY + timedelta(hours=1), end=MONDAY + timedelta(hours=3)), 10, None
    )

    assert {e.event_id for e in found} == {events[1].event_id, events[2].event_id}


async def test_keyset_pagination_visits_every_event_once_even_with_equal_timestamps(
    reader: MongoEventReader, collection: AsyncCollection[Document]
) -> None:
    events = [
        make_event(event_id=f"evt-{i}", timestamp=MONDAY + timedelta(hours=i // 3))
        for i in range(7)
    ]
    await seed(collection, *events)

    seen: list[str] = []
    cursor: Cursor | None = None
    while True:
        page = await reader.find(EventFilter(), 2, cursor)
        seen.extend(e.event_id for e in page)
        if len(page) < 2:
            break
        cursor = Cursor(page[-1].timestamp, page[-1].event_id)

    assert sorted(seen) == sorted(e.event_id for e in events)
    assert len(seen) == len(set(seen))


async def test_found_events_round_trip_with_utc_timestamps(
    reader: MongoEventReader, collection: AsyncCollection[Document]
) -> None:
    event = make_event(metadata={"browser": "safari", "nested": {"a": 1}})
    await seed(collection, event)

    [found] = await reader.find(EventFilter(), 10, None)

    assert found == event
    assert found.timestamp.tzinfo is not None


@pytest.mark.parametrize(
    ("bucket", "expected"),
    [
        (
            TimeBucket.HOUR,
            [
                (MONDAY, "click", 2),
                (MONDAY, "pageview", 1),
                (MONDAY + timedelta(hours=1), "click", 1),
                (MONDAY + timedelta(days=2, hours=5), "click", 1),
            ],
        ),
        (
            TimeBucket.DAY,
            [
                (MONDAY, "click", 3),
                (MONDAY, "pageview", 1),
                (MONDAY + timedelta(days=2), "click", 1),
            ],
        ),
        (TimeBucket.WEEK, [(MONDAY, "click", 4), (MONDAY, "pageview", 1)]),
    ],
)
async def test_count_by_bucket_groups_by_time_and_type(
    reader: MongoEventReader,
    collection: AsyncCollection[Document],
    bucket: TimeBucket,
    expected: list[tuple[datetime, str, int]],
) -> None:
    await seed(
        collection,
        make_event(event_type="click", timestamp=MONDAY + timedelta(minutes=5)),
        make_event(event_type="click", timestamp=MONDAY + timedelta(minutes=50)),
        make_event(event_type="pageview", timestamp=MONDAY + timedelta(minutes=10)),
        make_event(event_type="click", timestamp=MONDAY + timedelta(hours=1, minutes=1)),
        make_event(event_type="click", timestamp=MONDAY + timedelta(days=2, hours=5)),
        make_event(event_type="click", timestamp=MONDAY + timedelta(weeks=2)),  # out of range
    )

    counts = await reader.count_by_bucket(
        EventFilter(start=MONDAY, end=MONDAY + timedelta(days=7)), bucket
    )

    rows = [(c.bucket_start, c.event_type, c.count) for c in counts]
    assert rows == expected


async def test_ensure_indexes_is_idempotent(collection: AsyncCollection[Document]) -> None:
    await ensure_indexes(collection)
    await ensure_indexes(collection)

    names = {index["name"] async for index in await collection.list_indexes()}
    assert names == {"_id_"} | {model.document["name"] for model in EVENT_INDEXES}


@pytest.mark.parametrize(
    ("event_filter", "expected_index"),
    [
        (EventFilter(), "timestamp_id"),
        (EventFilter(start=MONDAY, end=MONDAY + timedelta(days=1)), "timestamp_id"),
        (EventFilter(event_type="click", start=MONDAY), "event_type_timestamp_id"),
        (EventFilter(user_id="u-1"), "user_id_timestamp_id"),
        (EventFilter(source_url="https://example.com/"), "source_url_timestamp_id"),
    ],
)
async def test_list_queries_use_an_index_without_in_memory_sort(
    collection: AsyncCollection[Document], event_filter: EventFilter, expected_index: str
) -> None:
    await ensure_indexes(collection)
    await seed(collection, make_event())

    plan = await (
        collection.find(build_find_query(event_filter, None)).sort(NEWEST_FIRST).limit(10).explain()
    )

    winning = str(plan["queryPlanner"]["winningPlan"])
    assert expected_index in winning
    assert "'stage': 'SORT'" not in winning
    assert "COLLSCAN" not in winning
