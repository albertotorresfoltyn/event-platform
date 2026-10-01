import pytest

from event_platform.application.backoff import ExponentialBackoff
from event_platform.application.ports import QueueMessage
from event_platform.application.processing import EventProcessor
from tests.factories import make_event
from tests.fakes import FakeEventIndexer, FakeEventRepository, RecordingConsumer


def make_processor(
    repository: FakeEventRepository,
    consumer: RecordingConsumer,
    indexer: FakeEventIndexer | None = None,
) -> EventProcessor:
    backoff = ExponentialBackoff(base_seconds=1, max_seconds=60, random=lambda: 1.0)
    return EventProcessor(
        repository=repository,
        indexer=indexer or FakeEventIndexer(),
        consumer=consumer,
        backoff=backoff,
    )


def make_message(receive_count: int = 1) -> QueueMessage:
    return QueueMessage(receipt_handle="r-1", event=make_event(), receive_count=receive_count)


async def test_event_is_persisted_indexed_and_acked() -> None:
    repository, indexer, consumer = FakeEventRepository(), FakeEventIndexer(), RecordingConsumer()
    message = make_message()

    await make_processor(repository, consumer, indexer).process(message)

    assert message.event.event_id in repository.events
    assert message.event.event_id in indexer.documents
    assert consumer.acked == ["r-1"]
    assert consumer.retried == []


async def test_duplicate_event_is_reindexed_and_acked_without_second_write() -> None:
    repository, indexer, consumer = FakeEventRepository(), FakeEventIndexer(), RecordingConsumer()
    message = make_message()
    await repository.save(message.event)

    await make_processor(repository, consumer, indexer).process(message)

    assert len(repository.events) == 1
    assert message.event.event_id in indexer.documents
    assert consumer.acked == ["r-1"]


@pytest.mark.parametrize(("receive_count", "expected_delay"), [(1, 1.0), (3, 4.0)])
async def test_failed_save_schedules_retry_with_backoff_and_skips_indexing(
    receive_count: int, expected_delay: float
) -> None:
    repository, indexer, consumer = (
        FakeEventRepository(failures=1),
        FakeEventIndexer(),
        RecordingConsumer(),
    )

    await make_processor(repository, consumer, indexer).process(make_message(receive_count))

    assert indexer.index_calls == 0
    assert consumer.acked == []
    assert consumer.retried == [("r-1", expected_delay)]


async def test_index_failure_after_save_is_retried_and_converges() -> None:
    repository, indexer, consumer = (
        FakeEventRepository(),
        FakeEventIndexer(failures=1),
        RecordingConsumer(),
    )
    processor = make_processor(repository, consumer, indexer)
    message = make_message()

    await processor.process(message)
    assert consumer.retried == [("r-1", 1.0)]
    assert message.event.event_id in repository.events

    redelivered = QueueMessage(receipt_handle="r-2", event=message.event, receive_count=2)
    await processor.process(redelivered)

    assert len(repository.events) == 1
    assert message.event.event_id in indexer.documents
    assert consumer.acked == ["r-2"]
