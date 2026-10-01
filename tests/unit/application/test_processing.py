import pytest

from event_platform.application.backoff import ExponentialBackoff
from event_platform.application.ports import QueueMessage
from event_platform.application.processing import EventProcessor
from tests.factories import make_event
from tests.fakes import FakeEventRepository, RecordingConsumer


def make_processor(repository: FakeEventRepository, consumer: RecordingConsumer) -> EventProcessor:
    backoff = ExponentialBackoff(base_seconds=1, max_seconds=60, random=lambda: 1.0)
    return EventProcessor(repository=repository, consumer=consumer, backoff=backoff)


def make_message(receive_count: int = 1) -> QueueMessage:
    return QueueMessage(receipt_handle="r-1", event=make_event(), receive_count=receive_count)


async def test_persisted_event_is_acked() -> None:
    repository, consumer = FakeEventRepository(), RecordingConsumer()
    message = make_message()

    await make_processor(repository, consumer).process(message)

    assert message.event.event_id in repository.events
    assert consumer.acked == ["r-1"]
    assert consumer.retried == []


async def test_duplicate_event_is_acked_without_second_write() -> None:
    repository, consumer = FakeEventRepository(), RecordingConsumer()
    message = make_message()
    await repository.save(message.event)

    await make_processor(repository, consumer).process(message)

    assert len(repository.events) == 1
    assert consumer.acked == ["r-1"]


@pytest.mark.parametrize(("receive_count", "expected_delay"), [(1, 1.0), (3, 4.0)])
async def test_failed_save_schedules_retry_with_backoff(
    receive_count: int, expected_delay: float
) -> None:
    repository, consumer = FakeEventRepository(failures=1), RecordingConsumer()

    await make_processor(repository, consumer).process(make_message(receive_count))

    assert consumer.acked == []
    assert consumer.retried == [("r-1", expected_delay)]
