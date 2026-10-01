import asyncio

import pytest

from event_platform.application.backoff import ExponentialBackoff
from event_platform.application.ports import QueueMessage
from event_platform.application.processing import EventProcessor
from event_platform.application.worker import EventWorker
from event_platform.domain.events import Event
from event_platform.infrastructure.queue.in_memory import InMemoryEventQueue
from tests.factories import make_event
from tests.fakes import FakeEventIndexer, FakeEventRepository, RecordingConsumer
from tests.polling import wait_until

MAX_RECEIVE_COUNT = 3


@pytest.fixture
def queue() -> InMemoryEventQueue:
    return InMemoryEventQueue(
        max_size=100, visibility_timeout_seconds=30, max_receive_count=MAX_RECEIVE_COUNT
    )


def make_worker(queue: InMemoryEventQueue, repository: FakeEventRepository) -> EventWorker:
    no_delay = ExponentialBackoff(base_seconds=1e-9, max_seconds=1e-9)
    processor = EventProcessor(
        repository=repository, indexer=FakeEventIndexer(), consumer=queue, backoff=no_delay
    )
    return EventWorker(queue, processor, batch_size=10, poll_wait_seconds=0)


async def drain(worker: EventWorker, iterations: int) -> None:
    for _ in range(iterations):
        await worker.run_once()
        await asyncio.sleep(0)


async def test_worker_persists_and_acks_a_batch(queue: InMemoryEventQueue) -> None:
    repository = FakeEventRepository()
    events = [make_event() for _ in range(3)]
    for event in events:
        await queue.publish(event)

    handled = await make_worker(queue, repository).run_once()

    assert handled == 3
    assert set(repository.events) == {e.event_id for e in events}
    assert queue.depth == 0


async def test_transient_failures_are_retried_until_success(queue: InMemoryEventQueue) -> None:
    repository = FakeEventRepository(failures=MAX_RECEIVE_COUNT - 1)
    event = make_event()
    await queue.publish(event)

    await drain(make_worker(queue, repository), iterations=MAX_RECEIVE_COUNT)

    assert event.event_id in repository.events
    assert queue.dead_letters == ()


async def test_event_exhausting_retries_lands_in_dead_letter_queue(
    queue: InMemoryEventQueue,
) -> None:
    repository = FakeEventRepository(failures=100)
    event = make_event()
    await queue.publish(event)

    await drain(make_worker(queue, repository), iterations=MAX_RECEIVE_COUNT + 1)

    assert repository.events == {}
    assert queue.dead_letters == (event,)


async def test_started_worker_consumes_in_background_and_stops_cleanly(
    queue: InMemoryEventQueue,
) -> None:
    repository = FakeEventRepository()
    worker = EventWorker(
        queue,
        EventProcessor(
            repository,
            FakeEventIndexer(),
            queue,
            ExponentialBackoff(base_seconds=1, max_seconds=1),
        ),
        batch_size=10,
        poll_wait_seconds=0.05,
    )
    worker.start()
    await queue.publish(make_event())

    for _ in range(50):
        if repository.events:
            break
        await asyncio.sleep(0.01)
    await worker.stop()

    assert len(repository.events) == 1


async def test_stop_without_start_is_a_no_op(queue: InMemoryEventQueue) -> None:
    await make_worker(queue, FakeEventRepository()).stop()


class FlakyConsumer(RecordingConsumer):
    """Raises on the first receive, then returns nothing."""

    def __init__(self) -> None:
        super().__init__()
        self.receive_calls = 0

    async def receive(self, max_messages: int, wait_seconds: float) -> list[QueueMessage]:
        self.receive_calls += 1
        if self.receive_calls == 1:
            raise RuntimeError("transient receive failure")
        await asyncio.sleep(0.01)
        return []


async def test_worker_keeps_running_after_an_iteration_fails() -> None:
    consumer = FlakyConsumer()
    processor = EventProcessor(
        FakeEventRepository(), FakeEventIndexer(), consumer, ExponentialBackoff(1, 1)
    )
    worker = EventWorker(
        consumer, processor, batch_size=1, poll_wait_seconds=0, error_pause_seconds=0
    )

    worker.start()
    await wait_until(lambda: _is_at_least(consumer.receive_calls, 2))
    await worker.stop()


async def test_stop_cancels_a_worker_stuck_past_the_timeout(queue: InMemoryEventQueue) -> None:
    class HangingRepository(FakeEventRepository):
        async def save(self, event: Event) -> bool:
            await asyncio.sleep(3600)
            return True

    worker = make_worker(queue, HangingRepository())
    await queue.publish(make_event())
    worker.start()
    await asyncio.sleep(0.01)

    await asyncio.wait_for(worker.stop(timeout_seconds=0.05), timeout=1)


async def _is_at_least(value: int, minimum: int) -> bool:
    return value >= minimum
