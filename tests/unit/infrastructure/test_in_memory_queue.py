import asyncio

import pytest

from event_platform.application.errors import QueueFullError
from event_platform.application.ports import QueueMessage
from event_platform.infrastructure.queue.in_memory import InMemoryEventQueue
from tests.factories import make_event

VISIBILITY_TIMEOUT = 30.0
MAX_RECEIVE_COUNT = 3


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def queue(clock: FakeClock) -> InMemoryEventQueue:
    return InMemoryEventQueue(
        max_size=3,
        visibility_timeout_seconds=VISIBILITY_TIMEOUT,
        max_receive_count=MAX_RECEIVE_COUNT,
        clock=clock,
    )


async def receive_now(queue: InMemoryEventQueue, max_messages: int = 10) -> list[QueueMessage]:
    return await queue.receive(max_messages=max_messages, wait_seconds=0)


async def test_published_event_is_received(queue: InMemoryEventQueue) -> None:
    event = make_event()
    await queue.publish(event)

    [message] = await receive_now(queue)

    assert message.event == event
    assert message.receive_count == 1


async def test_receive_respects_max_messages_and_fifo_order(queue: InMemoryEventQueue) -> None:
    events = [make_event() for _ in range(3)]
    for event in events:
        await queue.publish(event)

    batch = await receive_now(queue, max_messages=2)

    assert [m.event for m in batch] == events[:2]


async def test_in_flight_message_is_hidden_until_visibility_timeout(
    queue: InMemoryEventQueue, clock: FakeClock
) -> None:
    await queue.publish(make_event())
    await receive_now(queue)

    assert await receive_now(queue) == []

    clock.advance(VISIBILITY_TIMEOUT)
    [redelivered] = await receive_now(queue)
    assert redelivered.receive_count == 2


async def test_ack_removes_message(queue: InMemoryEventQueue, clock: FakeClock) -> None:
    await queue.publish(make_event())
    [message] = await receive_now(queue)

    await queue.ack(message.receipt_handle)
    clock.advance(VISIBILITY_TIMEOUT)

    assert await receive_now(queue) == []
    assert queue.depth == 0


async def test_stale_receipt_handle_cannot_ack(queue: InMemoryEventQueue, clock: FakeClock) -> None:
    await queue.publish(make_event())
    [first] = await receive_now(queue)
    clock.advance(VISIBILITY_TIMEOUT)
    await receive_now(queue)

    await queue.ack(first.receipt_handle)

    assert queue.depth == 1


async def test_stale_receipt_handle_cannot_change_visibility(
    queue: InMemoryEventQueue, clock: FakeClock
) -> None:
    await queue.publish(make_event())
    [first] = await receive_now(queue)
    clock.advance(VISIBILITY_TIMEOUT)
    await receive_now(queue)

    await queue.retry_later(first.receipt_handle, delay_seconds=0)

    assert await receive_now(queue) == []


async def test_retry_later_delays_redelivery(queue: InMemoryEventQueue, clock: FakeClock) -> None:
    await queue.publish(make_event())
    [message] = await receive_now(queue)

    await queue.retry_later(message.receipt_handle, delay_seconds=2)

    clock.advance(1.9)
    assert await receive_now(queue) == []
    clock.advance(0.1)
    assert len(await receive_now(queue)) == 1


async def test_message_moves_to_dead_letters_after_max_receives(
    queue: InMemoryEventQueue, clock: FakeClock
) -> None:
    event = make_event()
    await queue.publish(event)
    for _ in range(MAX_RECEIVE_COUNT):
        [message] = await receive_now(queue)
        await queue.retry_later(message.receipt_handle, delay_seconds=0)

    assert await receive_now(queue) == []
    assert queue.dead_letters == (event,)
    assert queue.depth == 0


async def test_publish_fails_when_queue_is_full(queue: InMemoryEventQueue) -> None:
    for _ in range(3):
        await queue.publish(make_event())

    with pytest.raises(QueueFullError):
        await queue.publish(make_event())


async def test_long_poll_wakes_up_when_event_is_published() -> None:
    queue = InMemoryEventQueue(max_size=10, visibility_timeout_seconds=30, max_receive_count=3)
    event = make_event()

    receiving = asyncio.create_task(queue.receive(max_messages=1, wait_seconds=5))
    await asyncio.sleep(0)
    await queue.publish(event)
    batch = await asyncio.wait_for(receiving, timeout=1)

    assert [m.event for m in batch] == [event]


async def test_long_poll_returns_empty_after_wait_expires() -> None:
    queue = InMemoryEventQueue(max_size=10, visibility_timeout_seconds=30, max_receive_count=3)

    assert await queue.receive(max_messages=1, wait_seconds=0.05) == []
