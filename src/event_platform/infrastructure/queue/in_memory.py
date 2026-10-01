"""In-process queue modelled after Amazon SQS standard queues.

Semantics reproduced from SQS:

* **At-least-once delivery** - a received message stays in the queue, hidden for
  ``visibility_timeout`` seconds. If it is not acked in time (e.g. the worker
  crashed mid-batch) it becomes visible again and is redelivered.
* **Receipt handles** - each delivery gets a new handle; handles from earlier
  deliveries become stale and cannot ack the message.
* **Backoff** - ``retry_later`` is the equivalent of ``ChangeMessageVisibility``.
* **Redrive policy** - once a message has been received ``max_receive_count``
  times it is moved to the dead-letter queue instead of being delivered again.
* **Bounded capacity** - unlike SQS, memory is finite, so ``publish`` fails fast
  with ``QueueFullError`` to apply backpressure to producers.

Messages live in process memory: they are lost on restart. See ARCHITECTURE.md.
"""

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass

from event_platform.application.errors import QueueFullError
from event_platform.application.ports import QueueMessage
from event_platform.domain.events import Event

logger = logging.getLogger(__name__)

Clock = Callable[[], float]

_POLL_INTERVAL_SECONDS = 0.1


@dataclass(slots=True)
class _Envelope:
    event: Event
    visible_at: float
    receive_count: int = 0
    receipt_handle: str | None = None


class InMemoryEventQueue:
    def __init__(
        self,
        *,
        max_size: int,
        visibility_timeout_seconds: float,
        max_receive_count: int,
        clock: Clock = time.monotonic,
    ) -> None:
        self._max_size = max_size
        self._visibility_timeout = visibility_timeout_seconds
        self._max_receive_count = max_receive_count
        self._clock = clock
        self._messages: dict[str, _Envelope] = {}  # insertion-ordered: best-effort FIFO
        self._message_ids_by_receipt: dict[str, str] = {}
        self._dead_letters: list[Event] = []
        self._message_available = asyncio.Event()

    @property
    def depth(self) -> int:
        """Messages waiting or in flight (the SQS ``ApproximateNumberOfMessages*``)."""
        return len(self._messages)

    @property
    def dead_letters(self) -> tuple[Event, ...]:
        return tuple(self._dead_letters)

    async def publish(self, event: Event) -> None:
        if len(self._messages) >= self._max_size:
            raise QueueFullError(f"queue is at capacity ({self._max_size} messages)")
        self._messages[uuid.uuid4().hex] = _Envelope(event=event, visible_at=self._clock())
        self._message_available.set()

    async def receive(self, max_messages: int, wait_seconds: float) -> list[QueueMessage]:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_seconds
        while True:
            self._message_available.clear()
            batch = self._take_visible(max_messages)
            remaining = deadline - loop.time()
            if batch or remaining <= 0:
                return batch
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._message_available.wait(),
                    timeout=min(remaining, _POLL_INTERVAL_SECONDS),
                )

    async def ack(self, receipt_handle: str) -> None:
        message_id = self._message_ids_by_receipt.pop(receipt_handle, None)
        if message_id is None:
            logger.warning("Ignoring ack with stale or unknown receipt handle")
            return
        del self._messages[message_id]

    async def retry_later(self, receipt_handle: str, delay_seconds: float) -> None:
        message_id = self._message_ids_by_receipt.get(receipt_handle)
        if message_id is None:
            logger.warning("Ignoring retry with stale or unknown receipt handle")
            return
        self._messages[message_id].visible_at = self._clock() + delay_seconds

    def _take_visible(self, max_messages: int) -> list[QueueMessage]:
        now = self._clock()
        batch: list[QueueMessage] = []
        for message_id, envelope in list(self._messages.items()):
            if len(batch) >= max_messages:
                break
            if envelope.visible_at > now:
                continue
            if envelope.receive_count >= self._max_receive_count:
                self._move_to_dead_letters(message_id, envelope)
                continue
            batch.append(self._deliver(message_id, envelope, now))
        return batch

    def _deliver(self, message_id: str, envelope: _Envelope, now: float) -> QueueMessage:
        if envelope.receipt_handle is not None:
            self._message_ids_by_receipt.pop(envelope.receipt_handle, None)
        envelope.receipt_handle = uuid.uuid4().hex
        envelope.receive_count += 1
        envelope.visible_at = now + self._visibility_timeout
        self._message_ids_by_receipt[envelope.receipt_handle] = message_id
        return QueueMessage(
            receipt_handle=envelope.receipt_handle,
            event=envelope.event,
            receive_count=envelope.receive_count,
        )

    def _move_to_dead_letters(self, message_id: str, envelope: _Envelope) -> None:
        if envelope.receipt_handle is not None:
            self._message_ids_by_receipt.pop(envelope.receipt_handle, None)
        del self._messages[message_id]
        self._dead_letters.append(envelope.event)
        logger.error(
            "Event %s moved to dead-letter queue after %d receives",
            envelope.event.event_id,
            envelope.receive_count,
        )
