from dataclasses import dataclass

import pytest

from scam_sniffer.core.events import MemoEventBus
from scam_sniffer.core.events import EventPublisher

@dataclass(frozen=True, slots=True)
class FirstEvent:
    value: int

@dataclass(frozen=True, slots=True)
class SecondEvent:
    value: str

class FakeEventSubscriber[TEvent]:
    def __init__(
        self,
        name: str,
        calls: list[str],
        error: Exception | None = None,
    ) -> None:
        self.name = name
        self.calls = calls
        self.error = error
        self.events: list[TEvent] = []

    async def on_event(self, event: TEvent) -> None:
        self.calls.append(self.name)
        if self.error is not None:
            raise self.error
        self.events.append(event)

@pytest.mark.asyncio
async def test_event_bus_routes_events_by_exact_type() -> None:
    calls: list[str] = []
    event_bus = MemoEventBus()
    first_subscriber = FakeEventSubscriber[FirstEvent](name="first", calls=calls)
    second_subscriber = FakeEventSubscriber[SecondEvent](name="second", calls=calls)

    event_bus.subscribe(event_type=FirstEvent, subscriber=first_subscriber)
    event_bus.subscribe(event_type=SecondEvent, subscriber=second_subscriber)
    publisher: EventPublisher[FirstEvent] = event_bus
    event = FirstEvent(value=1)

    await publisher.publish(event)

    assert calls == ["first"]
    assert first_subscriber.events == [event]
    assert second_subscriber.events == []

@pytest.mark.asyncio
async def test_event_bus_preserves_subscription_order_and_ignores_duplicates() -> None:
    calls: list[str] = []
    event_bus = MemoEventBus()
    first_subscriber = FakeEventSubscriber[FirstEvent](name="first", calls=calls)
    second_subscriber = FakeEventSubscriber[FirstEvent](name="second", calls=calls)

    is_first_added = event_bus.subscribe(
        event_type=FirstEvent,
        subscriber=first_subscriber,
    )
    is_duplicate_added = event_bus.subscribe(
        event_type=FirstEvent,
        subscriber=first_subscriber,
    )
    is_second_added = event_bus.subscribe(
        event_type=FirstEvent,
        subscriber=second_subscriber,
    )

    await event_bus.publish(FirstEvent(value=1))

    assert is_first_added
    assert is_second_added
    assert not is_duplicate_added
    assert calls == ["first", "second"]

@pytest.mark.asyncio
async def test_event_bus_unsubscribes_registered_consumer() -> None:
    calls: list[str] = []
    event_bus = MemoEventBus()
    subscriber = FakeEventSubscriber[FirstEvent](name="first", calls=calls)

    event_bus.subscribe(event_type=FirstEvent, subscriber=subscriber)
    is_removed = event_bus.unsubscribe(
        event_type=FirstEvent,
        subscriber=subscriber,
    )
    is_removed_again = event_bus.unsubscribe(
        event_type=FirstEvent,
        subscriber=subscriber,
    )

    await event_bus.publish(FirstEvent(value=1))

    assert is_removed
    assert not is_removed_again
    assert calls == []

@pytest.mark.asyncio
async def test_event_bus_propagates_subscriber_failure() -> None:
    calls: list[str] = []
    subscriber_error = RuntimeError("Subscriber failed")
    event_bus = MemoEventBus()
    failing_subscriber = FakeEventSubscriber[FirstEvent](
        name="failing",
        calls=calls,
        error=subscriber_error,
    )
    pending_subscriber = FakeEventSubscriber[FirstEvent](name="pending", calls=calls)

    event_bus.subscribe(event_type=FirstEvent, subscriber=failing_subscriber)
    event_bus.subscribe(event_type=FirstEvent, subscriber=pending_subscriber)

    with pytest.raises(RuntimeError) as error_info:
        await event_bus.publish(FirstEvent(value=1))

    assert error_info.value is subscriber_error
    assert calls == ["failing"]
    assert pending_subscriber.events == []
