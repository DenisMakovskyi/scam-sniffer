"""In-memory publisher and subscriber binding for domain events."""

from typing import cast, override

from scam_sniffer.domain.events.proto import EventPublisher, EventSubscriber

class MemoEventBus(EventPublisher[object]):
    """Deliver events to type-specific in-memory subscribers."""

    def __init__(self) -> None:
        """Initialize an event bus without registered subscribers."""
        self._subscribers: dict[type[object], list[EventSubscriber[object]]] = {}

    @override
    async def publish(self, event: object) -> None:
        """Deliver an event to a snapshot of its registered subscribers.

        Subscribers are called sequentially in registration order. Changes to
        subscriptions made during delivery apply only to later publications.

        Args:
            event: Event to deliver.

        Raises:
            Exception: If a subscriber cannot process the event.
        """
        subscribers = tuple(self._subscribers.get(type(event), ()))
        for subscriber in subscribers:
            await subscriber.on_event(event)

    def subscribe[TEvent](
        self,
        event_type: type[TEvent],
        subscriber: EventSubscriber[TEvent],
    ) -> bool:
        """Register a subscriber for one exact event type.

        Args:
            event_type: Concrete event class routed to the subscriber.
            subscriber: Typed event consumer to register.

        Returns:
            Whether the subscriber was added. Repeated registration returns false.
        """
        subscribers = self._subscribers.setdefault(event_type, [])
        stored_subscriber = cast(EventSubscriber[object], subscriber)
        if any(current is stored_subscriber for current in subscribers):
            return False
        subscribers.append(stored_subscriber)
        return True

    def unsubscribe[TEvent](
        self,
        event_type: type[TEvent],
        subscriber: EventSubscriber[TEvent],
    ) -> bool:
        """Remove a subscriber from one exact event type.

        Args:
            event_type: Concrete event class routed to the subscriber.
            subscriber: Typed event consumer to remove.

        Returns:
            Whether a matching registration was removed.
        """
        subscribers = self._subscribers.get(event_type)
        if subscribers is None:
            return False

        stored_subscriber = cast(EventSubscriber[object], subscriber)
        for index, current in enumerate(subscribers):
            if current is not stored_subscriber:
                continue
            subscribers.pop(index)
            if not subscribers:
                self._subscribers.pop(event_type)
            return True
        return False
