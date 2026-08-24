"""Typed event publisher and subscriber contracts for domain workflows."""

from typing import Protocol

class EventPublisher[TEvent](Protocol):
    """Publish typed domain events to downstream consumers."""

    async def publish(self, event: TEvent) -> None:
        """Publish one event after its related persistence succeeds.

        Args:
            event: Typed domain event ready for downstream processing.

        Raises:
            Exception: If a downstream subscriber cannot process the event.
        """
        ...

class EventSubscriber[TEvent](Protocol):
    """Consume typed domain events produced by upstream workflows."""

    async def on_event(self, event: TEvent) -> None:
        """Process one published domain event.

        Args:
            event: Typed domain event ready for processing.

        Raises:
            Exception: If the event cannot be processed.
        """
        ...
