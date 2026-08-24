"""Application use case contracts."""

from typing import Protocol

class UseCase(Protocol):
    """Execute one long-running application workflow."""

    async def execute(self) -> None:
        """Run the workflow until cancellation or a critical failure.

        Raises:
            ScamError: If the workflow cannot continue.
        """
        ...
