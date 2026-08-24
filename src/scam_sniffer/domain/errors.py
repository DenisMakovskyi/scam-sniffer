"""Failures exposed by domain workflows."""

from __future__ import annotations

from enum import StrEnum

from scam_sniffer.errors import AppError

class DomainError(AppError):
    """Represent a repository failure at the domain boundary."""

    def __init__(
        self,
        reason: DomainErrorReason,
        message: str,
        operation: str,
        root_cause: Exception | None = None,
    ) -> None:
        """Initialize a domain failure.

        Args:
            reason: Domain failure category.
            message: Human-readable failure description.
            operation: Repository operation active during the failure.
            root_cause: Lower-level data-source failure, if available.
        """
        super().__init__(
            reason=reason,
            message=message,
            operation=operation,
            root_cause=root_cause,
        )

class ManagerError(AppError):
    """Represent a manager configuration or workflow failure."""

    def __init__(
        self,
        reason: ManagerErrorReason,
        message: str,
        operation: str,
        root_cause: Exception | None = None,
    ) -> None:
        """Initialize a manager failure.

        Args:
            reason: Manager failure category.
            message: Human-readable failure description.
            operation: Manager operation active during the failure.
            root_cause: Lower-level domain failure, if available.
        """
        super().__init__(
            reason=reason,
            message=message,
            operation=operation,
            root_cause=root_cause,
        )

class DomainErrorReason(StrEnum):
    """Categorize failures crossing the domain repository boundary."""

    REMOTE = "remote"
    STORAGE = "storage"
    MAPPING = "mapping"

class ManagerErrorReason(StrEnum):
    """Categorize failures produced by application managers."""

    CONF = "conf"
    PARAMS = "params"
    LIFECYCLE = "lifecycle"
    PUBLISHER = "publisher"
    CONTINUITY = "continuity"
    REPOSITORY = "repository"
