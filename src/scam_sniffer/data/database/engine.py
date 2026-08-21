"""PostgreSQL connection pooling and migration lifecycle."""

from __future__ import annotations

from dataclasses import dataclass

from pathlib import Path

import asyncio
import asyncpg

from scam_sniffer.data.database.errors import DatabaseError, DatabaseErrorReason
from scam_sniffer.data.database.schema.common import (
    MIGRATION_LOCK,
    MIGRATION_UNLOCK,
    MIGRATION_TABLE_CREATE,
    MIGRATION_VERSION_READ,
    MIGRATION_VERSION_CREATE,
)

_MIGRATION_PATH = Path(__file__).with_name("migration")

@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    """Configure the PostgreSQL connection pool.

    Attributes:
        dsn: PostgreSQL connection string.
        pool_min_size: Minimum number of open pool connections.
        pool_max_size: Maximum number of open pool connections.
        command_timeout: Default SQL command timeout in seconds.
    """

    dsn: str
    pool_min_size: int = 1
    pool_max_size: int = 10
    command_timeout: float = 30.0

    def __post_init__(self) -> None:
        """Validate the connection and pool settings.

        Raises:
            DatabaseError: If the DSN, pool sizes, or timeout is invalid.
        """
        if not self.dsn.strip():
            raise DatabaseError(
                reason=DatabaseErrorReason.CONF,
                message="Database DSN cannot be empty",
                operation="init",
            )
        if self.pool_min_size < 1 or self.pool_max_size < self.pool_min_size:
            raise DatabaseError(
                reason=DatabaseErrorReason.CONF,
                message="Database pool sizes are invalid",
                operation="init",
            )
        if self.command_timeout <= 0:
            raise DatabaseError(
                reason=DatabaseErrorReason.CONF,
                message="Database command timeout must be positive",
                operation="init",
            )

class DatabaseEngine:
    """Own a PostgreSQL connection pool and apply versioned migration."""

    def __init__(self, config: DatabaseConfig) -> None:
        """Initialize a disconnected database engine.

        Args:
            config: Validated PostgreSQL pool configuration.
        """
        self._pool: asyncpg.Pool | None = None
        self._config = config

    @property
    def pool(self) -> asyncpg.Pool:
        """Return the active PostgreSQL pool.

        Returns:
            Connected PostgreSQL pool.

        Raises:
            DatabaseError: If the engine is not connected.
        """
        if self._pool is None:
            raise DatabaseError(
                reason=DatabaseErrorReason.CONNECTION,
                message="Database engine is not connected",
                operation="pool",
            )
        return self._pool

    async def close(self) -> None:
        """Close the active PostgreSQL pool.

        Raises:
            DatabaseError: If pool shutdown fails.
        """
        if self._pool is None:
            return
        try:
            await self._pool.close()
            self._pool = None
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise DatabaseError(
                reason=DatabaseErrorReason.CONNECTION,
                message="Database pool shutdown failed",
                operation="close",
                root_cause=error,
            ) from error

    async def connect(self) -> None:
        """Create the PostgreSQL pool when it is not connected.

        Raises:
            DatabaseError: If a pool connection cannot be established.
        """
        if self._pool is not None:
            return
        try:
            self._pool = await asyncpg.create_pool(
                dsn=self._config.dsn,
                min_size=self._config.pool_min_size,
                max_size=self._config.pool_max_size,
                command_timeout=self._config.command_timeout,
            )
        except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise DatabaseError(
                reason=DatabaseErrorReason.CONNECTION,
                message="Database connection failed",
                operation="connect",
                root_cause=error,
            ) from error

    async def migrate(self) -> None:
        """Apply every pending SQL migration under an advisory lock.

        Raises:
            DatabaseError: If migration are missing or cannot be applied.
        """
        migrations = sorted(_MIGRATION_PATH.glob("*.sql"))
        if not migrations:
            raise DatabaseError(
                reason=DatabaseErrorReason.MIGRATION,
                message="Database migration are missing",
                operation="migrate",
            )

        try:
            async with self.pool.acquire() as connection:
                await connection.execute(MIGRATION_LOCK)
                try:
                    await connection.execute(MIGRATION_TABLE_CREATE)
                    for migration in migrations:
                        await _apply_migration(migration=migration, connection=connection)
                finally:
                    await connection.execute(MIGRATION_UNLOCK)
        except DatabaseError:
            raise
        except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as error:
            raise DatabaseError(
                reason=DatabaseErrorReason.MIGRATION,
                message="Database migration failed",
                operation="migrate",
                root_cause=error,
            ) from error

async def _apply_migration(
    migration: Path,
    connection: asyncpg.Connection,
) -> None:
    """Apply one unapplied SQL migration transactionally.

    Args:
        migration: SQL migration file whose stem is the version identifier.
        connection: Locked PostgreSQL connection used for the migration.
    """
    version = migration.stem
    is_applied = await connection.fetchval(MIGRATION_VERSION_READ, version)
    if is_applied:
        return

    statement = await asyncio.to_thread(migration.read_text, encoding="utf-8")
    async with connection.transaction():
        await connection.execute(statement)
        await connection.execute(MIGRATION_VERSION_CREATE, version)
