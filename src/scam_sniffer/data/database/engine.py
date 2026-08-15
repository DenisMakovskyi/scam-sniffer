from __future__ import annotations

from dataclasses import dataclass

from pathlib import Path

import asyncio
import asyncpg

from scam_sniffer.data.database.errors import DatabaseError, DatabaseErrorReason
from scam_sniffer.data.database.schema_cmn import (
    MIGRATION_LOCK,
    MIGRATION_UNLOCK,
    MIGRATION_TABLE_CREATE,
    MIGRATION_VERSION_READ,
    MIGRATION_VERSION_CREATE,
)

_MIGRATION_PATH = Path(__file__).with_name("migrations")

@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    dsn: str
    pool_min_size: int = 1
    pool_max_size: int = 10
    command_timeout: float = 30.0

    def __post_init__(self) -> None:
        if not self.dsn.strip():
            raise DatabaseError(
                reason=DatabaseErrorReason.INVALID_CONFIG,
                message="Database DSN cannot be empty",
                operation="init",
            )
        if self.pool_min_size < 1 or self.pool_max_size < self.pool_min_size:
            raise DatabaseError(
                reason=DatabaseErrorReason.INVALID_CONFIG,
                message="Database pool sizes are invalid",
                operation="init",
            )
        if self.command_timeout <= 0:
            raise DatabaseError(
                reason=DatabaseErrorReason.INVALID_CONFIG,
                message="Database command timeout must be positive",
                operation="init",
            )

class DatabaseEngine:
    def __init__(self, config: DatabaseConfig) -> None:
        self._pool: asyncpg.Pool | None = None
        self._config = config

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise DatabaseError(
                reason=DatabaseErrorReason.NOT_CONNECTED,
                message="Database engine is not connected",
                operation="pool",
            )
        return self._pool

    async def close(self) -> None:
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
        migrations = sorted(_MIGRATION_PATH.glob("*.sql"))
        if not migrations:
            raise DatabaseError(
                reason=DatabaseErrorReason.MIGRATION,
                message="Database migrations are missing",
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
    version = migration.stem
    is_applied = await connection.fetchval(MIGRATION_VERSION_READ, version)
    if is_applied:
        return

    statement = await asyncio.to_thread(migration.read_text, encoding="utf-8")
    async with connection.transaction():
        await connection.execute(statement)
        await connection.execute(MIGRATION_VERSION_CREATE, version)
