"""Command-line entry point for applying database migration."""

from __future__ import annotations

import os
import asyncio

from scam_sniffer.data.database.engine import DatabaseConfig, DatabaseEngine

_DATABASE_URL = "postgresql://scam_sniffer:scam_sniffer@localhost:5432/scam_sniffer"
_DATABASE_URL_KEY = "SCAM_SNIFFER_DATABASE_URL"

async def migrate() -> None:
    """Connect to PostgreSQL and apply all pending migration."""
    config = DatabaseConfig(dsn=os.environ.get(_DATABASE_URL_KEY, _DATABASE_URL))
    engine = DatabaseEngine(config=config)
    try:
        await engine.connect()
        await engine.migrate()
    finally:
        await engine.close()

def main() -> None:
    """Run the asynchronous migration entry point."""
    asyncio.run(migrate())

if __name__ == "__main__":
    main()
