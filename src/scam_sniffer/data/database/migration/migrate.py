"""Command-line entry point for applying database migration."""

from __future__ import annotations

import asyncio

from scam_sniffer.config import AppConfig
from scam_sniffer.data.database.engine import DatabaseEngine

async def migrate() -> None:
    """Connect to PostgreSQL and apply all pending migration."""
    config = AppConfig.load()
    engine = DatabaseEngine(config=config.database_config)
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
