"""Command-line entry point for the local service."""

import asyncio

from scam_sniffer.app import Application
from scam_sniffer.config import AppConfig

from scam_sniffer.core.log.logger import configure_logging

def main() -> None:
    """Load configuration and run the application until shutdown."""
    config = AppConfig.load()
    configure_logging(config.log_config)
    application = Application(config=config)
    asyncio.run(application.run())

if __name__ == "__main__":
    main()
