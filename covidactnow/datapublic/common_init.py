import logging

import structlog
from structlog_sentry import SentryProcessor


def configure_structlog():
    structlog.configure(
        logger_factory=structlog.stdlib.LoggerFactory(),
        processors=[
            structlog.stdlib.add_log_level,  # required before SentryProcessor()
            # sentry_sdk creates events for level >= ERROR and keeps level >= INFO as breadcrumbs.
            SentryProcessor(level=logging.INFO),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
    )
