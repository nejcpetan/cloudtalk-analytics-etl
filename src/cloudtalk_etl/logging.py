# src/cloudtalk_etl/logging.py
import logging
import sys

import structlog


def setup_logging(log_level: str = "INFO") -> None:
    """
    Configure structured JSON logging via structlog.

    Output goes to stdout so Docker / Portainer can capture it via `docker logs`.
    All log entries are JSON objects with: event, level, timestamp, and any
    keyword arguments bound at the call site.

    Args:
        log_level: Python log level name (DEBUG, INFO, WARNING, ERROR).
                   Defaults to INFO. Invalid names fall back to INFO.
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
