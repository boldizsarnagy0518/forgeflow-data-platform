"""Structured logging configuration without credential-bearing environment dumps."""

import logging
import sys

import structlog


def configure_logging(level: str = "INFO", *, json_logs: bool = False) -> None:
    """Configure standard-library and structlog output once per process."""
    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=level)
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if json_logs
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level.upper())),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )
