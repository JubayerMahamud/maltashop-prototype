"""Structured JSON logging with a correlation ID (factor: logs as event streams)."""
import contextvars
import json
import logging
import os
import sys
from datetime import datetime, timezone

correlation_id_var = contextvars.ContextVar("correlation_id", default="-")

SERVICE = os.getenv("SERVICE_NAME", "unknown")
VERSION = os.getenv("APP_VERSION", "dev")
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")


class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": SERVICE,
            "version": VERSION,
            "environment": ENVIRONMENT,
            "correlation_id": correlation_id_var.get(),
            "event": record.getMessage(),
        }
        extra = getattr(record, "fields", None)
        if extra:
            entry.update(extra)
        return json.dumps(entry)


def get_logger():
    logger = logging.getLogger(SERVICE)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)  # stdout, not files
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log(event, **fields):
    get_logger().info(event, extra={"fields": fields})
